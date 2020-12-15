package app;

import io.javalin.Javalin;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class Main {
    /*
     * Not a good practice to store the session as a static field, but we do it for simplicity in this toy app.
     * A real application should also close the session.
     */
    private static Session session;

    /*
     * We're using a semaphore to limit the number of concurrent `session.execute` calls.
     * If we see that it takes too long to acquire the permit, we inform the end user that the service is overloaded.
     */
    private static final int LIMIT = 200;
    private static final Semaphore concurrencyLimiter = new Semaphore(LIMIT);

    /*
     * Our simple REST application has two routes: /one and /many.
     * The handler of /one wants to execute a single INSERT. The execution happens on the handler's thread.
     * The handler of /many wants to execute multiple INSERTs concurrently. To do that we use a separate thread pool.
     * Note that ALL requests -- both from /one and from /many -- go through concurrencyLimiter; we want to enforce
     * a global concurrency limit on the number of database requests, no matter where in the app they are performed.
     *
     * The size of the thread pool used to execute /many requests is smaller than the global concurrency limit. This
     * makes it easier for concurrent executions of route handlers (in particular, concurrent executions of /many)
     * to access the database.
     */
    private static final ExecutorService manyExecutor = Executors.newFixedThreadPool(20);

    private static final AtomicMonotonicTimestampGenerator timestampGenerator = new AtomicMonotonicTimestampGenerator();

    private static final int PORT = 54321;

    /* Metric collection stuff. We use the Dropwizard Metrics library. */
    private static final MetricRegistry metrics = new MetricRegistry();

    /* Measures durations of `execute` calls. */
    private static final Timer executions = metrics.timer(MetricRegistry.name("executions"));

    /* Measures durations of `executeWithRetries` calls. */
    private static final Timer executionsWithRetries = metrics.timer(MetricRegistry.name("executions-with-retries"));

    /* Measures durations of `concurrencyLimiter.tryAcquire` calls. */
    private static final Timer acquires = metrics.timer(MetricRegistry.name("limiter-waits"));

    /* Counts numbers of retries.
     * Note that the driver itself has a metric called 'retries' which counts numbers of retries
     * made using the RetryPolicy. We don't rely on a RetryPolicy and instead use our own retry loop
     * implementation in this app, so the driver's retries metric is not particularly useful.
     * We name our metric 'app-retries' so the name doesn't conflict with the driver's metric.
     */
    private static final Counter retries = metrics.counter(MetricRegistry.name("app-retries"));

    /* Counts the number of concurrent `execute` calls.
     * To measure concurrency, we increment this counter before each `execute` and decrement after it finishes.  */
    private static final Counter concurrentRequests = metrics.counter(MetricRegistry.name("concurrent-requests"));

    /*
     * This is an example of streaming the metrics to an external metric collection system.
     * The Dropwizard Metrics library supports multiple methods of exporting the metrics;
     * we export them to a Graphite server (http://graphiteapp.org/).
     * See https://graphite.readthedocs.io/en/latest/install.html# for a quick setup tutorial
     * for Graphite. If the Graphite server is not running, the app will still work, the metrics just
     * won't be exported.
     *
     * If you monitor Scylla with Grafana (which you SHOULD!), you can add Graphite as a data source.
     * See: https://grafana.com/docs/grafana/latest/datasources/graphite/
     */
    private static final Graphite graphite = new Graphite("127.0.0.1", 2003);
    private static final GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
        .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL).build(graphite);

    @SuppressWarnings("serial")
    private static class ServiceOverloadedException extends RuntimeException { }

    public static void main(String[] args) {
        var c = Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withQueryOptions(new QueryOptions()
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
            .withRetryPolicy(
                    /*
                     * We use a fallthrough retry policy, i.e. a policy which never retries
                     * but propagates the error to us. We use our own retry loop so we can introduce delays
                     * between retries, which is not possible with RetryPolicy (please don't use Thread.sleep
                     * inside RetryPolicy methods - it will block entire connections from handling any requests).
                     */
                    FallthroughRetryPolicy.INSTANCE)
            .withLoadBalancingPolicy(new TokenAwarePolicy(
                        DCAwareRoundRobinPolicy.builder()
                        /*
                         * Always remember to specify the local DC to be the one that's actually "local" to your application.
                         *
                         * Note that this not only affects performance, it affects consistency as well!
                         * (if you are using LOCAL_* consistency levels)
                         * For example, suppose that you're using LOCAL_QUORUM queries and you provide a bunch of contact points from all DCs to the driver.
                         * Then, if you don't specify the local DC using `.withLocalDc(...)`, the driver will choose the local DC on startup *randomly*
                         * using the DCs of nodes that were provided as contact points.
                         * This means that after your application restarts, the "local DC" may change; in particular, reads performed using LOCAL_QUORUM
                         * after the restart may not see writes performed using LOCAL_QUORUM from before the restart, as there was no guarantee
                         * that they ended up on the currently "local" DC (which, before the restart, was considered remote).
                         */
                        .withLocalDc("datacenter1")
                        /*
                         * Note that with this configuration, the driver will never attempt to contact nodes in remote DCs.
                         * If you want to use e.g. QUORUM consistency level, it may make sense to allow the driver to contact
                         * remote nodes in case all nodes in the local DC are unavailable; you'd then call `withUsedHostsPerRemoteDc(...)`
                         * on the policy builder (see the docs). However, this method is deprecated in driver versions 3.x, and removed in 4.x;
                         * see https://docs.datastax.com/en/developer/java-driver/4.9/manual/core/load_balancing/#local-only for the reason.
                         */
                        .build()))
                        .withPoolingOptions(new PoolingOptions()
                        /*
                         * Suppose we have a 6-node homogeneous cluster with 4 shards per node and we use RF = 3
                         * for each DC (see the keyspace definition below).
                         *
                         * With 3 nodes in our local DC and RF = 3, every request will potentially affect every node
                         * in the DC. Thus, with N concurrent requests, each node in the DC will deal with ~N requests at a time.
                         *
                         * With 4 shards per node, assuming that requests are uniformly distributed across shards,
                         * that will be ~ (N / 4) concurrent requests per shard on average for each node.
                         * Since we set our "global" concurrency limit to 200, and we use 1 connection per shard,
                         * this suggests that our concurrency limit of requests per connection should be ~ 50.
                         * We'll set it to 60 to account for little imbalances of request distribution among shards
                         * (but remember that these imbalances shouldn't be too big - if you often see large imbalances,
                         * that suggests a problem with your data model).
                         *
                         * In the above calculation we've used the fact that we have a single keyspace with a single replication factor
                         * per DC. With multiple keyspaces you can estimate the limit for each and take the maximum of those limits.
                         * Don't worry if you set the limits here a bit too high; the most important limit is the "outer"
                         * limit -- in this app the one enforced by the semaphore.
                         *
                         */
                        .setMaxRequestsPerConnection(HostDistance.LOCAL, 60)
                        /*
                         * `MaxRequestsPerConnection` for REMOTE nodes doesn't really matter in our case
                         * since with the current load balancing policy the driver will never send requests to remote DCs
                         * (see above).
                         */
                        .setMaxRequestsPerConnection(HostDistance.REMOTE, 60)
                        /*
                         * The default values for MaxConnectionsPerHost and CoreConnectionsPerHost is 1 so we don't set them manually.
                         * Our shard-aware driver rounds this number up to the number of shards, so we have 1 connection per shard
                         * (not counting the control connection which is established to one of the shards on one of the nodes).
                         * This should be enough in a vast majority of cases.
                         *
                         */)
                        /*
                         * We're exporting the metrics directly to Graphite so we don't need JMX reporting.
                         */
                        .withoutJMXReporting()
                        .build();

        session = c.connect();

        /* The driver itself collects useful metrics. We include them into our metrics registry so they get exported
         * to Graphite together with our metrics.
         *
         * Note that c.getMetrics() returns a null pointer unless the Cluster object was initialized. One
         * way to initialize it is to connect() with a session, hence the c.connect() call is before the c.getMetrics() call. */
        metrics.registerAll(c.getMetrics().getRegistry());

        /* Start exporting the metrics to Graphite. */
        reporter.start(1, TimeUnit.SECONDS);

        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3, 'datacenter2': 3}");
        session.execute("CREATE TABLE IF NOT EXISTS ks.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        var prepStmt = session.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)");

        /*
         * We're not using QueryBuilder, so we mark the statement as idempotent manually.
         * This is going to be used to decide whether or not to retry the query in case of failure.
         */
        prepStmt.setIdempotent(true);

        var app = Javalin.create().start(PORT);
        app.post("/one", ctx -> {
            /* Note that `stmt.isIdempotent()` is inherited from `prepStmt.isIdempotent()`.
            /* Keep in mind that (non-LWT) writes can be idempotent only if we reuse the same timestamp.
            */
            var stmt = prepStmt.bind(0, 0, "value");

            try {
                executeWithRetriesExponentialBackoff(stmt, 5).toString();
                ctx.result("Finished one\n");
                System.out.println("Success!");
            } catch (DriverException e) {
                /*
                 * A real application would have business-specific error handling code here,
                 * e.g. reporting a "pretty" error message to the user.
                 * Remember that some exceptions, such as `ParseError`, are obvious internal errors
                 * and indicate bugs in your code. Other may indicate bugs in the DB itself, e.g. `ServerError`.
                 */
                ctx.result("service unavailable: " + e.getMessage() + "\n");
            } catch (ServiceOverloadedException e) {
                ctx.result("service overloaded\n");
            }
        });
        app.post("/many", ctx -> {
            LinkedList<Throwable> errors = new LinkedList<Throwable>();

            CompletableFuture.allOf(
                IntStream.range(0, 100)
                .mapToObj(i -> CompletableFuture.runAsync(() -> {
                        var stmt = prepStmt.bind(42, i, String.valueOf(i));
                        try {
                            executeWithRetriesExponentialBackoff(stmt, 3);
                        } catch (Throwable t) {
                            synchronized(errors) {
                                errors.add(t);
                            }
                        }
                    },
                    /*
                     * The executor `manyExecutor`, which uses a fixed-size thread pool, limits the concurrency of requests
                     * performed from the /many route handler.
                     *
                     * Thus they must go through two limits: first through the executor's limit,
                     * then through the semaphore (which limits requests globally for the entire application).
                     *
                     * Note that this executor has an unbounded queue of tasks, i.e. we can always send more tasks to the executor;
                     * the tasks will wait in the queue (taking memory) until a thread becomes available.
                     * A good idea would be probably to use a separate semaphore here (or a bounded queue)
                     * for limiting the number of tasks that can enter this executor, so that the memory consumption
                     * doesn't grow in an unbounded fashion due to many parallel executions of the /many route
                     * (see e.g. https://stackoverflow.com/a/2001205/12844886 for a solution).
                     */
                    manyExecutor))
                .toArray(CompletableFuture[]::new));

            ctx.result("Finished many. Errors: " + errors.stream().map(e -> e.toString()).collect(Collectors.joining(", ")) + "\n");
        });
    }

    static ResultSet executeWithRetriesExponentialBackoff(Statement s, int numAttempts) throws InterruptedException {
        /*
         * This timestamp will be reused each time we execute this query, except in the following cases:
         * 1. The statement contains a `USING TIMESTAMP` clause; the timestamp from `USING TIMESTAMP`
         *    has a greater priority than the timestamp we're setting here. In this app we're not using `USING TIMESTAMP`.
         * 2. This is an LWT operation - in this case, the server will generate and override the timestamp.
         *        You cannot rely on timestamps if you want to achieve idempotency with LWT operations.
         */
        s.setDefaultTimestamp(timestampGenerator.next());

        // The first retry (second attempt) will be delayed by 50ms.
        int delay = 50;

        try (final Timer.Context ignored = executionsWithRetries.time()) {
            for (int attempt = 0; attempt < numAttempts; ++attempt) {
                try {
                    return executeWithLimiter(s);
                } catch (DriverException e) {
                    /*
                     * Remember to use a real logging solution in your app.
                     */
                    System.out.println(e.getMessage());
                    if (attempt != numAttempts - 1 && isSafeToRetry(e, s) && shouldRetry(e)) {
                        /* We decided to retry.
                         * The second attempt will be delayed by 50ms, the third by 100ms, fourth by 200ms, and so on.
                         *
                         * Note that for (non-LWT) writes, we may not want to drift away too much - we're reusing the same timestamp on each retry
                         * (in order to make the write idempotent). Therefore, with delayed retries, it is highly probable that the write will be made
                         * "to the past" compared to the current time (both as shown by your clock and the clocks of the DB nodes).
                         *
                         * This may be a problem e.g. if you're using CDC and reading a CDC log table with a moving time window; each windowed query reads only writes
                         * made with timestamps in this window. If you write too far "into the past", the moving time window may not catch that write.
                         * If you're using CDC, make sure that the length of the time window is significantly larger than the allowed drift.
                         *
                         * With numAttempts = 5 and the current delay strategy, the time drift of the last attempt will be
                         * <= (0+50+100+200+400)ms + the time it took to execute each query (including the waiting on semaphore, if any);
                         * with a server-side write timeout of ~5s and a semaphore timeout of ~2s, the drift can go as far as 7s * 5 + 750ms =~ 36s,
                         * and that doesn't include network delays between the client and the write coordinator, and between the coordinator and replicas.
                         *
                         * If you have the luxury of being able to safely perform write retries in a non-idempotent way (with a new timestamp on each retry)
                         * -- prefer that, because then the time drift won't depend on the number of retries, only on execution time.
                         */

                        System.out.println("Retrying in " + String.valueOf(delay));
                        Thread.sleep(delay);
                        delay *= 2;

                        /*
                         * In your app, you may want to have separate metrics for different error types.
                         */
                        retries.inc();
                    } else {
                        System.out.println("Giving up :(");
                        throw e;
                    }
                }
            }
        }

        /*
         * This code is unreachable: the loop above either returns or throws.
         */
        throw new RuntimeException("unreachable");
    }

    static ResultSet executeWithLimiter(Statement s) throws InterruptedException {
        try (final Timer.Context ignored = acquires.time()) {
            if (!concurrencyLimiter.tryAcquire(1, 2, TimeUnit.SECONDS)) {
                /*
                 * We've been waiting for the semaphore for two seconds with no avail.
                 * There must be some really long queries running and blocking all other requests,
                 * or there must be a really long queue of requests waiting for execution.

                 * If you run long paged queries and short quick queries in the same application,
                 * and you're using a single concurrency limit for both, the long queries may block the quick queries
                 * if you're unlucky and the long queries steal all permits.
                 * In those cases consider using separate concurrency limiters for the different workload types
                 * and perhaps even split your service into separate (micro)services.

                 * In any case, remember that the limiter is a safety measure - the semaphore should rarely block
                 * at all. If your metrics/logs show that queries are often waiting on the semaphore, you should start thinking about
                 * scaling up and then increasing the limits (or, if your cluster is constantly underutilized, just increase the limits
                 * - you may have underestimated them in the first place).
                 */
                throw new ServiceOverloadedException();
            }
        }

        concurrentRequests.inc();
        try (final Timer.Context ignored = executions.time()) {
            return session.execute(s);
        } finally {
            concurrencyLimiter.release();
            concurrentRequests.dec();
        }
    }

    /*
     * Won't retrying potentially cause the write to be performed twice?
     *
     * Note that in some apps double execution of some writes may still be OK.
     * In this toy example we arbitrarily decided that it's not.
     */
    static boolean isSafeToRetry(DriverException e, Statement s) {
        return s.isIdempotent() || isDefiniteFailure(e);
    }

    /*
     * Suppose that it's safe to retry a query.
     * Should we do it?
     *
     * Here we give a simple 0 or 1 response.
     * In your app, you may also want to adjust the delay between retries based on the type of error, e.g.:
     * - for TimeoutExceptions it probably already took quite long for the request to execute
     *   so it may make sense to reduce the delay before the next attempt
     * - for UnavailableException it may make sense to increase the delay
     * - for OverloadedException it may make sense to increase the delay even more (or not retry at all, as we do here)
     */
    static boolean shouldRetry(DriverException e) {
        return e instanceof ReadTimeoutException || e instanceof WriteTimeoutException || e instanceof UnavailableException
            || (e instanceof NoHostAvailableException &&
                    ((NoHostAvailableException) e).getErrors().values().stream().allMatch(
                        ex -> ex instanceof DriverException && shouldRetry((DriverException) ex)));
    }

    /*
     * Definite failure: no observable effect on the database.
     * Indefinite failure: could have modified the state of the database, affecting later results.
     *
     * All definite failures can be safely considered as indefinite, but not vice-versa.
     */
    static boolean isDefiniteFailure(DriverException e) {
        /*
         * There are other definite error types, like "BusyPoolException",
         * but we don't have to be 100% accurate - it's safe to consider a definite failure as an indefinite one.
         * The other way is not allowed, e.g. it would be unsafe to consider "WriteTimeoutException" as definite.
         */
        return e instanceof UnavailableException
            || (e instanceof NoHostAvailableException &&
                    ((NoHostAvailableException) e).getErrors().values().stream().allMatch(
                        ex -> ex instanceof DriverException && isDefiniteFailure((DriverException) ex)));
    }
}
