This is a simple REST application that provides two POST routes: `/many` and `/one`.
The application performs inserts to a Scylla cluster (`/many` performs multiple inserts concurrently, `/one` performs a single insert).
It uses the Javalin web framework (https://javalin.io/) to provide the REST functionality.

The purpose of this app is to illustrate some techniques useful for writing applications that use Scylla:
- limiting concurrency of database requests.
- Retrying requests: we only retry requests that are idempotent or if the previous attempt was a definite failure. We use our own retry loop (instead of the driver's `RetryPolicy` interface) so we can introduce delays between attempts. We use an exponential backoff method for the delays.
- Using prepared statements.
- Setting a proper load balancing policy.
- Gathering some useful metrics.

Compile with Maven. Requires JDK >= 10.
