Concurrent systems separate **what** happens from **when** it happens.  This is typically accomplished by specifying what the programmers wants to happen (e.g. callbacks), and layering atop an execution model that decides when and where the code should be run (e.g. one or more threads reading from a queue of callbacks to be invoked).  Often, this execution model is hardcoded, making interop between different stream representations much harder than necessary.

Manifold tries to make its execution model as configurable as possible, while still remaining functional for users who don't want to fiddle with the low-level details.  Under different circumstances, Manifold will lazily construct three different pools:

* *wait-pool* - Used solely to wait on blocking operations.  Only created when `manifold.stream/connect` is used on blocking stream abstractions like `java.util.BlockingQueue` or Clojure seqs, or `manifold.deferred/chain` is used with abstractions like `java.util.concurrent.Future` or Clojure promises.  This is an instrumented pool, and statistics can be consumed via `manifold.executor/register-wait-pool-stats-callback`.
* *execute-pool* - Used to execute `manifold.deferred/future` bodies, and only created if that macro is used.  This is an instrumented pool, and statistics can be consumed via `manifold.executor/register-execute-pool-stats-callback`.
* *scheduler-pool* - Used to execute delayed tasks, periodic tasks, or timeouts.  Only created when `manifold.time/in`, `manifold.time/every`, `manifold.stream/periodically`, or take/put timeouts are used.

However, by default messages are processed on whatever thread they were originally `put!` on.  This can get more complicated if multiple threads are calling `put!` on the same stream at the same time, in which case one thread may propagate messages from the other thread.  In general, this means that Manifold conforms to whatever the surrounding execution model is, and users can safely use it in concert with other frameworks.

However, this also means that `put!` will only return once the message has been completely propagated through the downstream topology, which is not always the desired behavior.  The same is also true for a deferred with a long chain of methods waiting on it to be realized.  Conversely, in core.async each hand-off between goroutines is a new task enqueued onto the main thread pool.  This gives better guarantees as to how long an enqueue operation will take before it returns, which can be useful in some situations.

In these cases, we can move the stream or deferred `onto` an executor, guaranteeing that all actions resulting from an operation will be enqueued onto a thread pool rather than immediately executed.  This executor can be generated via `manifold.executor/instrumented-executor`, or using the convenience methods `fixed-thread-executor` and `utilization-executor`.  In addition to providing automatic instrumentation, these executors will guarantee that any streams or deferred created within their scope will also be "on" that executor.  For this reason, it's often sufficient to only call `onto` on a single stream in a topology, as everything downstream of it will transitively be executed on the executor.

```clj
(require
  '[manifold.deferred :as d]
  '[manifold.stream :as s])

(def executor (fixed-thread-executor 42))

(-> (d/future 1)
  (d/onto executor)
  (d/chain inc inc inc))

(->> (s/->source (range 1e3))
  (s/onto executor)
  (s/map inc))
```

If you want to specify your own thread pool, it's important to note that such thread pools in practice either need to have an unbounded queue or an unbounded number of threads.  This is because thread pools with bounded queues and threads will throw a `RejectedExecutionException` when they're full, which can leave our message processing in an undefined state if we're only halfway through the message topology.  It's important to note, though, that the maximum number of enqueued actions is **not** equal to the number of messages we need to process, but rather to the number of nodes in our topology.  This number is usually either fixed, or proportional to something else we can control, such as the number of open connections.  In either case, it is not something that a single external actor can artifically inflate (or at least it shouldn't be).

This configurability is necessary given Manifold's goal of interop with other stream representations, but is only meant to be used by those who need it.  Most can, and should, ignore it.
