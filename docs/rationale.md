### the fundamental problems

Writing to a queue requires data flowing in two directions: messages flowing from the producer, and backpressure signals from the consumer.  In typical Java, the messages are an explicit channel of communication, but the backpressure is implicit - the producer's thread will simply block whenever the consumer is unable to accept more messages.  The JVM provides a large number of synchronous primitives, as well as a threading model optimized for this sort of backpressure, but fundamentally this means that every queue must have its own thread.

An asynchronous queue can't rely on the Java threading model, so it has to make the channel conveying backpressure explicit.  One example of this is node.js, which will simply [return `false`](http://nodejs.org/api/stream.html#stream_writable_write_chunk_encoding_callback) to calls which enqueue data when it can't accept any more.  Unfortunately, where the blocking semantics of Java are strictly enforced, this is more of a hint.  Nothing prevents the asynchronous programmer from blithely enqueueing more messages until they run out of memory or hit some underlying limitation which turns their code unexpectedly synchronous.

However, the asynchronous model does have a big advantage: it's a superset of the synchronous model.  The synchronous model uses **structural backpressure**, whereas the asynchronous method represents **backpressure as data**.  It's easy to transform data into structure: any asynchronous data, whether provided via a deferred value or a callback, can be blocked on.  The inverse, however, requires us to wrap a thread around any operation which might block, which creates significant overhead.  Worse yet, dependent tasks that use the same finite thread pool risk deadlock.  This can be worked around via the sort of the cooperative multithreading used by ForkJoin, but this can end up being quite fiddly and difficult to get right.

### core.async

[core.async](https://github.com/clojure/core.async) avoids tackling this problem head-on by emulating Java's structural backpressure without using threads.  This has a number of nice properties, but also introduces some problems of its own.

The most obvious problem is that core.async uses an execution model which assumes pervasive asynchrony, which is not a safe assumption on the JVM.  The programmer must manually differentiate between scopes where code will and will not block; failure to do so correctly will exhaust the non-blocking thread pool and cause a deadlock.

In practice, this means that core.async is most effectively used as an **application-level abstraction**, where the programmer can guarantee pervasive use of core.async, and can design the execution model around core.async's needs.

However, when creating a library that consumes and provides stream abstractions, using core.async channels means the library will only be used by people already using core.async.  Given the impedance mismatches with both synchronous and asynchronous JVM libraries, this seems unnecessarily limiting.  In general, **all of the existing stream representations are walled gardens**, including but not limited to [RxJava](https://github.com/Netflix/RxJava), [Reactive Streams](http://www.reactive-streams.org/), [Lamina](https://github.com/ztellman/lamina), and [Reactor](https://github.com/reactor/reactor).  This is at strong odds with Clojure's philosophy, which focuses on a large number of functions for a very small number of universal data structures.

### manifold

Manifold attempts to provide a common ground between all these abstractions.  It allows for simple, efficient interop with both synchronous and asynchronous queue implementations, allowing for other stream representations to be transformed to and from Manifold streams.  Manifold has a small number of design principles:

* pervasive asynchrony, emulated by wrapping threads around synchronous objects where necessary
* all asynchronous values and operations represented as deferreds
* stream interaction reduced to `put!`, `take!`, and variations of each which can time out
* all Manifold sources can be implicitly converted to seqs via `Seqable`, so Clojure's sequence operators can be directly applied to them.
* all Clojure sequences can be implicitly converted to Manifold sources, so Manifold's stream operators can be directly applied to them
* explicit stream topology constructed via `(connect source sink)`, which is the underlying mechanism for Manifold's stream operators

The `connect` and topology mechanisms are pluggable, allowing for other stream abstractions to "extend" a Manifold topology.  A Manifold stream can be transformed to and from a `BlockingQueue`, Clojure seq, and core.async channel.  Extending to other representations is as simple as defining `put!` and `take!` functions.  A Manifold deferred can be transparently substituted for a Clojure future or promise, and a future or promise will be automatically coerced to a deferred where necessary.

Manifold is not intended to be as feature-rich as other stream libraries, but rather to be just feature-rich enough to enable library developers to use it as an asynchronous [lingua franca](http://en.wikipedia.org/wiki/Lingua_franca).  It is, at this moment, fully functional but subject to change.  Feedback is welcomed.
