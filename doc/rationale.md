Manifold provides representations for data we don't yet have, and tools for acting upon it.  Sometimes this data is something we can compute ourselves, but more often it's sent to us by something outside our process.  And since we don't control when the data arrives, it's likely that sometimes it will arrive faster than we can process it.

This means that we not only need to correctly process the data, we need to have a strategy for when we get too much of it.  [This is discussed in depth in this talk](https://www.youtube.com/watch?v=1bNOO3xxMc0), but the typical strategy is to use **backpressure**, which is a signal sent to the producer that we can't handle more messages, and a subsequent message that we now can.

Backpressure is a fundamental property of Java's threading model, as shown by `BlockingQueues`, `Futures`, `InputStreams`, and others.  It's also a fundamental property of `core.async` channels, though it uses a completely different execution model built on callbacks and macros.  It's also a fundamental property of Clojure's lazy sequences, which like Java's abstractions are blocking, but unlike both Java and `core.async` relies on pulling data towards the consumer rather than having it pushed.

Unfortunately, while all of these abstractions (or [RxJava](https://github.com/ReactiveX/RxJava), or [Reactive Streams](http://www.reactive-streams.org/), or ...) can be used to similar effects, they don't necessarily work well with each other.  The practical effect of this is that by choosing one abstraction, we often make the others off-limits.  When writing an application, this may be acceptable, if not really desirable.  When writing a library or something meant to be reused, though, it's much worse; only people who have chosen your particular walled garden can use your work.

Manifold provides abstractions that sits at the intersection of all these similar, but incompatible, approaches.  It provides an extensible mechanism for coercing unrealized data into a generic form, and piping data from these generic forms into other stream representations.

It has relatively few central ideas:

* pervasive asynchrony, emulated by wrapping threads around synchronous objects where necessary
* all asynchronous values and operations represented as deferreds
* streams can either be message **sources**, message **sinks**, or both
  * sources are interacted with via `take!`, `try-take!`, and `close!`
  * sinks are interacted with via `put!`, `try-put!`, and `close!`
* messages from anything which is "sourceable" can be piped into anything which is "sinkable" via `manifold.stream/connect`
  * the topology created via `connect` is explicit, and can be walked and queried
* both deferreds and streams can have their execution pushed onto a thread pool via their respective `onto` methods

Manifold is not intended to be as feature-rich as other stream libraries, but rather to be just feature-rich enough to enable library developers to use it as an asynchronous [lingua franca](http://en.wikipedia.org/wiki/Lingua_franca).  It is, at this moment, fully functional but subject to change.  Feedback is welcomed.