![](docs/manifold.png)

This library provides basic building blocks for asynchronous programming, and can be used as a translation layer between libraries which use similar but incompatible abstractions.

Manifold provides two core abstractions: **deferreds**, which represent a single asynchronous value, and **streams**, which represent an ordered sequence of asynchronous values.

A detailed discussion of Manifold's rationale can be found [here](/docs/rationale.md).  Full documentation can be found [here](http://ideolalia.com/manifold).


```clj
[manifold "0.1.0-beta10"]
```

### deferreds

A deferred in Manifold is similar to a Clojure promise:

```clj
> (require '[manifold.deferred :as d])
nil

> (def d (d/deferred))
#'d

> (d/success! d :foo)
true

> @d
:foo
```

However, similar to Clojure's futures, deferreds in Manifold can also represent errors.  Crucially, they also allow for callbacks to be registered, rather than simply blocking on dereferencing.

```clj
> (def d (d/deferred))
#'d

> (d/error! d (Exception. "boom"))
true

> @d
Exception: boom
```

```clj
> (def d (d/deferred))
#'d

> (d/on-realized d
    (fn [x] (println "success!" x))
    (fn [x] (println "error!" x)))
true

> (d/success! d :foo)
< success! :foo >
true
```

Callbacks are a useful building block, but they're a painful way to create asynchronous workflows.  In practice, **no one should ever need to use `on-realized`**.  Manifold provides a number of operators for composing over deferred values, [which can be read about here](/docs/deferred.md).

### streams

Manifold's streams provide mechanisms for asynchronous puts and takes, timeouts, and backpressure.  They are compatible with Java's `BlockingQueues`, Clojure's lazy sequences, and core.async's channels.  Methods for converting to and from each are provided.

Manifold differentiates between **sources**, which emit messages, and **sinks**, which consume messages.  We can interact with sources using `take!` and `try-take!`, which return deferred values representing the next message.  We can interact with sinks using `put!` and `try-put!`, which return a deferred values which will yield `true` if the put is successful, or `false` otherwise.

We can create a stream using `(manifold.stream/stream)`:

```clj
> (require '[manifold.stream :as s])
nil
> (def s (s/stream))
#'s
> (s/put! s 1)
<< ... >>
> (s/take! s)
<< 1 >>
```

A stream is both a sink and a source; any message sent via `put!` can be received via `take!`.  We can also create sinks and sources from other stream representations using `->sink` and `->source`:

```clj
> (require '[clojure.core.async :as a])
nil
> (def c (a/chan))
#'c
> (def s (s/->source c))
#'s
> (a/go (a/>! c 1))
...
> @(s/take! s)
1
```

We can also turn a Manifold stream into a different representation by using `connect` to join them together:

```clj
> (def s (s/stream))
#'s
> (def c (a/chan))
#'c
> (s/connect s c)
nil
> (s/put! s 1)
...
> (a/<!! c)
1
```

Manifold also provides stream operators, similar to Clojure's sequence operators, including `map`, `filter`, `mapcat`, `zip`, `reductions`, `reduce`, `buffer`, and `batch`.  [To learn more about streams, go here](/docs/stream.md).

### license

Copyright © 2014 Zach Tellman

Distributed under the MIT License.
