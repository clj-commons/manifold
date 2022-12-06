[![Clojars Project](https://img.shields.io/clojars/v/manifold.svg)](https://clojars.org/manifold)
[![cljdoc badge](https://cljdoc.org/badge/manifold/manifold)](https://cljdoc.org/d/manifold/manifold)
[![CircleCI](https://circleci.com/gh/clj-commons/manifold.svg?style=svg)](https://circleci.com/gh/clj-commons/manifold)
![](doc/manifold.png)

Manifold provides basic building blocks for asynchronous programming, and can be used as a translation layer between libraries which use similar, but incompatible, abstractions.

Manifold provides two core abstractions: **deferreds**, which represent a single asynchronous value, and **streams**, which represent an ordered sequence of asynchronous values.

A detailed discussion of Manifold's rationale can be found [here](doc/rationale.md).  Full documentation can be found [here](https://cljdoc.org/d/manifold/manifold).

Leiningen:
```clojure
[manifold "0.3.0"]
```

deps.edn:
```clojure
manifold/manifold {:mvn/version "0.3.0"}
```

### Deferreds

A deferred in Manifold is similar to a Clojure promise:

```clojure
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

```clojure
> (def d (d/deferred))
#'d

> (d/error! d (Exception. "boom"))
true

> @d
Exception: boom
```

```clojure
> (def d (d/deferred))
#'d

> (d/on-realized d
    (fn [x] (println "success!" x))
    (fn [x] (println "error!" x)))
<< ... >>

> (d/success! d :foo)
success! :foo
true
```

Callbacks are a useful building block, but they're a painful way to create asynchronous workflows.  In practice, **no one should ever need to use `on-realized`**.  Manifold provides a number of operators for composing over deferred values, [which can be read about here](/doc/deferred.md).

### Streams

Manifold's streams provide mechanisms for asynchronous puts and takes, timeouts, and backpressure.  They are compatible with Java's `BlockingQueues`, Clojure's lazy sequences, and core.async's channels.  Methods for converting to and from each are provided.

Manifold differentiates between **sources**, which emit messages, and **sinks**, which consume messages.  We can interact with sources using `take!` and `try-take!`, which return deferred values representing the next message.  We can interact with sinks using `put!` and `try-put!`, which return a deferred values which will yield `true` if the put is successful, or `false` otherwise.

We can create a stream using `(manifold.stream/stream)`:

```clojure
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

```clojure
> (require '[clojure.core.async :as a])
nil
> (def c (a/chan))
#'c
> (def s (s/->source c))
#'s
> (a/go (a/>! c 1))
#object[clojure.core.async.impl.channels.ManyToManyChannel 0x7...
> @(s/take! s)
1
```

We can also turn a Manifold stream into a different representation by using `connect` to join them together:

```clojure
> (def s (s/stream))
#'s
> (def c (a/chan))
#'c
> (s/connect s c)
nil
> (s/put! s 1)
<< true >>
> (a/<!! c)
1
```

Manifold can use any transducer, which are applied via `transform`.  It also provides stream-specific transforms, including `zip`, `reduce`, `buffer`, `batch`, and `throttle`.  [To learn more about streams, go here](/doc/stream.md).

### Clojurescript

A Clojurescript implementation of Manifold can be found here: [dm3/manifold-cljs](https://github.com/dm3/manifold-cljs).


### License

Copyright © 2014-2022 Zach Tellman

Distributed under the MIT License.
