![](docs/manifold.png)

This library provides basic building blocks for asynchronous programming, and can be used as a translation layer between libraries which use similar but incompatible abstractions.

Manifold provides two core abstractions: **deferreds**, which represent a single asynchronous value, and **streams**, which represent an ordered sequence of asynchronous values.

Full documentation can be found [here](http://ideolalia.com/manifold).


```clj
[manifold "0.1.0-SNAPSHOT"]
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

Callbacks are a useful building block, but they're a painful way to create asynchronous workflows.  Manifold provides a number of operators for composing over deferred values, which can be read about [here](/docs/deferred.md).

### streams

...

### license

Copyright © 2014 Zach Tellman

Distributed under the MIT License.
