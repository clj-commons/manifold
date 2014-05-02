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

Callbacks are a useful building block, but they're a painful way to create asynchronous workflows.  This is made easier by `manifold.deferred/chain`, which chains together callbacks, left to right:

```clj
> (def d (d/deferred))
#'d

> (d/chain d inc inc inc #(println "x + 3 =" %))
#<Deferred: :pending>

> (d/success! d 0)
< x + 3 = 3 >
true
```

`chain` returns a deferred representing the return value of the right-most callback.  If any of the functions returns a deferred or a value that can be coerced into a deferred, the chain will be paused until the deferred yields a value.

Values that can be coerced into a deferred include Clojure futures, and core.async channels:

```clj
> (def d (d/deferred))
#'d

> (d/chain d
    #(future (inc %))
    #(println "the future returned" %))
#<Deferred: :pending>

> (d/success! d 0)
< the future returned 1 >
true
        ```

If any stage in `chain` throws an exception or returns a deferred that yields an error, all subsequent stages are skipped, and the deferred returned by `chain` yields that same error.  To handle these cases, you can use `manifold.deferred/catch`:

```clj
> (def d (d/deferred))
#p

> (-> d
    (d/chain dec #(/ 1 %))
    (d/catch Exception #(println "whoops, that didn't work:" %)))
#<Deferred: :pending>

> (d/success! d 1)
< whoops, that didn't work: #<ArithmeticException: Divide by zero> >
true
```

Using the `->` threading operator, `chain` and `catch` can be easily and arbitrarily composed.

To combine multiple deferrable values into a single deferred that yields all their results, we can use `manifold.deferred/zip`:

```clj
> @(d/zip (future 1) (future 2) (future 3))
(1 2 3)
```

Finally, we can use `manifold.deferred/timeout` to get a version of a deferred that will yield a special value if none is provided within the specified time:

```clj
> @(d/timeout
     (future (Thread/sleep 1000) :foo)
     100
     :bar)
:bar
```

### streams

...

### license

Copyright © 2014 Zach Tellman

Distributed under the MIT License.
