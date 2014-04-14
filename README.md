![](docs/manifold.png)

This library provides basic building blocks for asynchronous programming, and can be used as a translation layer between libraries which use similar but incompatible abstractions.

Manifold provides two core abstractions: **promises**, which represent a single asynchronous value, and **streams**, which represent an ordered sequence of asynchronous values.

Full documentation can be found [here](http://ideolalia.com/manifold).


```clj
[manifold "0.1.0-SNAPSHOT"]
```

### promises

A promise in Manifold behaves exactly like a Clojure promise:

```clj
> (require '[manifold.promise :as p])
nil
> (def p (p/promise))
#'p
> (p/success! p :foo)
true
> @p
:foo
```

However, similar to Clojure's futures, promises in Manifold can also represent errors.  Crucially, they also allow for callbacks to be registered, rather than simply blocking on derefencing.

```clj
> (def p (p/promise))
#'p
> (p/error! p (Exception. "boom"))
true
> @p
Exception: boom
```

```clj
> (def p (p/promise))
#'p
> (p/on-realized p
    (fn [x] (println "success!" x))
    (fn [x] (prtinln "error!" x)))
true
> (p/success! p :foo)
< success! :foo >
true
```

Callbacks are a useful building block, but they're a painful way to create asynchronous workflows.  This is made easier by `manifold.promise/chain`, which chains together callbacks, left to right:

```clj
> (def p (p/promise))
#'p
> (p/chain p inc inc inc #(println "x + 3 =" %))
#<Promise: :pending>
> (p/success! p 0)
< x + 3 = 3 >
true
```

`chain` returns a promise representing the return value of the right-most callback.  If any of the functions returns a promise or a value that can be coerced into a promise, the chain will be paused until the promise yields a value.

Values that can be coerced into a promise include Clojure futures, and core.async channels:

```clj
> (def p (p/promise))
#'p
> (p/chain p
    #(future (inc %))
    #(println "the future returned" %))
#<Promise: :pending>
> (p/success! p 0)
< the future returned 1 >
true
```

If any stage in `chain` throws an exception or returns a promise that yields an error, all subsequent stages are skipped, and the promise returned by `chain` yields that same error.  To handle these cases, you can use `manifold.promise/catch`:

```clj
> (def p (p/promise))
#p
> (-> p
    (p/chain dec #(/ 1 %))
    (p/catch Exception #(println "whoops, that didn't work:" %)))
#<Promise: :pending>
> (p/success! p 1)
< whoops, that didn't work: #<ArithmeticException: Divide by zero> >
true
```

Using the `->` threading operator, `chain` and `catch` can be easily and arbitrarily composed.

To combine multiple promisable values into a single promise that yields all their results, we can use `manifold.promise/zip`:

```clj
> @(p/zip (future 1) (future 2) (future 3))
(1 2 3)
```

Finally, we can use `manifold.promise/timeout` to get a version of a promise that will yield a special value if none is provided within the specified time:

```clj
> @(p/timeout
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
