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

### composing with deferreds

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

### `future` vs `defer`

Clojure's futures can be treated as deferreds, as can Clojure's promises.  However, since both of these abstractions using a blocking dereference, in order for Manifold to treat it as an asynchronous deferred value it must allocate a thread.

Wherever possible, use `manifold.deferred/deferred` instead of `promise`, and `manifold.deferred/defer` instead of `future`.  They will behave identically to their Clojure counterparts (`deliver` can be used on a Manifold deferred, for instance), but allow for callbacks to be registered, so no threads are required.

### let-flow

Let's say that we have two services which provide us numbers, and want to get their sum.  By using `zip` and `chain` together, this is relatively straightforward:

```clj
(defn deferred-sum []
  (let [a (call-service-a)
        b (call-service-b)]
    (chain (zip a b)
      (fn [[a b]]
        (+ a b)))))
```

However, this isn't a very direct expression of what we're doing.  For more complex relationships between deferred values, our code will become even more difficult to understand.  In these cases, it's often best to use `let-flow`.

```clj
(defn deferred-sum [a b]
  (let-flow [a (call-service-a)
             b (call-service-b)]
    (+ a b)))
```

In `let-flow`, we can treat deferred values as if they're realized.  This is only true of values declared within or closed over by `let-flow`, however.  So we can do this:

```clj
(let [a (future 1)]
  (let-flow [b (future (+ a 1))
             c (+ b 1)]
    (+ d 1)))
```

but not this:

```clj
(let-flow [a (future 1)
           b (let [c (future 1)]
                (+ a c))]
  (+ b 1))
```

In this example, `c` is declared within a normal `let` binding, and as such we can't treat it as if it were realized.

It can be helpful to think of `let-flow` as similar to Prismatic's [Graph](https://github.com/prismatic/plumbing#graph-the-functional-swiss-army-knife) library, except that the dependencies between values are inferred from the code, rather than explicitly specified.  Comparisons to core.async's go-routines are less accurate, since `let-flow` allows for concurrent execution of independent paths within the bindings, whereas operations within a go-routine are inherently sequential.
