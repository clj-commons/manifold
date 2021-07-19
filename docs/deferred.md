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
<< ... >>

> (d/success! d :foo)
success! :foo
true
```

### composing with deferreds

Callbacks are a useful building block, but they're a painful way to create asynchronous workflows.  In practice, no one should ever use `on-realized`.

Instead, they should use `manifold.deferred/chain`, which chains together callbacks, left to right:

```clj
> (def d (d/deferred))
#'d

> (d/chain d inc inc inc #(println "x + 3 =" %))
<< ... >>

> (d/success! d 0)
x + 3 = 3
true
```

`chain` returns a deferred representing the return value of the right-most callback.  If any of the functions returns a deferred or a value that can be coerced into a deferred, the chain will be paused until the deferred yields a value.

Values that can be coerced into a deferred include Clojure futures, Java futures, and Clojure promises.

```clj
> (def d (d/deferred))
#'d

> (d/chain d
    #(future (inc %))
    #(println "the future returned" %))
<< ... >>

> (d/success! d 0)
the future returned 1
true
```

If any stage in `chain` throws an exception or returns a deferred that yields an error, all subsequent stages are skipped, and the deferred returned by `chain` yields that same error.  To handle these cases, you can use `manifold.deferred/catch`:

```clj
> (def d (d/deferred))
#p

> (-> d
    (d/chain dec #(/ 1 %))
    (d/catch Exception #(println "whoops, that didn't work:" %)))
<< ... >>

> (d/success! d 1)
whoops, that didn't work: #error {:cause Divide by zero :via [{:type java.lang.ArithmeticException ...
true
```

Using the `->` threading operator, `chain` and `catch` can be easily and arbitrarily composed.

To combine multiple deferrable values into a single deferred that yields all their results, we can use `manifold.deferred/zip`:

```clj
> @(d/zip (future 1) (future 2) (future 3))
(1 2 3)
```

Finally, we can use `manifold.deferred/timeout!` to register a timeout on the deferred which will yield either a specified timeout value or a `TimeoutException` if the deferred is not realized within `n` milliseconds.

```clj
> @(d/timeout!
     (d/future (Thread/sleep 1000) :foo)
     100
     :bar)
:bar
```

Note that if a timeout is placed on a deferred returned by `chain`, the timeout elapsing will prevent any further stages from being executed.

### `future` vs `manifold.deferred/future`

Clojure's futures can be treated as deferreds, as can Clojure's promises.  However, since both of these abstractions use a blocking dereference, in order for Manifold to treat it as an asynchronous deferred value it must allocate a thread.

Wherever possible, use `manifold.deferred/deferred` instead of `promise`, and `manifold.deferred/future` instead of `future`.  They will behave identically to their Clojure counterparts (`deliver` can be used on a Manifold deferred, for instance), but allow for callbacks to be registered, so no additional threads are required.

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
(defn deferred-sum []
  (let-flow [a (call-service-a)
             b (call-service-b)]
    (+ a b)))
```

In `let-flow`, we can treat deferred values as if they're realized.  This is only true of values declared within or closed over by `let-flow`, however.  So we can do this:

```clj
(let [a (future 1)]
  (let-flow [b (future (+ a 1))
             c (+ b 1)]
    (+ c 1)))
```

but not this:

```clj
(let-flow [a (future 1)
           b (let [c (future 1)]
                (+ a c))]
  (+ b 1))
```

In this example, `c` is declared within a normal `let` binding, and as such we can't treat it as if it were realized.

It can be helpful to think of `let-flow` as similar to Prismatic's [Graph](https://github.com/prismatic/plumbing#graph-the-functional-swiss-army-knife) library, except that the dependencies between values are inferred from the code, rather than explicitly specified.  Comparisons to core.async's goroutines are less accurate, since `let-flow` allows for concurrent execution of independent paths within the bindings, whereas operations within a goroutine are inherently sequential.

### manifold.tsasvla

An alternate way to write code using deferreds is the macro `manifold.tsasvla/tsasvla`. This macro is an almost exact mirror of the `go` macro from [clojure/core.async](https://github.com/clojure/core.async), to the point where it actually utilizes the state machine functionality from core.async. In order to use this macro, `core.async` must be a dependency provided by the user. The main difference between `go` and `tsasvla`, besides tsasvla working with deferrables instead of core.async channels, is the `take` function being `<!?` instead of `<!`. The difference in function names is used to indicate exceptions behave the same as a non-async clojure block (i.e. are thrown) instead of silently swallowed & returning `nil`.  

The benefit of this macro over `let-flow` is that it gives complete control of when deferreds should be realized to the user of the macro, removing any potential surprises (especially around timeouts).

```clj
@(tsasvla (+ (<!? (d/future 10))
             (<!? (d/future 20))))                          ;; ==> 30
```

```clj
(<!! (core.async/go (try (<! (go (/ 5 0)))
                         (catch Exception e
                           "ERROR"))))                      ; ==> nil

@(tsasvla (try (<!? (d/future (/ 5 0)))
               (catch Exception e
                 "ERROR")))                                 ; ==> "ERROR"
```

### `manifold.deferred/loop`

Manifold also provides a `loop` macro, which allows for asynchronous loops to be defined.  Consider `manifold.stream/consume`, which allows a function to be invoked with each new message from a stream.  We can implement similar behavior like so:

```clj
(require
  '[manifold.deferred :as d]
  '[manifold.stream :as s])

(defn my-consume [f stream]
  (d/loop []
    (d/chain (s/take! stream ::drained)

      ;; if we got a message, run it through `f`
      (fn [msg]
        (if (identical? ::drained msg)
          ::drained
          (f msg)))

      ;; wait for the result from `f` to be realized, and
      ;; recur, unless the stream is already drained
      (fn [result]
        (when-not (identical? ::drained result)
          (d/recur))))))
 ```

Here we define a loop which takes messages one at a time from `stream`, and passes them into `f`.  If `f` returns an unrealized value, the loop will pause until it's realized.  To recur, we make sure the value returned from the final stage is `(manifold.deferred/recur & args)`, which will cause the loop to begin again from the top.

While Manifold doesn't provide anything as general purpose as core.async's `go` macro, the combination of `loop` and `let-flow` can allow for the specification of highly intricate asynchronous workflows.

### custom execution models

Both deferreds and streams allow for custom execution models to be specified.  To learn more, [go here](/docs/execution.md).
