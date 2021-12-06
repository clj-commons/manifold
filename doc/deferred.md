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

### Composing with deferreds

Callbacks are a useful building block, but they're a painful way to create asynchronous workflows.  In practice, no one should ever use `on-realized`.

Instead, they should use `manifold.deferred/chain`, which chains together callbacks, left to right:

```clojure
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

```clojure
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

```clojure
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

```clojure
> @(d/zip (future 1) (future 2) (future 3))
(1 2 3)
```

Finally, we can use `manifold.deferred/timeout!` to register a timeout on the deferred which will yield either a specified timeout value or a `TimeoutException` if the deferred is not realized within `n` milliseconds.

```clojure
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

```clojure
(defn deferred-sum []
  (let [a (call-service-a)
        b (call-service-b)]
    (chain (zip a b)
           (fn [[a b]]
             (+ a b)))))
```

However, this isn't a very direct expression of what we're doing.  For more complex relationships between deferred values, our code will become even more difficult to understand.  In these cases, it's often best to use `let-flow`.

```clojure
(defn deferred-sum []
  (let-flow [a (call-service-a)
             b (call-service-b)]
    (+ a b)))
```

In `let-flow`, we can treat deferred values as if they're realized.  This is only true of values declared within or closed over by `let-flow`, however.  So we can do this:

```clojure
(let [a (future 1)]
  (let-flow [b (future (+ a 1))
             c (+ b 1)]
    (+ c 1)))
```

but not this:

```clojure
(let-flow [a (future 1)
           b (let [c (future 1)]
               (+ a c))]
  (+ b 1))
```

In this example, `c` is declared within a normal `let` binding, and as such we can't treat it as if it were realized.

It can be helpful to think of `let-flow` as similar to Prismatic's [Graph](https://github.com/prismatic/plumbing#graph-the-functional-swiss-army-knife) library, except that the dependencies between values are inferred from the code, rather than explicitly specified.  Comparisons to core.async's goroutines are less accurate, since `let-flow` allows for concurrent execution of independent paths within the bindings, whereas operations within a goroutine are inherently sequential.

### go-off

An alternate way to write code using deferreds is the macro `manifold.go-off/go-off`. This macro is an almost-exact mirror of the `go` macro from [core.async](https://github.com/clojure/core.async), to the point where it actually utilizes the state machine functionality from core.async. In order to use this macro, `core.async` must be provided as a dependency by the user. 

There are a few major differences between `go` and `go-off`. First, `go-off` (unsurprisingly) works with Manifold deferreds and streams instead of core.async channels. Second, in addition to the `<!` fn, which always returns a value, there's also the `<!?` macro, which behaves the same unless a Throwable is retrieved, in which case it's automatically rethrown for you. Finally, there's no `>!` equivalent in `go-off`, as there's no way without altering the syntax to distinguish between success and error when putting into a deferred.

The benefit of `go-off` over `let-flow` is that it gives complete control of when deferreds should be realized to the user, removing any potential surprises (especially around timeouts).

```clojure
;; basic usage
@(go-off (+ (<!? (d/future 10))
            (<!? (d/future 20))))       ;; 30
```

```clojure
;; <!? usage

;; if you forget to catch an exception in go, it won't be returned
(<!! (go (try (<! (go (/ 5 0)))        
              (catch Exception e
                "ERROR"))))             ;; nil

;; d/future returns any exceptions, and <!? rethrows it for you 
@(go-off (try (<!? (d/future (/ 5 0)))
              (catch Exception e
                "ERROR")))              ;; "ERROR"
```

### loop

Manifold also provides a `loop` macro, which allows for asynchronous loops to be defined. Consider `manifold.stream/consume`, which allows a function to be invoked with each new message from a stream.  We can implement similar behavior like so:

```clojure
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


### Custom execution models

Both deferreds and streams allow for custom execution models to be specified.  To learn more, [go here](/doc/execution.md).
