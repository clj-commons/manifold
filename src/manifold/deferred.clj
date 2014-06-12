(ns manifold.deferred
  (:refer-clojure :exclude [realized? loop])
  (:require
    [manifold.utils :as utils]
    [manifold.time :as time]
    [clojure.set :as set])
  (:import
    [java.util
     LinkedList]
    [java.util.concurrent
     Future
     TimeoutException
     TimeUnit
     ConcurrentHashMap
     CountDownLatch]
    [java.util.concurrent.atomic
     AtomicBoolean
     AtomicInteger]
    [clojure.lang
     IPending
     IBlockingDeref
     IDeref]))

(set! *warn-on-reflection* true)

(defprotocol Deferrable
  (^:private to-deferred [_] "Provides a conversion mechanism to manifold deferreds."))

;; implies IDeref, IBlockingDeref, IPending
(definterface IDeferred
  (^boolean realized [])
  (onRealized [on-success on-error]))

(definline realized?
  "Returns true if the manifold deferred is realized."
  [x]
  `(.realized ~(with-meta x {:tag "manifold.deferred.IDeferred"})))

(definline on-realized
  "Registers callbacks with the manifold deferred for both success and error outcomes."
  [x on-success on-error]
  `(.onRealized ~(with-meta x {:tag "manifold.deferred.IDeferred"}) ~on-success ~on-error))

(definline deferred?
  "Returns true if the object is an instance of a Manifold deferred."
  [x]
  `(instance? IDeferred ~x))

(let [f (utils/fast-satisfies #'Deferrable)]
  (defn deferrable? [x]
    (or
      (instance? IDeferred x)
      (instance? Future x)
      (instance? IPending x)
      (f x))))

;; TODO: do some sort of periodic sampling so multiple futures can share a thread
(defn- register-future-callbacks [x on-success on-error]
  (if (or
        (when (instance? Future x)
          (or (.isDone ^Future x) (.isCancelled ^Future x)))
        (when (instance? IPending x)
          (.isRealized ^IPending x)))
    (try
      (on-success @x)
      (catch Throwable e
        (on-error e)))
    (utils/wait-for
      (try
        (on-success @x)
        (catch Throwable e
          (on-error e))))))

(defn ->deferred
  "Transforms `x` into an async-deferred, or returns `nil` if no such transformation is
   possible."
  [x]
  (condp instance? x
    IDeferred
    x

    Future
    (let [^Future x x]
      (reify
        IDeref
        (deref [_]
          (.get x))
        IBlockingDeref
        (deref [_ time timeout-value]
          (try
            (.get x time TimeUnit/MILLISECONDS)
            (catch TimeoutException e
              timeout-value)))
        IPending
        (isRealized [this]
          (realized? this))
        IDeferred
        (realized [_]
          (or (.isDone x) (.isCancelled x)))
        (onRealized [_ on-success on-error]
          (register-future-callbacks x on-success on-error))))

    IPending
    (reify
      IDeref
      (deref [_]
        (.deref ^IDeref x))
      IBlockingDeref
      (deref [_ time timeout-value]
        (.deref ^IBlockingDeref x time timeout-value))
      IPending
      (isRealized [_]
        (.isRealized ^IPending x))
      IDeferred
      (realized [_]
        (.isRealized ^IPending x))
      (onRealized [_ on-success on-error]
        (register-future-callbacks x on-success on-error)
        nil))

    (when (deferrable? x)
      (to-deferred x))))

;;;

(definterface IDeferredListener
  (onSuccess [x])
  (onError [err]))

(deftype Listener [on-success on-error]
  IDeferredListener
  (onSuccess [_ x] (utils/without-overflow (on-success x)))
  (onError [_ err] (utils/without-overflow (on-error err)))
  (equals [this x] (identical? this x))
  (hashCode [_] (System/identityHashCode on-success)))

(defn listener
  "Creates a listener which can be registered or cancelled via `add-listener!` and `cancel-listener!`."
  ([on-success]
     (listener on-success (fn [_])))
  ([on-success on-error]
     (Listener. on-success on-error)))

(definterface IMutableDeferred
  (success [x])
  (success [x claim-token])
  (error [x])
  (error [x claim-token])
  (claim [])
  (addListener [listener])
  (cancelListener [listener]))

(defn success!
  "Equivalent to `deliver`, but allows a `claim-token` to be passed in."
  ([^IMutableDeferred deferred x]
     (.success deferred x))
  ([^IMutableDeferred deferred x claim-token]
     (.success deferred x claim-token)))

(defn error!
  "Puts the deferred into an error state."
  ([^IMutableDeferred deferred x]
     (.error deferred x))
  ([^IMutableDeferred deferred x claim-token]
     (.error deferred x claim-token)))

(defn claim!
  "Attempts to claim the deferred for future updates.  If successful, a claim token is returned, otherwise returns `nil`."
  [^IMutableDeferred deferred]
  (.claim deferred))

(defn add-listener!
  "Registers a listener which can be cancelled via `cancel-listener!`.  Unless this is useful, prefer `on-realized`."
  [^IMutableDeferred deferred listener]
  (.addListener deferred listener))

(defn cancel-listener!
  "Cancels a listener which has been registered via `add-listener!`."
  [^IMutableDeferred deferred listener]
  (.cancelListener deferred listener))

(defmacro ^:private set-deferred [val token success? claimed?]
  `(utils/with-lock ~'lock
     (if (when (and
                 (identical? ~(if claimed? ::claimed ::unset) ~'state)
                 ~@(when claimed?
                     `((identical? ~'claim-token ~token))))
           (set! ~'val ~val)
           (set! ~'state ~(if success? ::success ::error))
           true)
       (try
         (clojure.core/loop []
           (if (.isEmpty ~'listeners)
             nil
             (do
               (~(if success? `.onSuccess `.onError) ^IDeferredListener (.pop ~'listeners) ~val)
               (recur))))
         true
         (finally
           (.clear ~'listeners)))
       ~(if claimed?
         `(throw (IllegalStateException.
                   (if (identical? ~'claim-token ~token)
                     "deferred isn't claimed"
                     "invalid claim-token")))
         false))))

(defmacro ^:private deref-deferred [timeout-value & await-args]
  `(condp identical? ~'state
     ::success ~'val
     ::error   (if (instance? Throwable ~'val)
                 (throw ~'val)
                 (throw (ex-info "" {:error ~'val})))
     (let [latch# (CountDownLatch. 1)
            f# (fn [_#] (.countDown latch#))]
        (try
          (on-realized ~'this f# f#)
          (.await latch# ~@await-args)
          (if (identical? ::success ~'state)
            ~'val
            (throw ~'val))
          ~@(when-not (empty? await-args)
              `((catch TimeoutException _#
                  ~timeout-value)))))))

(deftype Deferred
  [^:volatile-mutable val
   ^:volatile-mutable state
   ^:unsynchronized-mutable claim-token
   lock
   ^LinkedList listeners
   ^:volatile-mutable mta
   ^:volatile-mutable consumed?]

  clojure.lang.IObj
  (meta [_] mta)
  clojure.lang.IReference
  (resetMeta [_ m]
    (utils/with-lock lock
      (set! mta m)))
  (alterMeta [_ f args]
    (utils/with-lock lock
      (set! mta (apply f mta args))))

  IMutableDeferred
  (claim [_]
    (utils/with-lock lock
      (when (identical? state ::unset)
        (set! state ::claimed)
        (set! claim-token (Object.)))))
  (addListener [_ listener]
    (set! consumed? true)
    (when-let [f (utils/with-lock lock
                   (condp identical? state
                     ::success #(.onSuccess ^IDeferredListener listener val)
                     ::error   #(.onError ^IDeferredListener listener val)
                     (do
                       (.add listeners listener)
                       nil)))]
      (f))
    true)
  (cancelListener [_ listener]
    (utils/with-lock lock
      (let [state state]
        (if (or (identical? ::unset state)
              (identical? ::set state))
          (.remove listeners listener)
          false))))
  (success [_ x]
    (set-deferred x nil true false))
  (success [_ x token]
    (set-deferred x token true true))
  (error [_ x]
    (set-deferred x nil false false))
  (error [_ x token]
    (set-deferred x token false true))

  clojure.lang.IFn
  (invoke [this x]
    (if (success! this x)
      this
      nil))

  IDeferred
  (realized [_]
    (let [state state]
      (or (identical? ::success state)
        (identical? ::error state))))
  (onRealized [this on-success on-error]
    (add-listener! this (listener on-success on-error)))

  clojure.lang.IPending
  (isRealized [this] (realized? this))

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [this]
    (set! consumed? true)
    (deref-deferred nil))
  (deref [this time timeout-value]
    (set! consumed? true)
    (deref-deferred timeout-value time TimeUnit/MILLISECONDS)))

(deftype SuccessDeferred
  [val
   ^:volatile-mutable mta]

  clojure.lang.IObj
  (meta [_] mta)
  clojure.lang.IReference
  (resetMeta [_ m]
    (set! mta m))
  (alterMeta [this f args]
    (locking this
      (set! mta (apply f mta args))))

  IMutableDeferred
  (claim [_] false)
  (addListener [_ listener]
    (.onSuccess ^IDeferredListener listener val)
    true)
  (cancelListener [_ listener] false)
  (success [_ x] false)
  (success [_ x token] false)
  (error [_ x] false)
  (error [_ x token] false)

  clojure.lang.IFn
  (invoke [this x] nil)

  IDeferred
  (realized [_] true)
  (onRealized [this on-success on-error] (on-success val))

  clojure.lang.IPending
  (isRealized [this] true)

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [this] val)
  (deref [this time timeout-value] val)

  (toString [_] (pr-str val)))

(deftype ErrorDeferred
  [^Throwable error
   ^:volatile-mutable mta
   ^:volatile-mutable consumed?]

  clojure.lang.IObj
  (meta [_] mta)
  clojure.lang.IReference
  (resetMeta [_ m]
    (set! mta m))
  (alterMeta [this f args]
    (locking this
      (set! mta (apply f mta args))))

  IMutableDeferred
  (claim [_] false)
  (addListener [_ listener]
    (set! consumed? true)
    (.onError ^IDeferredListener listener error)
    true)
  (cancelListener [_ listener] false)
  (success [_ x] false)
  (success [_ x token] false)
  (error [_ x] false)
  (error [_ x token] false)

  clojure.lang.IFn
  (invoke [this x] nil)

  IDeferred
  (realized [_] true)
  (onRealized [this on-success on-error] (on-error error))

  clojure.lang.IPending
  (isRealized [this] true)

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [this]
    (set! consumed? true)
    (if (instance? Throwable error)
      (throw error)
      (throw (ex-info "" {:error error}))))
  (deref [this time timeout-value]
    (set! consumed? true)
    (if (instance? Throwable error)
      (throw error)
      (throw (ex-info "" {:error error})))))

(defn deferred
  "Equivalent to Clojure's `deferred`, but also allows asynchronous callbacks to be registered
   via `on-realized`."
  []
  (Deferred. nil ::unset nil (utils/mutex) (LinkedList.) nil false))

(defn success-deferred
  "A deferred which already contains a realized value"
  [val]
  (SuccessDeferred. val nil))

(defn error-deferred
  "A deferred which already contains a realized error"
  [error]
  (ErrorDeferred. error nil false))

(declare chain)

(defn- unwrap [x]
  (if-let [d (->deferred x)]
    (if (realized? d)
      (let [d' (try
                 @d
                 (catch Throwable _
                   ::error))]
        (cond
          (identical? ::error d')
          d

          (deferrable? d')
          (recur d')

          :else
          d'))
      d)
    x))

(defn connect
  "Conveys the realized value of `a` into `b`."
  [a b]
  (assert (instance? IDeferred b) "sink `b` must be a Manifold deferred")
  (let [a (unwrap a)]
    (if (instance? IDeferred a)
      (if (realized? b)
        false
        (do
          (on-realized a
            #(let [a' (unwrap %)]
               (if (instance? IDeferred a')
                 (connect a' b)
                 (success! b a')))
            #(error! b %))
          true))
      (success! b a))))

(defmacro defer
  "Equivalent to Clojure's `future`, but returns a Manifold deferred."
  [& body]
  `(let [d# (deferred)]
     (clojure.core/future
       (try
         (success! d# (do ~@body))
         (catch Throwable e#
           (error! d# e#))))
     d#))

;;;

(defn chain
  "Composes functions, left to right, over the value `x`, returning a deferred containing
   the result.  When composing, either `x` or the returned values may be values which can
   be converted to a deferred, causing the composition to be paused.

   The returned deferred will only be realized once all functions have been applied and their
   return values realized.

       @(chain 1 inc #(future (inc %))) => 3

       @(chain (future 1) inc inc) => 3

   "
  ([x]
     (chain x identity identity identity))
  ([x f]
     (chain x f identity identity))
  ([x f g]
     (chain x f g identity))
  ([x f g h]
     (try
       (let [x' (unwrap x)]
         (if (deferred? x')
           (let [d (deferred)]
             (on-realized x'
               #(let [x (chain % f g h)]
                  (if (deferred? x)
                    (connect x d)
                    (success! d x)))
               #(error! d %))
             d)
           (let [x'' (f x')]
             (if (and (not (identical? x x'')) (deferrable? x''))
               (chain x'' g h identity)
               (let [x''' (g x'')]
                 (if (and (not (identical? x'' x''')) (deferrable? x'''))
                   (chain x''' h identity identity)
                   (let [x'''' (h x''')]
                     (if (and (not (identical? x''' x'''')) (deferrable? x''''))
                       (chain x'''' identity identity identity)
                       (success-deferred x'''')))))))))
       (catch Throwable e
         (error-deferred e))))
  ([x f g h & fs]
     (let [x' (chain x f g h)
           d (deferred)]
       (on-realized x'
         #(utils/without-overflow
            (connect (apply chain % fs) d))
         #(error! d %))
       d)))

(defn catch
  "An equivalent of the catch clause, which takes an `error-handler` function that will be invoked
   with the exception, and whose return value will be yielded as a successful outcome.  If an
   `error-class` is specified, only exceptions of that type will be caught.  If not, all exceptions
   will be caught.

       (-> d
         (chain f g h)
         (catch IOException #(str \"oh no, IO: \" (.getMessage %)))
         (catch             #(str \"something unexpected: \" (.getMessage %))))

    "
  ([d error-handler]
     (catch d Throwable error-handler))
  ([d error-class error-handler]
     (if-not (deferrable? d)
       (throw (IllegalArgumentException. "'catch' expects a value that can be treated as a deferred."))
       (let [d' (deferred)]
         (on-realized d
           #(success! d' %)
           #(try
              (if (instance? error-class %)
                (success! d' (error-handler %))
                (error! d' %))
              (catch Throwable e
                (error! d' e))))
         d'))))

(defn zip
  "Takes a list of values, some of which may be deferreds, and returns a deferred that will yield a list
   of realized values.

        @(zip 1 2 3) => [1 2 3]
        @(zip (future 1) 2 3) => [1 2 3]

  "
  [& deferred-or-values]
  (let [cnt (count deferred-or-values)
        ^objects ary (object-array cnt)
        counter (AtomicInteger. cnt)
        d (deferred)]
    (clojure.core/loop [idx 0, s deferred-or-values]

      (if (empty? s)

        ;; no further results, decrement the counter one last time
        ;; and return the result if everything else has been realized
        (if (zero? (.get counter))
          (success-deferred (or (seq ary) (list)))
          d)

        (let [x (first s)]
          (if-let [x' (->deferred x)]

            (on-realized (chain x')
              (fn [val]
                (aset ary idx val)
                (when (zero? (.decrementAndGet counter))
                  (success! d (seq ary))))
              (fn [err]
                (error! d err)))

            ;; not deferrable - set, decrement, and recur
            (do
              (aset ary idx x)
              (.decrementAndGet counter)))

          (recur (unchecked-inc idx) (rest s)))))))

(defn timeout
  "Takes a deferred, and returns a deferred that will be realized as `timeout-value` (or a
   TimeoutException if none is specified) if the original deferred is not realized within
   `interval` milliseconds."
  ([d interval]
     (let [d' (deferred)]
       (connect d d')
       (time/in interval
         #(error! d'
            (TimeoutException.
              (str "timed out after " interval " milliseconds"))))
       d'))
  ([d interval timeout-value]
     (let [d' (deferred)]
       (connect d d')
       (time/in interval #(success! d' timeout-value))
       d')))

(deftype Recur [s]
  clojure.lang.IDeref
  (deref [_] s))

(defn recur [& args]
  (Recur. args))

(defmacro loop
  "A version of Clojure's loop which allows for asynchronous loops, via `manifold.deferred/recur`.
  `loop` will always return a deferred value, even if the body is synchronous.

   (loop [i 1e6]
     (chain (future i)
       #(if (zero? %)
          %
          (recur (dec %)))))"
  [bindings & body]
  (let [vars (->> bindings (partition 2) (map first))
        vals (->> bindings (partition 2) (map second))
        x-sym (gensym "x")]
    `(let [result# (deferred)]
       ((fn this# [result# ~@vars]
         (clojure.core/loop
           [~@(interleave vars vars)]
           (let [~x-sym (try
                          ~@body
                          (catch Throwable e#
                            (error! result# e#)
                            nil))
                 d# (->deferred ~x-sym)]
             (if (nil? d#)
               (if (instance? Recur ~x-sym)
                 (~'recur
                   ~@(map
                       (fn [n] `(nth @~x-sym ~n))
                       (range (count vars))))
                 (success! result# ~x-sym))
               (if (realized? d#)
                 (let [~x-sym @d#]
                   (if (instance? Recur ~x-sym)
                     (~'recur
                       ~@(map
                           (fn [n] `(nth @~x-sym ~n))
                           (range (count vars))))
                     (success! result# ~x-sym)))
                 (on-realized (chain d#)
                   (fn [x#]
                     (if (instance? Recur x#)
                       (apply this# result# @x#)
                       (success! result# x#)))
                   (fn [err#]
                     (error! result# err#))))))))
        result#
        ~@vals)
       result#)))

;;;

(utils/when-core-async
  (extend-protocol Deferrable

    clojure.core.async.impl.channels.ManyToManyChannel
    (to-deferred [ch]
      (let [d (deferred)]
        (a/go
          (if-let [x (<! ch)]
            (if (instance? Throwable x)
              (error! d x)
              (success! d x))
            (success! d nil)))
        d))))

;;;

(defn- back-references [form]
  (let [syms (atom #{})]
    ((resolve 'riddley.walk/walk-exprs)
      symbol?
      (fn [s]
        (when (some-> ((resolve 'riddley.compiler/locals)) (find s) key meta ::flow-var)
          (swap! syms conj s)))
      form)
    @syms))

(defmacro let-flow
  "A version of `let` where deferred values that are let-bound or closed over can be treated
   as if they are realized values.  The body will only be executed once all of the let-bound
   values, even ones only used for side effects, have been computed.

   Returns a deferred value, representing the value returned by the body.

      (let-flow [x (future 1)]
        (+ x 1))

      (let-flow [x (future 1)
                 y (future (+ x 1))]
        (+ y 1))

      (let [x (future 1)]
        (let-flow [y (future (+ x 1))]
          (+ y 1)))"
  [bindings & body]
  (require 'riddley.walk)
  (let [locals (keys ((resolve 'riddley.compiler/locals)))
        vars (->> bindings (partition 2) (map first))
        vars' (->> vars (concat locals) (map #(vary-meta % assoc ::flow-var true)))
        gensyms (repeatedly (count vars') gensym)
        vals' (->> bindings (partition 2) (map second) (concat locals))
        gensym->deps (zipmap
                       gensyms
                       (->> (count vars')
                         range
                         (map
                           (fn [n]
                             `(let [~@(interleave (take n vars') (repeat nil))
                                    ~(nth vars' n) ~(nth vals' n)])))
                         (map back-references)))
        var->gensym (zipmap vars' gensyms)]
    `(let [~@(interleave
               gensyms
               (map
                 (fn [n var val gensym]
                   (let [var->gensym (zipmap (take n vars') gensyms)
                         deps (gensym->deps gensym)]
                     (if (empty? deps)
                       val
                       `(chain (zip ~@(map var->gensym deps))
                          (fn [[~@deps]]
                            ~val)))))
                 (range)
                 vars'
                 vals'
                 gensyms))]
       ~(let [dep? (set/union
                     (back-references `(let [~@(interleave
                                                 vars'
                                                 (repeat nil))]
                                         ~@body))
                     (apply set/union (vals gensym->deps)))
              vars' (filter dep? vars')
              gensyms' (map var->gensym vars')]
          `(chain (zip ~@gensyms')
             (fn [[~@gensyms']]
               (let [~@(interleave vars' gensyms')]
                 ~@body)))))))
