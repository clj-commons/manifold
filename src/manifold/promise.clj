(ns manifold.promise
  (:refer-clojure :exclude [realized? promise future])
  (:require
    [manifold.utils :as utils]
    [manifold.time :as time])
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

(defprotocol Promisable
  (^:private to-promise [_] "Provides a conversion mechanism to manifold promises."))

;; implies IDeref, IBlockingDeref, IPending
(definterface IPromise
  (^boolean realized [])
  (onRealized [on-success on-error]))

(definline ^:private realized?
  "Returns true if the manifold promise is realized."
  [x]
  `(.realized ~(with-meta x {:tag "manifold.promise.IPromise"})))

(definline on-realized
  "Registers callbacks with the manifold promise for both success and error outcomes."
  [x on-success on-error]
  `(.onRealized ~(with-meta x {:tag "manifold.promise.IPromise"}) ~on-success ~on-error))

(definline promise?
  "Returns true if the object is an instance of a Manifold promise."
  [x]
  `(instance? IPromise ~x))

(let [^ConcurrentHashMap classes (ConcurrentHashMap.)]
  (add-watch #'Promisable ::memoization (fn [& _] (.clear classes)))
  (defn promisable?
    "Returns true if the object can be turned into a Manifold promise."
    [x]
    (if (nil? x)
      false
      (let [cls (class x)
            val (.get classes cls)]
        (if (nil? val)
          (let [val (or (instance? IPromise x)
                      (instance? Future x)
                      (instance? IDeref x)
                      (satisfies? Promisable x))]
            (.put classes cls val)
            val)
          val)))))

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
    (utils/defer
      (try
        (on-success @x)
        (catch Throwable e
          (on-error e))))))

(defn ->promise
  "Transforms `x` into an async-promise, or returns `nil` if no such transformation is
   possible."
  [x]
  (condp instance? x
    IPromise
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
        IPromise
        (realized [_]
          (or (.isDone x) (.isCancelled x)))
        (onRealized [_ on-success on-error]
          (register-future-callbacks x on-success on-error))))

    IDeref
    (if (instance? IPending x)

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
        IPromise
        (realized [_]
          (.isRealized ^IPending x))
        (onRealized [_ on-success on-error]
          (register-future-callbacks x on-success on-error)
          nil))

      ;; we don't know when we're pending, but make sure we're not
      ;; pending once it's dereferenced
      (let [pending? (AtomicBoolean. true)]
        (reify
          IDeref
          (deref [_]
            (try
              (.deref ^IDeref x)
              (finally
                (.set pending? false))))
          IBlockingDeref
          (deref [_ time timeout-value]
            (try
              (let [v (.deref ^IBlockingDeref x time timeout-value)]
                (when-not (identical? timeout-value v)
                  (.set pending? false))
                v)
              (catch Throwable e
                (.set pending? false)
                (throw e))))
          IPending
          (isRealized [_]
            (not (.get pending?)))
          IPromise
          (realized [_]
            (not (.get pending?)))
          (onRealized [f on-success on-error]
            (utils/defer
              (try
                (on-success (.deref ^IDeref x))
                (catch Throwable e
                  (on-error e))
                (finally
                  (.set pending? false))))))))

    (when (promisable? x)
      (to-promise x))))

;;;

(definterface IPromiseListener
  (onSuccess [x])
  (onError [err]))

(deftype Listener [on-success on-error]
  IPromiseListener
  (onSuccess [_ x] (on-success x))
  (onError [_ err] (on-error err))
  (equals [this x] (identical? this x))
  (hashCode [_] (System/identityHashCode on-success)))

(defn listener
  "Creates a listener which can be registered or cancelled via `add-listener!` and `cancel-listener!`."
  ([on-success]
     (listener on-success (fn [_])))
  ([on-success on-error]
     (Listener. on-success on-error)))

(definterface IMutablePromise
  (success [x])
  (success [x claim-token])
  (error [x])
  (error [x claim-token])
  (claim [])
  (addListener [listener])
  (cancelListener [listener]))

(defn success!
  "Equivalent to `deliver`, but allows a `claim-token` to be passed in."
  ([^IMutablePromise promise x]
     (.success promise x))
  ([^IMutablePromise promise x claim-token]
     (.success promise x claim-token)))

(defn error!
  "Puts the promise into an error state."
  ([^IMutablePromise promise x]
     (.error promise x))
  ([^IMutablePromise promise x claim-token]
     (.error promise x claim-token)))

(defn claim!
  "Attempts to claim the promise for future updates.  If successful, a claim token is returned, otherwise returns `nil`."
  [^IMutablePromise promise]
  (.claim promise))

(defn add-listener!
  "Registers a listener which can be cancelled via `cancel-listener!`.  Unless this is useful, prefer `on-realized`."
  [^IMutablePromise promise listener]
  (.addListener promise listener))

(defn cancel-listener!
  "Cancels a listener which has been registered via `add-listener!`."
  [^IMutablePromise promise listener]
  (.cancelListener promise listener))

(defmacro ^:private set-promise [val token success? claimed?]
  `(utils/with-lock ~'lock
     (if (when (and
                 (identical? ~(if claimed? ::claimed ::unset) ~'state)
                 ~@(when claimed?
                     `((identical? ~'claim-token ~token))))
           (set! ~'val ~val)
           (set! ~'state ~(if success? ::success ::error))
           true)
       (try
         (loop []
           (if (.isEmpty ~'listeners)
             nil
             (do
               (~(if success? `.onSuccess `.onError) ^IPromiseListener (.pop ~'listeners) ~val)
               (recur))))
         true
         (finally
           (.clear ~'listeners)))
       ~(if claimed?
         `(throw (IllegalStateException.
                   (if (identical? ~'claim-token ~token)
                     "promise isn't claimed"
                     "invalid claim-token")))
         false))))

(defmacro ^:private deref-promise [timeout-value & await-args]
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

(deftype Promise
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

  IMutablePromise
  (claim [_]
    (utils/with-lock lock
      (when (identical? state ::unset)
        (set! state ::claimed)
        (set! claim-token (Object.)))))
  (addListener [_ listener]
    (set! consumed? true)
    (when-let [f (utils/with-lock lock
                   (condp identical? state
                     ::success #(.onSuccess ^IPromiseListener listener val)
                     ::error   #(.onError ^IPromiseListener listener val)
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
    (set-promise x nil true false))
  (success [_ x token]
    (set-promise x token true true))
  (error [_ x]
    (set-promise x nil false false))
  (error [_ x token]
    (set-promise x token false true))

  clojure.lang.IFn
  (invoke [this x]
    (if (success! this x)
      this
      nil))

  IPromise
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
    (deref-promise nil))
  (deref [this time timeout-value]
    (set! consumed? true)
    (deref-promise timeout-value time TimeUnit/MILLISECONDS))

  (toString [_]
    (condp state
      ::success (pr-str val)
      ::error (str "ERROR: " (pr-str val))
      "...")))

(deftype SuccessPromise
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

  IMutablePromise
  (claim [_] false)
  (addListener [_ listener]
    (.onSuccess ^IPromiseListener listener val)
    true)
  (cancelListener [_ listener] false)
  (success [_ x] false)
  (success [_ x token] false)
  (error [_ x] false)
  (error [_ x token] false)

  clojure.lang.IFn
  (invoke [this x] nil)

  IPromise
  (realized [_] true)
  (onRealized [this on-success on-error] (on-success val))

  clojure.lang.IPending
  (isRealized [this] true)

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [this] val)
  (deref [this time timeout-value] val)

  (toString [_] (pr-str val)))

(deftype ErrorPromise
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

  IMutablePromise
  (claim [_] false)
  (addListener [_ listener]
    (set! consumed? true)
    (.onError ^IPromiseListener listener error)
    true)
  (cancelListener [_ listener] false)
  (success [_ x] false)
  (success [_ x token] false)
  (error [_ x] false)
  (error [_ x token] false)

  clojure.lang.IFn
  (invoke [this x] nil)

  IPromise
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
      (throw (ex-info "" {:error error}))))

  (toString [_] (str "ERROR: " (pr-str error))))

(defn promise
  "Equivalent to Clojure's `promise`, but also allows asynchronous callbacks to be registered
   via `on-realized`."
  []
  (Promise. nil ::unset nil (utils/mutex) (LinkedList.) nil false))

(defn success-promise
  "A promise which already contains a realized value"
  [val]
  (SuccessPromise. val nil))

(defn error-promise
  "A promise which already contains a realized error"
  [error]
  (ErrorPromise. error nil false))

(defn- unwrap [x]
  (if (promisable? x)
    (let [p (->promise x)]
      (if (realized? p)
        (let [p' (try
                   @p
                   (catch Throwable _
                     ::error))]
          (cond
            (identical? ::error p')
            p

            (promisable? p')
            (recur p')

            :else
            p'))
        p))
    x))

(defn connect
  "Conveys the realized value of `a` into `b`."
  [a b]
  (assert (instance? IPromise b) "sink `b` must be a Manifold promise")
  (let [a (unwrap a)]
    (if (not (instance? IPromise a))
      (success! b a)
      (if (realized? b)
        false
        (do
          (on-realized a
            #(success! b %)
            #(error! b %))
          true)))))

(defmacro future
  "Equivalent to Clojure's `future`, but returns a Manifold promise."
  [& body]
  `(let [p# (promise)]
     (clojure.core/future
       (try
         (success! p# (do ~@body))
         (catch Throwable e#
           (error! p# e#))))
     p#))

;;;

(defn chain
  "Composes functions, left to right, over the value `x`, returning a promise containing
   the result.  When composing, either `x` or the returned values may be values which can
   be converted to a promise, causing the composition to be paused.

   The returned promise will only be realized once all functions have been applied and their
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
         (if (promise? x')
           (let [p (promise)]
             (on-realized x'
               #(let [x (chain % f g h)]
                  (if (promise? x)
                    (connect x p)
                    (success! p x)))
               #(error! p %))
             p)
           (let [x'' (f x')]
             (if (and (not (identical? x x'')) (promisable? x''))
               (chain x'' g h identity)
               (let [x''' (g x'')]
                 (if (and (not (identical? x'' x''')) (promisable? x'''))
                   (chain x''' h identity identity)
                   (let [x'''' (h x''')]
                     (if (and (not (identical? x''' x'''')) (promisable? x''''))
                       (chain x'''' identity identity identity)
                       (success-promise x'''')))))))))
       (catch Throwable e
         (error-promise e))))
  ([x f g h & fs]
     (let [x' (chain x f g h)
           p (promise)]
       (on-realized x'
         #(utils/without-overflow
            (connect (apply chain % fs) p))
         #(error! p %))
       p)))

(defn catch
  "An equivalent of the catch clause, which takes an `error-handler` function that will be invoked
   with the exception, and whose return value will be yielded as a successful outcome.  If an
   `error-class` is specified, only exceptions of that type will be caught.  If not, all exceptions
   will be caught.

       (-> p
         (chain f g h)
         (catch IOException #(str \"oh no, IO: \" (.getMessage %)))
         (catch             #(str \"something unexpected: \" (.getMessage %))))

    "
  ([p error-handler]
     (catch p Throwable error-handler))
  ([p error-class error-handler]
     (if-not (promisable? p)
       (throw (IllegalArgumentException. "'catch' expects a value that can be treated as a promise."))
       (let [p' (promise)]
         (on-realized p
           #(success! p' %)
           #(try
              (if (instance? error-class %)
                (success! p' (error-handler %))
                (error! p' %))
              (catch Throwable e
                (error! p' e))))
         p'))))

(defn zip
  "Takes a list of values, some of which may be promises, and returns a promise that will yield a list
   of realized values.

        @(zip 1 2 3) => [1 2 3]
        @(zip (future 1) 2 3) => [1 2 3]

  "
  [& promise-or-values]
  (let [cnt (count promise-or-values)
        ^objects ary (object-array cnt)
        counter (AtomicInteger. cnt)
        p (promise)]
    (loop [idx 0, s promise-or-values]

      (if (empty? s)

        ;; no further results, decrement the counter one last time
        ;; and return the result if everything else has been realized
        (if (zero? (.get counter))
          (success-promise (seq ary))
          p)

        (let [x (first s)]
          (if-let [x' (->promise x)]

            (on-realized x'
              (fn [val]
                (aset ary idx val)
                (when (zero? (.decrementAndGet counter))
                  (success! p (seq ary))))
              (fn [err]
                (error! p err)))

            ;; not promisable - set, decrement, and recur
            (do
              (aset ary idx x)
              (.decrementAndGet counter)))

          (recur (unchecked-inc idx) (rest s)))))))

(defn timeout
  "Takes a promise, and returns a promise that will be realized as `timeout-value` (or a
   TimeoutException if none is specified) if the original promise is not realized within
   `interval` milliseconds."
  ([p interval]
     (let [p' (promise)]
       (connect p p')
       (time/in interval
         #(error! p'
            (TimeoutException.
              (str "timed out after " interval " milliseconds"))))
       p'))
  ([p interval timeout-value]
     (let [p' (promise)]
       (connect p p')
       (time/in interval #(success! p' timeout-value))
       p')))

(utils/when-core-async
  (extend-protocol Promisable

    clojure.core.async.impl.channels.ManyToManyChannel
    (to-promise [ch]
      (let [p (promise)]
        (a/go
          (if-let [x (<! ch)]
            (if (instance? Throwable x)
              (error! p x)
              (success! p x))
            (success! p nil)))
        p))))
