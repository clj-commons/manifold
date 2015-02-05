(ns
  ^{:author "Zach Tellman"
    :doc "Methods for creating, transforming, and interacting with asynchronous values."}
  manifold.deferred
  (:refer-clojure :exclude [realized? loop future])
  (:require
    [clojure.tools.logging :as log]
    [manifold
     [utils :as utils]
     [time :as time]
     [debug :as debug]]
    [clojure.set :as set])
  (:import
    [java.util
     LinkedList]
    [java.io
     Writer]
    [java.util.concurrent
     Future
     TimeoutException
     TimeUnit
     ConcurrentHashMap
     CountDownLatch
     Executor]
    [java.util.concurrent.locks
     Lock]
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
  (executor [])
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

(def ^:private satisfies-deferrable?
  (utils/fast-satisfies #'Deferrable))

(defn deferrable? [x]
  (or
    (instance? IDeferred x)
    (instance? Future x)
    (instance? IPending x)
    (satisfies-deferrable? x)))

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
  "Transforms `x` into a deferred if possible, or returns `default-val`.  If no default value
   is given, an `IllegalArgumentException` is thrown."
  ([x]
     (let [x' (->deferred x ::none)]
       (if (identical? ::none x')
         (throw
           (IllegalArgumentException.
             (str "cannot convert " (.getCanonicalName (class x)) " to deferred.")))
         x')))
  ([x default-val]
     (cond
       (deferred? x)
       x

       (satisfies-deferrable? x)
       (to-deferred x)

       (instance? Future x)
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

       (instance? IPending x)
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

       :else
       default-val)))

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

(defmacro ^:private set-deferred [val token success? claimed? executor]
  `(if (utils/with-lock* ~'lock
         (when (and
                 (identical? ~(if claimed? ::claimed ::unset) ~'state)
                 ~@(when claimed?
                     `((identical? ~'claim-token ~token))))
           (set! ~'val ~val)
           (set! ~'state ~(if success? ::success ::error))
           true))
     (do
       (clojure.core/loop []
         (if (.isEmpty ~'listeners)
           nil
           (do
             (try
               (let [^IDeferredListener l# (.pop ~'listeners)]
                 (if (nil? ~executor)
                   (~(if success? `.onSuccess `.onError) ^IDeferredListener l# ~val)
                   (.execute ~(with-meta executor {:tag "java.util.concurrent.Executor"})
                     (fn []
                       (try
                         (~(if success? `.onSuccess `.onError) l# ~val)
                         (catch Throwable e#
                           (log/error e# "error in deferred handler")))))))
               (catch Throwable e#
                 (log/error e# "error in deferred handler")))
             (recur))))
       true)
     ~(if claimed?
        `(throw (IllegalStateException.
                  (if (identical? ~'claim-token ~token)
                    "deferred isn't claimed"
                    "invalid claim-token")))
        false)))

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

(deftype DeferredState
  [val state claim-token consumed?])

(deftype Deferred
  [^:volatile-mutable val
   ^:volatile-mutable state
   ^:volatile-mutable claim-token
   ^Lock lock
   ^LinkedList listeners
   ^:volatile-mutable mta
   ^:volatile-mutable consumed?
   ^Executor executor]

  Object
  #_(finalize [_]
    (when (and
            (identical? ::error state)
            (not consumed?)
            debug/*dropped-error-logging-enabled?*)
      (log/warn val "unconsumed deferred in error state")))

  clojure.lang.IObj
  (meta [_] mta)
  clojure.lang.IReference
  (resetMeta [_ m]
    (utils/with-lock* lock
      (set! mta m)))
  (alterMeta [_ f args]
    (utils/with-lock* lock
      (set! mta (apply f mta args))))

  IMutableDeferred
  (claim [_]
    (utils/with-lock* lock
      (when (identical? state ::unset)
        (set! state ::claimed)
        (set! claim-token (Object.)))))
  (addListener [_ listener]
    (set! consumed? true)
    (when-let [f (utils/with-lock* lock
                   (condp identical? state
                     ::success #(.onSuccess ^IDeferredListener listener val)
                     ::error   #(.onError ^IDeferredListener listener val)
                     (do
                       (.add listeners listener)
                       nil)))]
      (if executor
        (.execute executor f)
        (f)))
    true)
  (cancelListener [_ listener]
    (utils/with-lock* lock
      (let [state state]
        (if (or (identical? ::unset state)
              (identical? ::set state))
          (.remove listeners listener)
          false))))
  (success [_ x]
    (set-deferred x nil true false executor))
  (success [_ x token]
    (set-deferred x token true true executor))
  (error [_ x]
    (set-deferred x nil false false executor))
  (error [_ x token]
    (set-deferred x token false true executor))

  clojure.lang.IFn
  (invoke [this x]
    (if (success! this x)
      this
      nil))

  IDeferred
  (executor [_]
    executor)
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
   ^:volatile-mutable mta
   ^Executor executor]

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
    (if (nil? executor)
      (.onSuccess ^IDeferredListener listener val)
      (.execute executor #(.onSuccess ^IDeferredListener listener val)))
    true)
  (cancelListener [_ listener] false)
  (success [_ x] false)
  (success [_ x token] false)
  (error [_ x] false)
  (error [_ x token] false)

  clojure.lang.IFn
  (invoke [this x] nil)

  IDeferred
  (executor [_] executor)
  (realized [this] true)
  (onRealized [this on-success on-error]
    (if executor
      (.execute executor #(on-success val))
      (on-success val)))

  clojure.lang.IPending
  (isRealized [this] (realized? this))

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [this] val)
  (deref [this time timeout-value] val))

(deftype ErrorDeferred
  [^Throwable error
   ^:volatile-mutable mta
   ^:volatile-mutable consumed?
   ^Executor executor]

  Object
  (finalize [_]
    (when (and
            (not consumed?)
            debug/*dropped-error-logging-enabled?*)
      (log/warn error "unconsumed deferred in error state")))

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
  (executor [_] executor)
  (realized [_] true)
  (onRealized [this on-success on-error]
    (if (nil? executor)
      (on-error error)
      (.execute executor #(on-error executor))))

  clojure.lang.IPending
  (isRealized [this] (realized? this))

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
  "Equivalent to Clojure's `promise`, but also allows asynchronous callbacks to be registered
   and composed via `chain`."
  ([]
     (Deferred. nil ::unset nil (utils/mutex) (LinkedList.) nil false nil))
  ([executor]
     (Deferred. nil ::unset nil (utils/mutex) (LinkedList.) nil false executor)))

(let [true-d   (SuccessDeferred. true nil nil)
      false-d  (SuccessDeferred. false nil nil)
      nil-d    (SuccessDeferred. nil nil nil)]
  (defn success-deferred
    "A deferred which already contains a realized value"
    ([val]
       (condp identical? val
         true true-d
         false false-d
         nil nil-d
         (SuccessDeferred. val nil nil)))
    ([val executor]
       (SuccessDeferred. val nil executor))))

(defn error-deferred
  "A deferred which already contains a realized error"
  ([error]
     (ErrorDeferred. error nil false nil))
  ([error executor]
     (ErrorDeferred. error nil false executor)))

(declare chain)

(defn- unwrap' [x]
  (if (deferred? x)
    (if (realized? x)
      (let [x' (try
                 @x
                 (catch Throwable _
                   ::error))]
        (if (identical? ::error x')
          x
          (recur x')))
      x)
    x))

(defn- unwrap [x]
  (let [d (->deferred x nil)]
    (if (nil? d)
      x
      (if (realized? d)
        (let [x' (try
                   @x
                   (catch Throwable _
                     ::error))]
          (if (identical? ::error x')
            d
            (recur x')))
        d))))

(defn connect
  "Conveys the realized value of `a` into `b`."
  [a b]
  (assert (deferred? b) "sink `b` must be a Manifold deferred")
  (let [a (unwrap a)]
    (if (instance? IDeferred a)
      (if (realized? b)
        false
        (do
          (on-realized a
            #(let [a' (unwrap %)]
               (if (deferred? a')
                 (connect a' b)
                 (success! b a')))
            #(error! b %))
          true))
      (success! b a))))

(defn onto
  "Returns a deferred whose callbacks will be run on `executor`."
  [^IDeferred d executor]
  (if (identical? executor (.executor d) )
    d
    (connect d (deferred executor))))

(defmacro future-with
  "Equivalent to Clojure's `future`, but allows specification of the executor
   and returns a Manifold deferred."
  [executor & body]
  `(let [d# (deferred)]
     (utils/future-with ~executor
       (try
         (success! d# (do ~@body))
         (catch Throwable e#
           (error! d# e#))))
     d#))

(defmacro future
  "Equivalent to Clojure's `future`, but returns a Manifold deferred."
  [& body]
  `(future-with @utils/execute-pool ~@body))

;;;

(defn- chain'-
  ([d x]
     (chain'- d x identity identity))
  ([d x f]
     (chain'- d x f identity))
  ([d x f g]
     (try
       (let [x' (unwrap' x)]

         (if (deferred? x')
           (let [d (or d (deferred))]
             (on-realized x'
               #(chain'- d % f g)
               #(error! d %))
             d)

           (let [x'' (f x')]
             (if (deferred? x'')
               (chain'- d x'' g identity)
               (let [x''' (g x'')]
                 (if (deferred? x''')
                   (chain'- d x''' identity identity)
                   (if (nil? d)
                     (success-deferred x''')
                     (success! d x'''))))))))
       (catch Throwable e
         (error! d e))))
  ([d x f g & fs]
     (when (or (nil? d) (not (realized? d)))
       (let [d (or d (deferred))]
         (clojure.core/loop [x x, fs (list* f g fs)]
           (if (empty? fs)
             (success! d x)
             (let [[f g & fs] fs
                   d' (deferred)
                   _ (if (nil? g)
                       (chain'- d' x f)
                       (chain'- d' x f g))]
               (if (realized? d')
                 (when (try
                         @d'
                         true
                         (catch Throwable e
                           (error! d e)
                           false))
                   (recur @d' fs))
                 (on-realized d'
                   #(apply chain'- d % fs)
                   #(error! d %))))))
         d))))

(defn- chain-
  ([d x]
     (chain- d x identity identity))
  ([d x f]
     (chain- d x f identity))
  ([d x f g]
     (if (or (nil? d) (not (realized? d)))
       (try
         (let [x' (unwrap x)]

           (if (deferred? x')
             (let [d (or d (deferred))]
               (on-realized x'
                 #(chain- d % f g)
                 #(error! d %))
               d)

             (let [x'' (f x')]
               (if (and (not (identical? x x'')) (deferrable? x''))
                 (chain- d x'' g identity)
                 (let [x''' (g x'')]
                   (if (and (not (identical? x'' x''')) (deferrable? x'''))
                     (chain- d x''' identity identity)
                     (if (nil? d)
                       (success-deferred x''')
                       (success! d x'''))))))))
         (catch Throwable e
           (if (nil? d)
             (error-deferred e)
             (do
               (error! d e)
               d))))
       d))
  ([d x f g & fs]
     (when (or (nil? d) (not (realized? d)))
       (let [d (or d (deferred))]
         (clojure.core/loop [x x, fs (list* f g fs)]
           (if (empty? fs)
             (success! d x)
             (let [[f g & fs] fs
                   d' (deferred)
                   _ (if (nil? g)
                       (chain- d' x f)
                       (chain- d' x f g))]
               (if (realized? d')
                 (when (try
                         @d'
                         true
                         (catch Throwable e
                           (error! d e)
                           false))
                   (recur @d' fs))
                 (on-realized d'
                   #(utils/without-overflow
                      (apply chain- d % fs))
                   #(error! d %))))))
         d))))

(defn chain'
  "Like `chain`, but does not coerce deferrable values.  This is useful both when coercion
   is undesired, or for 2-4x better performance than `chain`."
  ([x]
     (chain'- nil x identity identity))
  ([x f]
     (chain'- nil x f identity))
  ([x f g]
     (chain'- nil x f g))
  ([x f g & fs]
     (apply chain'- nil x f g fs)))

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
     (chain- nil x identity identity))
  ([x f]
     (chain- nil x f identity))
  ([x f g]
     (chain- nil x f g))
  ([x f g & fs]
     (apply chain- nil x f g fs)))

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
  ([x error-handler]
     (catch x Throwable error-handler))
  ([x error-class error-handler]
     (if-let [d (chain (->deferred x nil))]
       (if (realized? d)

         ;; see what's inside
         (try
           @x
           x
           (catch Throwable e
             (if (instance? error-class e)
               (try
                 (success-deferred (error-handler e))
                 (catch Throwable e
                   (error-deferred e)))
               x)))

         (let [d' (deferred)]
           (on-realized d
             #(success! d' %)
             #(try
                (if (instance? error-class %)
                  (success! d' (error-handler %))
                  (error! d' %))
                (catch Throwable e
                  (error! d' e))))
           d'))
       x)))

(defn catch'
  "Like `catch`, but does not coerce deferrable values."
  ([x error-handler]
     (catch' x Throwable error-handler))
  ([x error-class error-handler]
     (let [x (chain' x)]
       (if-not (instance? IDeferred x)
         x
         (if (realized? x)

           ;; see what's inside
           (try
             @x
             x
             (catch Throwable e
               (if (instance? error-class e)
                 (try
                   (success-deferred (error-handler e))
                   (catch Throwable e
                     (error-deferred e)))
                 x)))

           (let [d' (deferred)]
             (on-realized x
               #(success! d' %)
               #(try
                  (if (instance? error-class %)
                    (success! d' (error-handler %))
                    (error! d' %))
                  (catch Throwable e
                    (error! d' e))))
             d'))))))

(defn finally
  "An equivalent of the finally clause, which takes a no-arg side-effecting function that executes
   no matter what the result."
  [x f]
  (if-let [d (->deferred x nil)]
    (if (realized? d)
      (try
        (f)
        d
        (catch Throwable e
          (error-deferred e)))
      (chain' d
        (fn [x']
          (f)
          x')))
    (try
      (f)
      x
      (catch Throwable e
        (error-deferred e)))))

(defn finally'
  "Like `finally`, but doesn't coerce deferrable values."
  [x f]
  (if (instance? IDeferred x)
    (if (realized? x)
      (try
        (f)
        x
        (catch Throwable e
          (error-deferred e)))
      (chain' x
        (fn [x']
          (f)
          x')))
    (try
      (f)
      x
      (catch Throwable e
        (error-deferred e)))))

(defn zip
  "Takes a list of values, some of which may be deferrable, and returns a deferred that will yield a list
   of realized values.

        @(zip 1 2 3) => [1 2 3]
        @(zip (future 1) 2 3) => [1 2 3]

  "
  ([x]
     (chain x vector))
  ([a & rst]
     (let [deferred-or-values (list* a rst)
           cnt (count deferred-or-values)
           ^objects ary (object-array cnt)
           counter (AtomicInteger. cnt)]
       (clojure.core/loop [d nil, idx 0, s deferred-or-values]

         (if (empty? s)

           ;; no further results, decrement the counter one last time
           ;; and return the result if everything else has been realized
           (if (zero? (.get counter))
             (success-deferred (or (seq ary) (list)))
             d)

           (let [x (first s)
                 rst (rest s)
                 idx' (unchecked-inc idx)]
             (if-let [x' (->deferred x nil)]

               (if (realized? x')
                 (do
                   (aset ary idx @x')
                   (.decrementAndGet counter)
                   (recur d idx' rst))

                 (let [d (or d (deferred))]
                   (on-realized (chain' x')
                     (fn [val]
                       (aset ary idx val)
                       (when (zero? (.decrementAndGet counter))
                         (success! d (seq ary))))
                     (fn [err]
                       (error! d err)))
                   (recur d idx' rst)))

               ;; not deferrable - set, decrement, and recur
               (do
                 (aset ary idx x)
                 (.decrementAndGet counter)
                 (recur d idx' rst)))))))))

(defn zip'
  "Like `zip`, but only unwraps Manifold deferreds."
  ([x]
     (chain' x vector))
  ([a & rst]
     (let [deferred-or-values (list* a rst)
           cnt (count deferred-or-values)
           ^objects ary (object-array cnt)
           counter (AtomicInteger. cnt)]
       (clojure.core/loop [d nil, idx 0, s deferred-or-values]

         (if (empty? s)

           ;; no further results, decrement the counter one last time
           ;; and return the result if everything else has been realized
           (if (zero? (.get counter))
             (success-deferred (or (seq ary) (list)))
             d)

           (let [x (first s)
                 rst (rest s)
                 idx' (unchecked-inc idx)]
             (if (deferred? x)

               (if (realized? x)
                 (do
                   (aset ary idx @x)
                   (.decrementAndGet counter)
                   (recur d idx' rst))

                 (let [d (or d (deferred))]
                   (on-realized (chain' x)
                     (fn [val]
                       (aset ary idx val)
                       (when (zero? (.decrementAndGet counter))
                         (success! d (seq ary))))
                     (fn [err]
                       (error! d err)))
                   (recur d idx' rst)))

               ;; not deferrable - set, decrement, and recur
               (do
                 (aset ary idx x)
                 (.decrementAndGet counter)
                 (recur d idx' rst)))))))))

(defn timeout!
  "Takes a deferred, and sets a timeout on it, such that it will be realized as `timeout-value`
   (or a TimeoutException if none is specified) if it is not realized in `interval` ms.  Returns
   the deferred that was passed in.

   This will act directly on the deferred value passed in.  If the deferred represents a value
   returned by `chain`, all actions not yet completed will be short-circuited upon timeout."
  ([d interval]
     (cond
       (or (nil? interval) (realized? d))
       nil

       (not (pos? interval))
       (error! d
         (TimeoutException.
           (str "timed out after " interval " milliseconds")))

       :else
       (time/in interval
        #(error! d
           (TimeoutException.
             (str "timed out after " interval " milliseconds")))))
     d)
  ([d interval timeout-value]
     (cond
       (or (nil? interval) (realized? d))
       nil

       (not (pos? interval))
       (success! d timeout-value)

       :else
       (time/in interval #(success! d timeout-value)))
     d))

(deftype Recur [s]
  clojure.lang.IDeref
  (deref [_] s))

(defn recur
  "A special recur that can be used with `manifold.deferred/loop`."
  [& args]
  (Recur. args))

(defmacro loop
  "A version of Clojure's loop which allows for asynchronous loops, via `manifold.deferred/recur`.
  `loop` will always return a deferred value, even if the body is synchronous.  Note that `loop` does **not** coerce values to deferreds, actual Manifold deferreds must be used.

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
                            nil))]
             (if (deferred? ~x-sym)
               (if (realized? ~x-sym)
                 (let [~x-sym @~x-sym]
                   (if (instance? Recur ~x-sym)
                     (~'recur
                       ~@(map
                           (fn [n] `(nth @~x-sym ~n))
                           (range (count vars))))
                     (success! result# ~x-sym)))
                 (on-realized (chain ~x-sym)
                   (fn [x#]
                     (if (instance? Recur x#)
                       (apply this# result# @x#)
                       (success! result# x#)))
                   (fn [err#]
                     (error! result# err#))))
               (if (instance? Recur ~x-sym)
                 (~'recur
                   ~@(map
                       (fn [n] `(nth @~x-sym ~n))
                       (range (count vars))))
                 (success! result# ~x-sym))))))
        result#
        ~@vals)
       result#)))

;;;

(utils/when-core-async
  (extend-protocol Deferrable

    clojure.core.async.impl.channels.ManyToManyChannel
    (to-deferred [ch]
      (let [d (deferred)]
        (a/take! ch
          (fn [msg]
            (if (instance? Throwable msg)
              (error! d msg)
              (success! d msg))))
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

(defmethod print-method IDeferred [o ^Writer w]
  (.write w
    (str
      "<< "
      (if (realized? o)
       (try
         (let [x @o]
           (pr-str x))
         (catch Throwable e
           (str "ERROR: " (pr-str e))))
       "\u2026")
      " >>")))

(prefer-method print-method IDeferred IDeref)

;;;

(alter-meta! #'->Deferred assoc :private true)
(alter-meta! #'->DeferredState assoc :private true)
(alter-meta! #'->ErrorDeferred assoc :private true)
(alter-meta! #'->SuccessDeferred assoc :private true)
(alter-meta! #'->Listener assoc :private true)
(alter-meta! #'->Recur assoc :private true)
