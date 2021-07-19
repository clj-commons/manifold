(ns
  ^{:author "Zach Tellman"
    :doc "Methods for creating, transforming, and interacting with asynchronous values."}
  manifold.deferred
  (:refer-clojure :exclude [realized? loop future])
  (:require
    [clojure.tools.logging :as log]
    [riddley.walk :as walk]
    [riddley.compiler :as compiler]
    [manifold
     [executor :as ex]
     [utils :as utils :refer [defprotocol+ deftype+ definterface+]]
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
     AtomicInteger
     AtomicLong]
    [clojure.lang
     IPending
     IBlockingDeref
     IDeref]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(defprotocol+ Deferrable
  (^:private to-deferred [_] "Provides a conversion mechanism to manifold deferreds."))

;; implies IDeref, IBlockingDeref, IPending
(definterface+ IDeferred
  (executor [])
  (^boolean realized [])
  (onRealized [on-success on-error])
  (successValue [default])
  (errorValue [default]))

(definline realized?
  "Returns true if the manifold deferred is realized."
  [x]
  `(.realized ~(with-meta x {:tag "manifold.deferred.IDeferred"})))

(definline ^:no-doc success-value [x default-value]
  `(.successValue ~(with-meta x {:tag "manifold.deferred.IDeferred"}) ~default-value))

(definline ^:no-doc error-value [x default-value]
  `(.errorValue ~(with-meta x {:tag "manifold.deferred.IDeferred"}) ~default-value))

(defmacro ^:no-doc success-error-unrealized
  [deferred
   success-value
   success-clause
   error-value
   error-clause
   unrealized-clause]
  `(let [d# ~deferred
         ~success-value (success-value d# ::none)]
     (if (identical? ::none ~success-value)
       (let [~error-value (error-value d# ::none)]
         (if (identical? ::none ~error-value)
           ~unrealized-clause
           ~error-clause))
       ~success-clause)))

(definline on-realized
  "Registers callbacks with the manifold deferred for both success and error outcomes."
  [x on-success on-error]
  `(let [^manifold.deferred.IDeferred x# ~x]
     (.onRealized x# ~on-success ~on-error)
     x#))

(definline deferred?
  "Returns true if the object is an instance of a Manifold deferred."
  [x]
  `(instance? IDeferred ~x))

(def ^:private satisfies-deferrable?
  (utils/fast-satisfies #'Deferrable))

(defn deferrable?
  "Returns true if the object can be coerced to a Manifold deferred."
  [x]
  (or
    (instance? IDeferred x)
    (instance? Future x)
    (and (instance? IPending x) (instance? clojure.lang.IDeref x))
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
            (register-future-callbacks x on-success on-error))
          (successValue [_ default]
            (if (.isDone x)
              (try
                (.get x)
                (catch Throwable e
                  default))
              default))
          (errorValue [this default]
            (if (.realized this)
              (try
                (.get x)
                default
                (catch Throwable e
                  e))
              default))))

      (and (instance? IPending x) (instance? clojure.lang.IDeref x))
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
          nil)
        (successValue [_ default]
          (if (.isRealized ^IPending x)
            (try
              (.deref ^IDeref x)
              (catch Throwable e
                default))
            default))
        (errorValue [this default]
          (if (.isRealized ^IPending x)
            (try
              (.deref ^IDeref x)
              default
              (catch Throwable e
                e))
            default)))

      :else
      default-val)))

;;;

(definterface+ IDeferredListener
  (onSuccess [x])
  (onError [err]))

(deftype+ Listener [on-success on-error]
  IDeferredListener
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

(definterface+ IMutableDeferred
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
         (when-let [^IDeferredListener l# (.poll ~'listeners)]
           (try
             (if (nil? ~executor)
               (~(if success? `.onSuccess `.onError) ^IDeferredListener l# ~val)
               (.execute ~(with-meta executor {:tag "java.util.concurrent.Executor"})
                 (fn []
                   (try
                     (~(if success? `.onSuccess `.onError) l# ~val)
                     (catch Throwable e#
                       #_(.printStackTrace e#)
                       (log/error e# "error in deferred handler"))))))
             (catch Throwable e#
               #_(.printStackTrace e#)
               (log/error e# "error in deferred handler")))
           (recur)))
       true)
     ~(if claimed?
        `(throw (IllegalStateException.
                  (if (identical? ~'claim-token ~token)
                    "deferred isn't claimed"
                    "invalid claim-token")))
        false)))

(defmacro ^:private throw' [val]
  `(if (instance? Throwable ~val)
     (throw ~val)
     (throw (ex-info "" {:error ~val}))))

(defmacro ^:private deref-deferred [timeout-value & await-args]
  (let [latch-sym (with-meta (gensym "latch") {:tag "java.util.concurrent.CountDownLatch"})]
    `(condp identical? ~'state
       ::success ~'val
       ::error   (throw' ~'val)
       (let [~latch-sym (CountDownLatch. 1)
             f# (fn [_#] (.countDown ~latch-sym))]
         (on-realized ~'this f# f#)
         (let [result# ~(if (empty? await-args)
                          `(do
                             (.await ~latch-sym)
                             true)
                          `(.await ~latch-sym ~@await-args))]
           (if result#
             (if (identical? ::success ~'state)
               ~'val
               (throw' ~'val))
             ~timeout-value))))))

(defmacro ^:private both [body]
  `(do
     ~(->> body
        (map #(if (and (seq? %) (= 'either (first %)))
                (nth % 1)
                [%]))
        (apply concat))
     ~(->> body
        (map #(if (and (seq? %) (= 'either (first %)))
                (nth % 2)
                [%]))
        (apply concat))))

(both
  (deftype+ (either [LeakAwareDeferred] [Deferred])
    [^:volatile-mutable val
     ^:volatile-mutable state
     ^:volatile-mutable claim-token
     ^Lock lock
     ^LinkedList listeners
     ^:volatile-mutable mta
     ^:volatile-mutable consumed?
     ^Executor executor]

    ;; we only want one in a small number of deferreds to incur this cost at finalization,
    ;; but it's an important sanity check since we might be getting errors bubbling up all
    ;; over without them reaching somewhere we'd notice
    (either
      [Object
       (finalize [_]
         (utils/with-lock lock
           (when (and (identical? ::error state) (not consumed?))
             (log/warn val "unconsumed deferred in error state, make sure you're using `catch`."))))]
      nil)

    clojure.lang.IReference
    (meta [_] mta)
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
    (successValue [this default-value]
      (if (identical? ::success state)
        (do
          (set! consumed? true)
          val)
        default-value))
    (errorValue [this default-value]
      (if (identical? ::error state)
        (do
          (set! consumed? true)
          val)
        default-value))

    clojure.lang.IPending
    (isRealized [this] (realized? this))

    clojure.lang.IDeref
    clojure.lang.IBlockingDeref
    (deref [this]
      (set! consumed? true)
      (deref-deferred nil))
    (deref [this time timeout-value]
      (set! consumed? true)
      (deref-deferred timeout-value time TimeUnit/MILLISECONDS))))

(deftype+ SuccessDeferred
  [val
   ^:volatile-mutable mta
   ^Executor executor]

  clojure.lang.IReference
  (meta [_] mta)
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
  (successValue [_ default-value]
    val)
  (errorValue [_ default-value]
    default-value)

  clojure.lang.IPending
  (isRealized [this] (realized? this))

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [this] val)
  (deref [this time timeout-value] val))

(deftype+ ErrorDeferred
  [^Throwable error
   ^:volatile-mutable mta
   ^:volatile-mutable consumed?
   ^Executor executor]

  Object
  (finalize [_]
    (when (and
            (not consumed?)
            debug/*dropped-error-logging-enabled?*)
      (log/warn error "unconsumed deferred in error state, make sure you're using `catch`.")))

  clojure.lang.IReference
  (meta [_] mta)
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
    (set! consumed? true)
    (if (nil? executor)
      (on-error error)
      (.execute executor #(on-error error))))
  (successValue [_ default-value]
    default-value)
  (errorValue [_ default-value]
    (set! consumed? true)
    error)

  clojure.lang.IPending
  (isRealized [this] (realized? this))

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [this]
    (set! consumed? true)
    (throw' error))
  (deref [this time timeout-value]
    (set! consumed? true)
    (throw' error)))

(let [created (AtomicLong. 0)]
  (defn deferred
    "Equivalent to Clojure's `promise`, but also allows asynchronous callbacks to be registered
     and composed via `chain`."
    ([]
       (deferred (ex/executor)))
    ([executor]
      (if (and (zero? (rem (.incrementAndGet created) 1024))
            debug/*dropped-error-logging-enabled?*)
        (LeakAwareDeferred. nil ::unset nil (utils/mutex) (LinkedList.) nil false executor)
        (Deferred. nil ::unset nil (utils/mutex) (LinkedList.) nil false executor)))))

(def ^:no-doc true-deferred-  (SuccessDeferred. true nil nil))
(def ^:no-doc false-deferred- (SuccessDeferred. false nil nil))
(def ^:no-doc nil-deferred-   (SuccessDeferred. nil nil nil))

(defn success-deferred
  "A deferred which already contains a realized value"
  #_{:inline
     (fn [val]
       (cond
         (true? val)   'manifold.deferred/true-deferred-
         (false? val)  'manifold.deferred/false-deferred-
         (nil? val)    'manifold.deferred/nil-deferred-
         :else         `(SuccessDeferred. ~val nil nil)))
   :inline-arities #{1}}
  ([val]
     (SuccessDeferred. val nil (ex/executor)))
  ([val executor]
     (if (nil? executor)
       (condp identical? val
         true true-deferred-
         false false-deferred-
         nil nil-deferred-
         (SuccessDeferred. val nil nil))
       (SuccessDeferred. val nil executor))))

(defn error-deferred
  "A deferred which already contains a realized error"
  ([error]
    (ErrorDeferred. error nil false (ex/executor)))
  ([error executor]
    (ErrorDeferred. error nil false executor)))

(declare chain)

(defn unwrap' [x]
  (if (deferred? x)
    (let [val (success-value x ::none)]
      (if (identical? val ::none)
        x
        (recur val)))
    x))

(defn unwrap [x]
  (let [d (->deferred x nil)]
    (if (nil? d)
      x
      (let [val (success-value d ::none)]
        (if (identical? ::none val)
          d
          (recur val))))))

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
  (if (identical? executor (.executor d))
    d
    (let [d' (deferred executor)]
      (connect d d')
      d')))

(defmacro future-with
  "Equivalent to Clojure's `future`, but allows specification of the executor
   and returns a Manifold deferred."
  [executor & body]
  `(let [d# (deferred)]
     (manifold.utils/future-with ~executor
       (when-not (realized? d#)
         (try
           (success! d# (do ~@body))
           (catch Throwable e#
             (error! d# e#)))))
     d#))

(defmacro future
  "Equivalent to Clojure's `future`, but returns a Manifold deferred."
  [& body]
  `(future-with (ex/execute-pool) ~@body))

;;;

(defn- unroll-chain
  [unwrap-fn v & fs]
  (let [fs' (repeatedly (count fs) #(gensym "f"))
        v-sym (gensym "v")
        idx-sym (gensym "idx")
        idx-sym' (gensym "idx")]
    `(let [~@(interleave fs' fs)]
       ((fn this# [d# v# ^long idx#]
          (try
            (clojure.core/loop [~v-sym v# ~idx-sym idx#]
              (let [~v-sym (~unwrap-fn ~v-sym)]
                (if (deferred? ~v-sym)
                  (let [d# (or d# (deferred))]
                    (on-realized ~v-sym
                      (fn [v#] (this# d# v# ~idx-sym))
                      (fn [e#] (error! d# e#)))
                    d#)
                  (let [~idx-sym' (unchecked-inc ~idx-sym)]
                    (case (unchecked-int ~idx-sym)
                      ~@(apply concat
                          (map
                            (fn [idx f]
                              `(~idx (recur (~f ~v-sym) ~idx-sym')))
                            (range)
                            fs'))
                      ~(count fs) (if (nil? d#)
                                    (success-deferred ~v-sym)
                                    (success! d# ~v-sym))
                      nil)))))
            (catch Throwable e#
              (if (nil? d#)
                (error-deferred e#)
                (error! d# e#)))))
        nil
        ~v
        0))))

(let [;; factored out for greater inlining joy
      subscribe (fn
                  ([this d x]
                     (let [d (or d (deferred))]
                       (on-realized x
                         #(this d %)
                         #(error! d %))
                       d))
                  ([this d x f]
                     (let [d (or d (deferred))]
                       (on-realized x
                         #(this d % f)
                         #(error! d %))
                       d))
                  ([this d x f g]
                     (let [d (or d (deferred))]
                       (on-realized x
                         #(this d % f g)
                         #(error! d %))
                       d)))]

  (defn ^:no-doc chain'-
   ([d x]
      (try
        (let [x' (unwrap' x)]

          (if (deferred? x')

            (subscribe chain'- d x')

            (if (nil? d)
              (success-deferred x')
              (success! d x'))))
        (catch Throwable e
          (if (nil? d)
            (error-deferred e)
            (error! d e)))))
   ([d x f]
      (try
        (let [x' (unwrap' x)]

          (if (deferred? x')

            (subscribe chain'- d x' f)

            (let [x'' (f x')]
              (if (deferred? x'')
                (chain'- d x'')
                (if (nil? d)
                  (success-deferred x'')
                  (success! d x''))))))
        (catch Throwable e
          (if (nil? d)
            (error-deferred e)
            (error! d e)))))
   ([d x f g]
      (try
        (let [x' (unwrap' x)]

          (if (deferred? x')

            (subscribe chain'- d x' f g)

            (let [x'' (f x')]
              (if (deferred? x'')
                (chain'- d x'' g)
                (let [x''' (g x'')]
                  (if (deferred? x''')
                    (chain'- d x''')
                    (if (nil? d)
                      (success-deferred x''')
                      (success! d x'''))))))))
        (catch Throwable e
          (if (nil? d)
            (error-deferred e)
            (error! d e)))))
   ([d x f g & fs]
      (when (or (nil? d) (not (realized? d)))
        (let [d (or d (deferred))]
          (clojure.core/loop [x x, fs (list* f g fs)]
            (if (empty? fs)
              (success! d x)
              (let [[f g & fs] fs
                    d' (if (nil? g)
                         (chain'- nil x f)
                         (chain'- nil x f g))]
                (success-error-unrealized d'
                  val (recur val fs)
                  err (error! d err)
                  (on-realized d'
                    #(apply chain'- d % fs)
                    #(error! d %))))))
          d)))))

(let [;; factored out for greater inlining joy
      subscribe (fn
                  ([this d x]
                     (let [d (or d (deferred))]
                       (on-realized x
                         #(this d %)
                         #(error! d %))
                       d))
                  ([this d x f]
                     (let [d (or d (deferred))]
                       (on-realized x
                         #(this d % f)
                         #(error! d %))
                       d))
                  ([this d x f g]
                     (let [d (or d (deferred))]
                       (on-realized x
                         #(this d % f g)
                         #(error! d %))
                       d)))]

  (defn ^:no-doc chain-
    ([d x]
       (let [x' (unwrap x)]

         (if (deferred? x')

           (subscribe chain- d x')

           (if (nil? d)
             (success-deferred x')
             (success! d x')))))
    ([d x f]
       (if (or (nil? d) (not (realized? d)))
         (try
           (let [x' (unwrap x)]

             (if (deferred? x')

               (subscribe chain- d x' f)

               (let [x'' (f x')]
                 (if (deferrable? x'')
                   (chain- d x'')
                   (if (nil? d)
                     (success-deferred x'')
                     (success! d x''))))))
           (catch Throwable e
             (if (nil? d)
               (error-deferred e)
               (error! d e))))
         d))
    ([d x f g]
       (if (or (nil? d) (not (realized? d)))
         (try
           (let [x' (unwrap x)]

             (if (deferred? x')

               (subscribe chain- d x' f g)

               (let [x'' (f x')]
                 (if (deferrable? x'')
                   (chain- d x'' g)
                   (let [x''' (g x'')]
                     (if (deferrable? x''')
                       (chain- d x''')
                       (if (nil? d)
                         (success-deferred x''')
                         (success! d x'''))))))))
           (catch Throwable e
             (if (nil? d)
               (error-deferred e)
               (error! d e))))
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
                 (success-error-unrealized d
                   val (recur val fs)
                   err (error! d err)
                   (on-realized d'
                     #(apply chain- d % fs)
                     #(error! d %))))))
           d)))))

(defn chain'
  "Like `chain`, but does not coerce deferrable values.  This is useful both when coercion
   is undesired, or for 2-4x better performance than `chain`."
  {:inline (fn [& args]
             (if false #_(< 3 (count args))
               (apply unroll-chain 'manifold.deferred/unwrap' args)
               `(chain'- nil ~@args)))}
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
  {:inline (fn [& args]
             (if false #_(< 3 (count args))
               (apply unroll-chain 'manifold.deferred/unwrap args)
               `(chain- nil ~@args)))}
  ([x]
    (chain- nil x identity identity))
  ([x f]
    (chain- nil x f identity))
  ([x f g]
    (chain- nil x f g))
  ([x f g & fs]
     (apply chain- nil x f g fs)))

(defn catch'
  "Like `catch`, but does not coerce deferrable values."
  ([x error-handler]
     (catch' x nil error-handler))
  ([x error-class error-handler]
     (let [x (chain' x)
           catch? #(or (nil? error-class) (instance? error-class %))]
       (if-not (deferred? x)

         ;; not a deferred value, skip over it
         x

         (success-error-unrealized x
           val x

           err (try
                 (if (catch? err)
                   (chain' (error-handler err))
                   (error-deferred err))
                 (catch Throwable e
                   (error-deferred e)))

           (let [d' (deferred)]

             (on-realized x
               #(success! d' %)
               #(try
                  (if (catch? %)
                    (chain'- d' (error-handler %))
                    (chain'- d' (error-deferred %)))
                  (catch Throwable e
                    (error! d' e))))

             d'))))))

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
    (catch x nil error-handler))
  ([x error-class error-handler]
     (if-let [d (->deferred x nil)]
       (-> d
         chain
         (catch' error-class error-handler)
         chain)
       x)))

(defn finally'
  "Like `finally`, but doesn't coerce deferrable values."
  [x f]
  (success-error-unrealized x

    val (try
          (f)
          x
          (catch Throwable e
            (error-deferred e)))

    err (try
          (f)
          (error-deferred err)
          (catch Throwable e
            (error-deferred e)))

    (let [d (deferred)]
      (on-realized x
        #(try
           (f)
           (success! d %)
           (catch Throwable e
             (error! d e)))
        #(try
           (f)
           (error! d %)
           (catch Throwable e
             (error! d e))))
      d)))

(defn finally
  "An equivalent of the finally clause, which takes a no-arg side-effecting function that executes
   no matter what the result."
  [x f]
  (if-let [d (->deferred x nil)]
    (finally' d f)
    (finally' x f)))

(defn zip'
  "Like `zip`, but only unwraps Manifold deferreds."
  {:inline (fn [x] `(chain' ~x vector))
   :inline-arities #{1}}
  [& vals]
  (let [cnt (count vals)
        ^objects ary (object-array cnt)
        counter (AtomicInteger. cnt)]
    (clojure.core/loop [d nil, idx 0, s vals]

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

            (success-error-unrealized x

              val (do
                    (aset ary idx val)
                    (.decrementAndGet counter)
                    (recur d idx' rst))

              err (error-deferred err)

              (let [d (or d (deferred))]
                (on-realized (chain' x)
                  (fn [val]
                    (aset ary idx val)
                    (when (zero? (.decrementAndGet counter))
                      (success! d (seq ary))))
                  (fn [err]
                    (error! d err)))
                (recur d idx' rst)))

            ;; not deferred - set, decrement, and recur
            (do
              (aset ary idx x)
              (.decrementAndGet counter)
              (recur d idx' rst))))))))

(defn zip
  "Takes a list of values, some of which may be deferrable, and returns a deferred that will yield a list
   of realized values.

        @(zip 1 2 3) => [1 2 3]
        @(zip (future 1) 2 3) => [1 2 3]

  "
  {:inline (fn [x] `(chain ~x vector))
   :inline-arities #{1}}
  [& vals]
  (->> vals
    (map #(or (->deferred % nil) %))
    (apply zip')))

;; same technique as clojure.core.async/random-array
(defn- random-array [n]
  (let [a (int-array n)]
    (clojure.core/loop [i 1]
      (if (= i n)
        a
        (let [j (rand-int (inc i))]
          (aset a i (aget a j))
          (aset a j i)
          (recur (inc i)))))))

(defn alt'
  "Like `alt`, but only unwraps Manifold deferreds."
  [& vals]
  (let [d (deferred)
        cnt (count vals)
        ^ints idxs (random-array cnt)]
    (clojure.core/loop [i 0]
      (when (< i cnt)
        (let [i' (aget idxs i)
              x (nth vals i')]
          (if (deferred? x)
            (success-error-unrealized x
              val (success! d val)
              err (error! d err)
              (do (on-realized (chain' x)
                    #(success! d %)
                    #(error! d %))
                  (recur (inc i))))
            (success! d x)))))
    d))

(defn alt
  "Takes a list of values, some of which may be deferrable, and returns a
  deferred that will yield the value which was realized first.

    @(alt 1 2) => 1
    @(alt (future (Thread/sleep 1) 1)
          (future (Thread/sleep 1) 2)) => 1 or 2 depending on the thread scheduling

  Values appearing earlier in the input are preferred."
  [& vals]
  (->> vals
       (map #(or (->deferred % nil) %))
       (apply alt')))

(defn timeout!
  "Takes a deferred, and sets a timeout on it, such that it will be realized as `timeout-value`
   (or a TimeoutException if none is specified) if it is not realized in `interval` ms.  Returns
   the deferred that was passed in.

   This will act directly on the deferred value passed in.  If the deferred represents a value
   returned by `chain`, all actions not yet completed will be short-circuited upon timeout."
  ([d interval]
    (cond
      (or (nil? interval) (not (deferred? d)) (realized? d))
      nil

      (not (pos? interval))
      (error! d
        (TimeoutException.
          (str "timed out after " interval " milliseconds")))

      :else
      (let [timeout-d (time/in interval
                               #(error! d
                                        (TimeoutException.
                                          (str "timed out after " interval " milliseconds"))))]
        (chain d (fn [_]
                   (success! timeout-d true)))))
    d)
  ([d interval timeout-value]
    (cond
      (or (nil? interval) (not (deferred? d)) (realized? d))
      nil

      (not (pos? interval))
      (success! d timeout-value)

      :else
      (let [timeout-d (time/in interval
                               #(success! d timeout-value))]
        (chain d (fn [_]
                   (success! timeout-d true)))))
    d))

(deftype+ Recur [s]
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
        x-sym (gensym "x")
        val-sym (gensym "val")
        var-syms (map (fn [_] (gensym "var")) vars)]
    `(let [result# (deferred)]
       ((fn this# [result# ~@var-syms]
          (clojure.core/loop
            [~@(interleave vars var-syms)]
            (let [~x-sym (try
                           ~@body
                           (catch Throwable e#
                             (error! result# e#)
                             nil))]
              (cond

                (deferred? ~x-sym)
                (success-error-unrealized ~x-sym
                  ~val-sym (if (instance? Recur ~val-sym)
                             (let [~val-sym @~val-sym]
                               (~'recur
                                 ~@(map
                                     (fn [n] `(nth ~val-sym ~n))
                                     (range (count vars)))))
                             (success! result# ~val-sym))

                  err# (error! result# err#)

                  (on-realized (chain' ~x-sym)
                    (fn [x#]
                      (if (instance? Recur x#)
                        (apply this# result# @x#)
                        (success! result# x#)))
                    (fn [err#]
                      (error! result# err#))))

                (instance? Recur ~x-sym)
                (~'recur
                  ~@(map
                      (fn [n] `(nth @~x-sym ~n))
                      (range (count vars))))

                :else
                (success! result# ~x-sym)))))
         result#
         ~@vals)
       result#)))

;;;

#_(utils/when-core-async

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

(utils/when-class java.util.concurrent.CompletableFuture

  (extend-protocol Deferrable

    java.util.concurrent.CompletableFuture
    (to-deferred [f]
      (let [d (deferred)]
        (.handle ^java.util.concurrent.CompletableFuture f
          (reify java.util.function.BiFunction
            (apply [_ val err]
              (if (nil? err)
                (success! d val)
                (error! d err)))))
        d))))

;;;

(defn- back-references [marker form]
  (let [syms (atom #{})]
    (walk/walk-exprs
      symbol?
      (fn [s]
        (when (some-> (compiler/locals) (find s) key meta (get marker))
          (swap! syms conj s)))
      form)
    @syms))

(defn- expand-let-flow [chain-fn zip-fn bindings body]
  (let [[_ bindings & body] (walk/macroexpand-all `(let ~bindings ~@body))
        locals              (keys (compiler/locals))
        vars                (->> bindings (partition 2) (map first))
        marker              (gensym)
        vars'               (->> vars (concat locals) (map #(vary-meta % assoc marker true)))
        gensyms             (repeatedly (count vars') gensym)
        gensym->var         (zipmap gensyms vars')
        vals'               (->> bindings (partition 2) (map second) (concat locals))
        gensym->deps        (zipmap
                              gensyms
                              (->> (count vars')
                                range
                                (map
                                  (fn [n]
                                    `(let [~@(interleave (take n vars') (repeat nil))
                                           ~(nth vars' n) ~(nth vals' n)])))
                                (map
                                  (fn [n form]
                                    (map
                                      (zipmap vars' (take n gensyms))
                                      (back-references marker form)))
                                  (range))))
        binding-dep?        (->> gensym->deps vals (apply concat) set)

        body-dep?           (->> `(let [~@(interleave
                                            vars'
                                            (repeat nil))]
                                    ~@body)
                              (back-references marker)
                              (concat vars)
                              (map (zipmap vars' gensyms))
                              set)
        dep?                (set/union binding-dep? body-dep?)]
    `(let [executor# (or (manifold.executor/executor) (ex/execute-pool))]
       (manifold.executor/with-executor nil
         (let [~@(mapcat
                   (fn [n var val gensym]
                     (let [deps (gensym->deps gensym)]
                       (if (empty? deps)
                         (when (dep? gensym)
                           [gensym val])
                         [gensym
                          `(~chain-fn (~zip-fn ~@deps)
                            (bound-fn [[~@(map gensym->var deps)]]
                              ~val))])))
                   (range)
                   vars'
                   vals'
                   gensyms)]
           (~chain-fn (onto (~zip-fn ~@body-dep?) executor#)
            (bound-fn [[~@(map gensym->var body-dep?)]]
              ~@body)))))))

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
  (expand-let-flow
    'manifold.deferred/chain
    'manifold.deferred/zip
    bindings
    body))

(defmacro let-flow'
  "Like `let-flow`, but only for Manifold deferreds."
  [bindings & body]
  (expand-let-flow
    'manifold.deferred/chain'
    'manifold.deferred/zip'
    bindings
    body))

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
(alter-meta! #'->LeakAwareDeferred assoc :private true)
(alter-meta! #'->ErrorDeferred assoc :private true)
(alter-meta! #'->SuccessDeferred assoc :private true)
(alter-meta! #'->Listener assoc :private true)
(alter-meta! #'->Recur assoc :private true)
