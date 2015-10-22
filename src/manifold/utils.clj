(ns manifold.utils
  (:refer-clojure
    :exclude [future])
  (:require
    [clojure.tools.logging :as log]
    [manifold.executor :as ex])
  (:import
    [java.util.concurrent
     Executors
     Executor
     ThreadFactory
     BlockingQueue
     ConcurrentHashMap]
    [java.util.concurrent.locks
     ReentrantLock
     Lock]))

;;;

(defmacro wait-for [& body]
  `(let [f# (fn []
              (try
                ~@body
                (catch Throwable e#
                  (log/error e# "error in manifold.utils/wait-for"))))]
     (.execute ^Executor (ex/wait-pool) ^Runnable f#)
     nil))

(defmacro future-with [executor & body]
  `(let [frame# (clojure.lang.Var/cloneThreadBindingFrame)
         ^Executor executor# ~executor
         f# (fn []
              (let [curr-frame# (clojure.lang.Var/getThreadBindingFrame)]
                (clojure.lang.Var/resetThreadBindingFrame frame#)
                (try
                  ~@body
                  (catch Throwable e#
                    (log/error e# "error in manifold.utils/future-with"))
                  (finally
                    (clojure.lang.Var/resetThreadBindingFrame curr-frame#)))))]
     (.execute executor# ^Runnable f#)
     nil))

;;;

(def ^ThreadLocal stack-depth (ThreadLocal.))

(def ^:const max-depth 50)

(defmacro without-overflow [executor & body]
  `(let [depth# (.get stack-depth)
         depth'# (if (nil? depth#) 0 depth#)
         f# (fn [] ~@body)]
     (if (> depth'# max-depth)
       (future-with ~executor (f#))
       (try
         (.set stack-depth (unchecked-inc (unchecked-long depth'#)))
         (f#)
         (finally
           (when (nil? depth#)
             (.set stack-depth nil)))))))

;;;

(defn invoke-callbacks [^BlockingQueue callbacks]
  (loop []
    (when-let [c (.poll callbacks)]
      (try
        (c)
        (catch Throwable e
          (log/error e "error in invoke-callbacks")))
      (recur))))

;;;

(defn fast-satisfies [protocol-var]
  (let [^ConcurrentHashMap classes (ConcurrentHashMap.)]
    (add-watch protocol-var ::memoization (fn [& _] (.clear classes)))
    (fn [x]
      (if (nil? x)
        false
        (let [cls (class x)
              val (.get classes cls)]
          (if (nil? val)
            (let [val (satisfies? @protocol-var x)]
              (.put classes cls val)
              val)
            val))))))

;;;

(defn mutex []
  (ReentrantLock.))

(defmacro with-lock [lock & body]
  `(let [^java.util.concurrent.locks.Lock lock# ~lock]
     (.lock lock#)
     (try
       ~@body
       (finally
         (.unlock lock#)))))

(defmacro with-lock* [lock & body]
  `(let [^java.util.concurrent.locks.Lock lock# ~lock]
     (.lock lock#)
     (let [x# (do ~@body)]
       (.unlock lock#)
       x#)))

;;;

(defmacro when-core-async [& body]
  (when (try
          (require '[clojure.core.async :as a])
          true
          (catch Exception _
            false))
    `(do ~@body)))

(defmacro when-class [class & body]
  (let [disable-property (System/getProperty "manifold.disable-jvm8-primitives")
        disabled? (and disable-property (not= disable-property "false"))]
    (when (and (not disabled?)
            (try
              (Class/forName (name class))
              (catch Throwable e
                )))
      `(do ~@body))))
