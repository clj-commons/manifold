(ns manifold.utils
  (:refer-clojure
    :exclude [future])
  (:require
    [clojure.tools.logging :as log])
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

(defn ^ThreadFactory thread-factory
  ([name-generator]
     (reify ThreadFactory
       (newThread [_ runnable]
         (let [name (name-generator)]
           (doto
             (Thread. nil #(.run ^Runnable runnable) name)
             (.setDaemon true))))))
  ([name-generator stack-size]
     (reify ThreadFactory
       (newThread [_ runnable]
         (let [name (name-generator)]
           (doto
             (Thread. nil #(.run ^Runnable runnable) name stack-size)
             (.setDaemon true)))))))

;;;

(def ^Executor wait-pool
  (let [cnt (atom 0)]
    (Executors/newCachedThreadPool
      (thread-factory #(str "manifold-wait-" (swap! cnt inc)) 1e2))))

(defmacro wait-for [& body]
  `(let [f# (fn []
              (try
                ~@body
                (catch Throwable e#
                  (log/error e# "error in manifold.utils/wait-for"))))]
     (.execute wait-pool ^Runnable f#)
     nil))

;;;

(def ^ThreadLocal stack-depth (ThreadLocal.))

(def ^Executor execute-pool
  (let [cnt (atom 0)]
    (Executors/newCachedThreadPool
      (thread-factory #(str "manifold-execute-" (swap! cnt inc))))))

(def ^:const max-depth 50)

(defmacro future [& body]
  `(let [frame# (clojure.lang.Var/cloneThreadBindingFrame)
         f# (fn []
              (let [curr-frame# (clojure.lang.Var/getThreadBindingFrame)]
                (clojure.lang.Var/resetThreadBindingFrame frame#)
                (try
                  ~@body
                  (catch Throwable e#
                    (log/error e# "error in manifold.utils/future"))
                  (finally
                    (clojure.lang.Var/resetThreadBindingFrame curr-frame#)))))]
     (.execute execute-pool ^Runnable f#)
     nil))

(defmacro without-overflow [& body]
  `(let [depth# (.get stack-depth)
         depth'# (if (nil? depth#) 0 depth#)
         f# (fn [] ~@body)]
     (if (> depth'# max-depth)
       (future (f#))
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

;;;

(defmacro when-core-async [& body]
  (when (try
          (require '[clojure.core.async :as a])
          true
          (catch Exception _
            false))
    `(do ~@body)))

(defmacro when-lamina [& body]
  (when (try
          (require '[lamina.core :as l])
          (require '[lamina.api :as la])
          true
          (catch Exception _
            false))
    `(do ~@body)))
