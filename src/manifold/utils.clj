(ns manifold.utils
  (:import
    [java.util.concurrent
     Executors
     Executor
     ThreadFactory
     BlockingQueue]
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

(def ^Executor waiting-pool
  (let [cnt (atom 0)]
    (Executors/newCachedThreadPool
      (thread-factory #(str "manifold-waiting-pool-" (swap! cnt inc)) 1e3))))

(defmacro defer [& body]
  `(let [f# (fn [] ~@body)]
     (.execute waiting-pool ^Runnable f#)
     nil))

;;;

(def ^ThreadLocal stack-depth (ThreadLocal.))

(def ^Executor overflow-protection-pool
  (let [cnt (atom 0)]
    (Executors/newCachedThreadPool
      (thread-factory #(str "manifold-overflow-protection-pool-" (swap! cnt inc))))))

(def ^:const max-depth 50)

(defmacro without-overflow [& body]
  `(let [depth# (.get stack-depth)
         depth'# (if (nil? depth#) 0 depth#)
         f# (fn [] ~@body)]
     (if (> depth'# max-depth)
       (do
         (.execute overflow-protection-pool ^Runnable f#)
         nil)
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
          ;; todo: log something
          )))))

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
