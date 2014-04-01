(ns manifold.utils
  (:import
    [java.util.concurrent
     Executors
     Executor
     ThreadFactory]
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

(def ^Executor blocking-pool
  (let [cnt (atom 0)]
    (Executors/newCachedThreadPool
      (thread-factory #(str "manifold-blocking-pool-" (swap! cnt inc)) 1e3))))

(defmacro defer [& body]
  `(let [f# (fn [] ~@body)]
     (.execute blocking-pool ^Runnable f#)
     nil))

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
