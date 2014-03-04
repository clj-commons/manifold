(ns eventual.utils
  (:import
    [java.util.concurrent
     Executors
     Executor
     ThreadFactory]))

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
      (thread-factory #(str "eventual-blocking-pool-" (swap! cnt inc)) 1e3))))

(defmacro defer [& body]
  `(let [f# (fn [] ~@body)]
     (.execute blocking-pool ^Runnable f#)
     nil))
