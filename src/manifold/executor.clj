(ns manifold.executor
  (:require
    [clojure.tools.logging :as log])
  (:import
    [io.aleph.dirigiste
     Executors
     Executor
     Executor$Controller
     Stats
     Stats$Metric]
    [java.util
     EnumSet]
    [java.util.concurrent
     SynchronousQueue
     LinkedBlockingQueue
     ArrayBlockingQueue
     ThreadFactory
     TimeUnit]))

;;;

(def ^ThreadLocal executor-thread-local (ThreadLocal.))

(definline executor []
  `(.get manifold.executor/executor-thread-local))

(defmacro with-executor [executor & body]
  `(let [executor# (executor)]
     (.set executor-thread-local ~executor)
     (try
       ~@body
       (finally
         (.set executor-thread-local executor#)))))

(defn ^ThreadFactory thread-factory
  ([name-generator executor-promise]
     (thread-factory name-generator executor-promise nil))
  ([name-generator executor-promise stack-size]
    (reify ThreadFactory
      (newThread [_ runnable]
        (let [name (name-generator)
              f #(do
                   (.set executor-thread-local @executor-promise)
                   (.run ^Runnable runnable))]
          (doto
            (if stack-size
              (Thread. nil f name stack-size)
              (Thread. nil f name))
            (.setDaemon true)))))))

;;;

(defn stats->map
  "Converts a Dirigiste `Stats` object into a map of values onto quantiles."
  ([s]
    (stats->map s [0.5 0.9 0.95 0.99 0.999]))
  ([^Stats s quantiles]
    (let [stats (.getMetrics s)
          q #(zipmap quantiles (mapv % quantiles))]
      (merge
        {:num-workers (.getNumWorkers s)}
        (when (contains? stats Stats$Metric/QUEUE_LENGTH)
          {:queue-length (q #(.getQueueLength s %))})
        (when (contains? stats Stats$Metric/QUEUE_LATENCY)
          {:queue-latency (q #(double (/ (.getQueueLatency s %) 1e6)))})
        (when (contains? stats Stats$Metric/TASK_LATENCY)
          {:task-latency (q #(double (/ (.getTaskLatency s %) 1e6)))})
        (when (contains? stats Stats$Metric/TASK_ARRIVAL_RATE)
          {:task-arrival-rate (q #(.getTaskArrivalRate s %))})
        (when (contains? stats Stats$Metric/TASK_COMPLETION_RATE)
          {:task-completion-rate (q #(.getTaskCompletionRate s %))})
        (when (contains? stats Stats$Metric/TASK_REJECTION_RATE)
          {:task-rejection-rate (q #(.getTaskRejectionRate s %))})
        (when (contains? stats Stats$Metric/UTILIZATION)
          {:utilization (q #(.getUtilization s %))})))))

(def ^:private factory-count (atom 0))

(defn instrumented-executor
  "Returns a `java.util.concurrent.ExecutorService`, using [Dirigiste](https://github.com/ztellman/dirigiste).

   |:---|:----
   | `thread-factory` | an optional `java.util.concurrent.ThreadFactory` that creates the executor's threads. |
   | `queue-length` | the maximum number of pending tasks before `.execute()` begins throwing `java.util.concurrent.RejectedExecutionException`, defaults to `0`.
   | `stats-callback` | a function that will be invoked every `control-period` with the relevant statistics for the executor.
   | `sample-period` | the interval, in milliseconds, between sampling the state of the executor for resizing and gathering statistics, defaults to `25`.
   | `control-period` | the interval, in milliseconds, between use of the controller to adjust the size of the executor, defaults to `10000`.
   | `controller` | the Dirigiste controller that is used to guide the pool's size.
   | `metrics` | an `EnumSet` of the metrics that should be gathered for the controller, defaults to all.
   | `initial-thread-count` | the number of threads that the pool should begin with.
   | `onto?` | if true, all streams and deferred generated in the scope of this executor will also be 'on' this executor."
  [{:keys
    [thread-factory
     queue-length
     stats-callback
     sample-period
     control-period
     controller
     metrics
     initial-thread-count
     onto?]
    :or {initial-thread-count 1
         sample-period 25
         control-period 10000
         metrics (EnumSet/allOf Stats$Metric)
         onto? true}}]
  (let [executor-promise (promise)
        thread-count (atom 0)
        factory (swap! factory-count inc)
        thread-factory (if thread-factory
                         thread-factory
                         (manifold.executor/thread-factory
                           #(str "manifold-pool-" factory "-" (swap! thread-count inc))
                           (if onto?
                             executor-promise
                             (deliver (promise) nil))))
        ^Executor$Controller c controller
        metrics (if (identical? :none metrics)
                  (EnumSet/noneOf Stats$Metric)
                  metrics)]
    (assert controller "must specify :controller")
    @(deliver executor-promise
       (Executor.
         thread-factory
         (if (and queue-length (pos? queue-length))
           (if (<= queue-length 1024)
             (ArrayBlockingQueue. queue-length false)
             (LinkedBlockingQueue. (int queue-length)))
           (SynchronousQueue. false))
         (if stats-callback
           (reify Executor$Controller
             (shouldIncrement [_ n]
               (.shouldIncrement c n))
             (adjustment [_ s]
               (stats-callback (stats->map s))
               (.adjustment c s)))
           c)
         initial-thread-count
         metrics
         sample-period
         control-period
         TimeUnit/MILLISECONDS))))

(defn fixed-thread-executor
  "Returns an executor which has a fixed number of threads."
  ([num-threads]
    (fixed-thread-executor num-threads nil))
  ([num-threads options]
    (instrumented-executor
      (-> options
        (update-in [:queue-length] #(or % Integer/MAX_VALUE))
        (assoc
          :max-threads num-threads
          :controller (reify Executor$Controller
                        (shouldIncrement [_ n]
                          (< n num-threads))
                        (adjustment [_ s]
                          (- num-threads (.getNumWorkers s)))))))))

(defn utilization-executor
  "Returns an executor which sizes the thread pool according to target utilization, within
   `[0,1]`, up to `max-threads`.  The `queue-length` for this executor is always `0`, and by
   default has an unbounded number of threads."
  ([utilization]
    (utilization-executor utilization Integer/MAX_VALUE nil))
  ([utilization max-threads]
    (utilization-executor utilization max-threads nil))
  ([utilization max-threads options]
    (instrumented-executor
      (assoc options
        :queue-length 0
        :max-threads max-threads
        :controller (Executors/utilizationController utilization max-threads)))))

;;;

(def ^:private wait-pool-stats-callbacks (atom #{}))

(defn register-wait-pool-stats-callback
  "Registers a callback which will be called with wait-pool stats."
  [c]
  (swap! wait-pool-stats-callbacks conj c))

(defn unregister-wait-pool-stats-callback
  "Unregisters a previous wait-pool stats callback."
  [c]
  (swap! wait-pool-stats-callbacks disj c))

(let [wait-pool-promise
      (delay
        (let [cnt (atom 0)]
          (utilization-executor 0.95 Integer/MAX_VALUE
            {:thread-factory (thread-factory
                               #(str "manifold-wait-" (swap! cnt inc))
                               (deliver (promise) nil)
                               1e2)
             :stats-callback (fn [stats]
                               (doseq [f @wait-pool-stats-callbacks]
                                 (try
                                   (f stats)
                                   (catch Throwable e
                                     (log/error e "error in wait-pool stats callback")))))})))]
  (defn wait-pool []
    @wait-pool-promise))

;;;

(def ^:private execute-pool-stats-callbacks (atom #{}))

(defn register-execute-pool-stats-callback
  "Registers a callback which will be called with execute-pool stats."
  [c]
  (swap! execute-pool-stats-callbacks conj c))

(defn unregister-execute-pool-stats-callback
  "Unregisters a previous execute-pool stats callback."
  [c]
  (swap! execute-pool-stats-callbacks disj c))

(let [execute-pool-promise
      (delay
        (let [cnt (atom 0)]
          (utilization-executor 0.95 Integer/MAX_VALUE
            {:thread-factory (thread-factory
                               #(str "manifold-execute-" (swap! cnt inc))
                               (deliver (promise) nil))
             :stats-callback (fn [stats]
                               (doseq [f @execute-pool-stats-callbacks]
                                 (try
                                   (f stats)
                                   (catch Throwable e
                                     (log/error e "error in execute-pool stats callback")))))})))]
  (defn execute-pool []
    @execute-pool-promise))
