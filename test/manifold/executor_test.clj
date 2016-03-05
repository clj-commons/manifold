(ns manifold.executor-test
  (:require
    [clojure.test :refer :all]
    [clojure.string :as str]
    [manifold.executor :as e])
  (:import
    [io.aleph.dirigiste
     Executors
     Executor]
    [java.lang.management
     ManagementFactory
     ThreadInfo]))

(defn filter-threads [name-prefix]
  (let [thread-mx-bean (ManagementFactory/getThreadMXBean)
        thread-infos (into [] (.getThreadInfo thread-mx-bean (.getAllThreadIds thread-mx-bean)))]
    (filter (fn [^ThreadInfo info] (str/starts-with? (.getThreadName info) name-prefix)) thread-infos)))

(deftest test-instrumented-executor-uses-thread-factory
  (let [controller (Executors/utilizationController 0.95 1)
        thread-count (atom 0)
        pool-prefix "my-pool-prefix-"
        factory (e/thread-factory
                  #(str pool-prefix (swap! thread-count inc))
                  (deliver (promise) nil))
        thread-count 1
        executor (e/instrumented-executor
                   {:controller           controller
                    :thread-factory       factory
                    :initial-thread-count thread-count
                    })]
    (.execute ^Executor executor #(prn "Hello world!"))
    (is (>= (count (filter-threads pool-prefix)) thread-count))))
