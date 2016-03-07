(ns manifold.executor-test
  (:require
    [clojure.test :refer :all]
    [manifold.executor :as e])
  (:import
    [io.aleph.dirigiste
     Executors
     Executor]
    [java.util.concurrent
     LinkedBlockingQueue]))

(deftest test-instrumented-executor-uses-thread-factory
  (let [thread-count (atom 0)
        threadpool-prefix "my-pool-prefix-"
        thread-factory (e/thread-factory
                  #(str threadpool-prefix (swap! thread-count inc))
                  (deliver (promise) nil))
        executor (e/instrumented-executor
                   {:controller     (Executors/utilizationController 0.95 1)
                    :thread-factory thread-factory})
        thread-names (LinkedBlockingQueue. 1)]
    (.execute ^Executor executor #(.put thread-names (.getName (Thread/currentThread))))
    (is (= (.take thread-names) (str threadpool-prefix @thread-count)))))
