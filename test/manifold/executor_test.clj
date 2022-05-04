(ns manifold.executor-test
  (:require
    [clojure.test :refer :all]
    [manifold.executor :as e])
  (:import
    [io.aleph.dirigiste
     Executor
     Executor$Controller]
    [java.util.concurrent
     ExecutorService
     Executors
     LinkedBlockingQueue
     ThreadFactory]))

(deftest test-instrumented-executor-uses-thread-factory
  (let [thread-count      (atom 0)
        threadpool-prefix "my-pool-prefix-"
        thread-factory    (e/thread-factory
                            #(str threadpool-prefix (swap! thread-count inc))
                            (deliver (promise) nil))
        controller        (reify Executor$Controller
                            (shouldIncrement [_ n] (< n 2))
                            (adjustment [_ s] 1))
        executor          (e/instrumented-executor
                            {:controller     controller
                             :thread-factory thread-factory})
        thread-names      (LinkedBlockingQueue. 1)]
    (.execute ^Executor executor #(.put thread-names (.getName (Thread/currentThread))))
    (is (contains? #{(str threadpool-prefix 1) (str threadpool-prefix 2)} (.take thread-names)))))

(deftest test-rt-dynamic-classloader
  (let [num-threads      (atom 0)
        in-thread-loader (promise)
        tf               (e/thread-factory
                           #(str "my-loader-prefix-" (swap! num-threads inc))
                           (deliver (promise) nil))
        executor         (Executors/newFixedThreadPool 1 ^ThreadFactory tf)]
    (.execute ^ExecutorService executor
              (fn []
                (let [l (clojure.lang.RT/baseLoader)]
                  (deliver in-thread-loader l))))
    (is (instance? clojure.lang.DynamicClassLoader @in-thread-loader))))

(defn- ^ThreadFactory thread-factory
  ([] (thread-factory nil))
  ([new-thread-fn] (thread-factory new-thread-fn nil))
  ([new-thread-fn stack-size]
   (let [num-threads (atom 0)
         tf (e/thread-factory
             #(str "my-pool-prefix" (swap! num-threads inc))
             (deliver (promise) nil)
             stack-size
             false
             new-thread-fn)]
     tf)))

(deftest test-thread-factory
  (let [tf (thread-factory)]
    (.newThread tf (constantly nil)))
  (let [tf (thread-factory
            (fn [group target _ stack-size]
              (proxy [Thread] [group target "custom-name" stack-size])))
        thread (.newThread tf (constantly nil))]
    (is (= "custom-name" (.getName thread))))
  (let [tf (thread-factory
            (fn [group target _ stack-size]
              (proxy [Thread] [group target "custom-name" stack-size]))
            500)
        thread (.newThread tf (constantly nil))]
    (is (= "custom-name" (.getName thread)))))
