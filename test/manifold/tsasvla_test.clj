(ns manifold.tsasvla-test
  (:require [clojure.test :refer :all]
            [manifold.tsasvla :refer [tsasvla <!? tsasvla-exeuctor]]
            [manifold.deferred :as d]
            [manifold.test-utils :refer :all]
            [manifold.executor :as ex]
            [clojure.string :as str]
            [manifold.stream :as s])
  (:import (java.util.concurrent TimeoutException Executor)))

(deftest async-test
  (testing "values are returned correctly"
    (is (= 10
           @(tsasvla (<!? (d/success-deferred 10))))))

  (testing "case with tsasvla"
    (is (= :1
           @(tsasvla (case (name :1)
                  "0" :0
                  "1" :1
                  :3)))))

  (testing "nil result of tsasvla"
    (is (= nil
           @(tsasvla nil))))

  (testing "take inside binding of loop"
    (is (= 42
           @(tsasvla (loop [x (<!? (d/success-deferred 42))]
                  x)))))

  (testing "can get from a catch"
    (let [c (d/success-deferred 42)]
      (is (= 42
             @(tsasvla (try
                    (assert false)
                    (catch Throwable ex (<!? c)))))))))

(deftest enqueued-chan-ops
  (testing "enqueued channel takes re-enter async properly"
    (is (= :foo
           (let [d          (d/deferred)
                 async-chan (tsasvla (<!? d))]
             (d/success! d :foo)
             @async-chan)))

    (is (= 3
           (let [d1 (d/deferred)
                 d2 (d/deferred)
                 d3 (d/deferred)
                 async-chan (tsasvla (+ (<!? d1) (<!? d2) (<!? d3)))]
             (d/success! d3 1)
             (d/success! d2 1)
             (d/success! d1 1)
             @async-chan)))))

(deftest tsasvla-nests
  (testing "return deferred will always result in a a realizable value, not another deferred"
    (is (= [23 42] @(tsasvla (let [let* 1 a 23] (tsasvla (let* [b 42] [a b]))))))
    (is (= 5 @(tsasvla (tsasvla (tsasvla (tsasvla (tsasvla (tsasvla (tsasvla 5))))))))))
  (testing "Parking unwraps nested deferreds"
    (is (= 5 @(tsasvla (<!? (tsasvla (tsasvla (tsasvla 5)))))))))

(deftest error-propagation
  (is (= "chained catch"
         @(d/catch (tsasvla (/ 5 0))
                   (constantly "chained catch"))))

  (is (= "try/catch in block"
         @(tsasvla (try (/ 5 0)
                       (catch Throwable _ "try/catch in block")))))

  (testing "Try/catch around parking will continue block"
    (is (= "try/catch parking"
           @(tsasvla (try (<!? (d/future (/ 5 0)))
                         (catch Throwable _ "try/catch parking")))))
    (is (= 5
           @(tsasvla (try (<!? (d/future (/ 5 0)))
                         (catch Throwable _))
                    5))))

  (testing "Normal deferred handling still works"
    (is (= 5
           @(tsasvla (<!? (d/catch (d/future (/ 5 0)) (constantly 5))))))))

(deftest non-deferred-takes
  (testing "Can take from non-deffereds"
    (is (= 5 @(tsasvla (<!? 5))))
    (is (= "test" @(tsasvla (<!? "test"))))))

(deftest already-realized-values
  (testing "When taking from already realized values, the threads should not change."
    (let [original-thread (atom nil)]
      (is (= @(tsasvla (reset! original-thread (Thread/currentThread))
                      (<!? "cat")
                      (Thread/currentThread))
             @original-thread)))

    (let [original-thread (atom nil)]
      (is (= @(tsasvla (reset! original-thread (Thread/currentThread))
                      (<!? (d/success-deferred "cat"))
                      (Thread/currentThread))
             @original-thread)))

    (testing "Taking from already realized value doesn't cause remaining body to run twice"
      (let [blow-up-counter (atom 0)
            blow-up-fn      (fn [& _] (is (= 1 (swap! blow-up-counter inc))))]
        @(tsasvla (<!? "cat")
                  (blow-up-fn))))
    ;; Sleep is here to make sure that the secondary invocation of `blow-up-fn` that was happening has
    ;; had time to report it's failure before the test finishes
    (Thread/sleep 500)))

(deftest deferred-interactions
  (testing "timeouts"
    (is (= ::timeout @(tsasvla (<!? (d/timeout! (d/deferred) 10 ::timeout)))))
    (is (= ::timeout @(d/timeout! (tsasvla (<!? (d/deferred))) 10 ::timeout)))
    (is (thrown? TimeoutException @(tsasvla (<!? (d/timeout! (d/deferred) 10)))))
    (is (thrown? TimeoutException @(d/timeout! (tsasvla (<!? (d/deferred))) 10))))

  (testing "alt"
    (is (= ::timeout @(tsasvla (<!? (d/alt (d/deferred) (d/timeout! (d/deferred) 10 ::timeout))))))
    (is (= ::timeout @(d/alt (tsasvla (<!? (d/deferred))) (d/timeout! (d/deferred) 10 ::timeout))))
    (is (= 1 @(tsasvla (<!? (d/alt (d/deferred) (d/success-deferred 1))))))
    (is (= 1 @(d/alt (tsasvla (<!? (d/deferred))) (d/success-deferred 1))))))

(deftest tsasvla-specify-executor-pool
  (let [prefix          "tsasvla-custom-executor"
        cnt             (atom 0)
        custom-executor (ex/utilization-executor 0.95 Integer/MAX_VALUE
                                                 {:thread-factory (ex/thread-factory
                                                                    #(str prefix (swap! cnt inc))
                                                                    (deliver (promise) nil))
                                                  :stats-callback (constantly nil)})]
    (try (is (str/starts-with? @(tsasvla-exeuctor custom-executor (.getName (Thread/currentThread))) prefix)
             "Running on custom executor, thread naming should be respected.")
         (println @(tsasvla-exeuctor custom-executor (.getName (Thread/currentThread))))
         (finally (.shutdown custom-executor)))))

(deftest tsasvla-streams
  (let [test-stream (s/stream)
        test-d      (tsasvla [(<!? test-stream)
                              (<!? test-stream)
                              (<!? test-stream)
                              (<!? test-stream)
                              (<!? test-stream)])]
    (dotimes [n 3]
      (s/put! test-stream n))
    (s/close! test-stream)
    (is (= @test-d [0 1 2 nil nil]))))

#_(deftest ^:benchmark benchmark-tsasvla
  (bench "invoke comp x1"
         ((comp inc) 0))
  (bench "tsasvla x1"
         @(tsasvla (inc (<!? 0))))
  (bench "tsasvla deferred x1"
         @(tsasvla (inc (<!? (d/success-deferred 0)))))
  (bench "tsasvla future 200 x1"
         @(tsasvla (inc (<!? (d/future (Thread/sleep 200) 0)))))
  (bench "invoke comp x2"
         ((comp inc inc) 0))
  (bench "tsasvla x2"
         @(tsasvla (inc (<!? (inc (<!? 0))))))
  (bench "tsasvla deferred x2"
         @(tsasvla (inc (<!? (inc (<!? (d/success-deferred 0)))))))
  (bench "tsasvla future 200 x2"
         @(tsasvla (inc (<!? (inc (<!? (d/future (Thread/sleep 200) 0)))))))
  (bench "invoke comp x5"
         ((comp inc inc inc inc inc) 0))
  (bench "tsasvla x5"
         @(tsasvla (inc (<!? (inc (<!? (inc (<!? (inc (<!? (inc (<!? 0))))))))))))
  (bench "tsasvla deferred x5"
         @(tsasvla (inc (<!? (inc (<!? (inc (<!? (inc (<!? (inc (<!? (d/success-deferred 0))))))))))))
         (bench "tsasvla future 200 x5"
                @(tsasvla (inc (<!? (inc (<!? (inc (<!? (inc (<!? (inc (<!? (d/future (Thread/sleep 200) 0)))))))))))))))