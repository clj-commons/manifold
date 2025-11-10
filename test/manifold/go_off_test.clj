(ns manifold.go-off-test
  (:require [clojure.test :refer :all]
            [manifold.go-off :refer [go-off <!? go-off-with]]
            [manifold.deferred :as d]
            [manifold.test :refer :all]
            [manifold.test-utils :refer :all]
            [manifold.executor :as ex]
            [clojure.string :as str]
            [manifold.stream :as s])
  (:import (java.util.concurrent TimeoutException)))

(deftest async-test
  (testing "values are returned correctly"
    (is (= 10
           @(go-off (<!? (d/success-deferred 10))))))

  (testing "case with go-off"
    (is (= :1
           @(go-off (case (name :1)
                      "0" :0
                      "1" :1
                      :3)))))

  (testing "nil result of go-off"
    (is (= nil
           @(go-off nil))))

  (testing "take inside binding of loop"
    (is (= 42
           @(go-off (loop [x (<!? (d/success-deferred 42))]
                      x)))))

  (testing "can get from a catch"
    (let [c (d/success-deferred 42)]
      (is (= 42
             @(go-off (try
                        (assert false)
                        (catch Throwable ex (<!? c)))))))))

(deftest enqueued-chan-ops
  (testing "enqueued channel takes re-enter async properly"
    (is (= :foo
           (let [d          (d/deferred)
                 async-chan (go-off (<!? d))]
             (d/success! d :foo)
             @async-chan)))

    (is (= 3
           (let [d1         (d/deferred)
                 d2         (d/deferred)
                 d3         (d/deferred)
                 async-chan (go-off (+ (<!? d1) (<!? d2) (<!? d3)))]
             (d/success! d3 1)
             (d/success! d2 1)
             (d/success! d1 1)
             @async-chan)))))

(deftest go-off-nests
  (testing "return deferred will always result in a a realizable value, not another deferred"
    (is (= [23 42] @(go-off (let [let* 1 a 23] (go-off (let* [b 42] [a b]))))))
    (is (= 5 @(go-off (go-off (go-off (go-off (go-off 5))))))))
  (testing "Parking unwraps nested deferreds"
    (is (= 5 @(go-off (<!? (go-off (go-off (go-off 5)))))))))

(deftest error-propagation
  (is (= "chained catch"
         @(d/catch (go-off (/ 5 0))
                   (constantly "chained catch"))))

  (is (= "try/catch in block"
         @(go-off (try (/ 5 0)
                       (catch Throwable _ "try/catch in block")))))

  (testing "Try/catch around parking will continue block"
    (is (= "try/catch parking"
           @(go-off (try (<!? (d/future (/ 5 0)))
                         (catch Throwable _ "try/catch parking")))))
    (is (= 5
           @(go-off (try (<!? (d/future (/ 5 0)))
                         (catch Throwable _))
                    5))))

  (testing "Normal deferred handling still works"
    (is (= 5
           @(go-off (<!? (d/catch (d/future (/ 5 0)) (constantly 5))))))))

(deftest non-deferred-takes
  (testing "Can take from non-deferreds"
    (is (= 5 @(go-off (<!? 5))))
    (is (= "test" @(go-off (<!? "test"))))))

(deftest already-realized-values
  (testing "When taking from already realized values, the threads should not change."
    (let [original-thread (atom nil)]
      (is (= @(go-off (reset! original-thread (Thread/currentThread))
                      (<!? "cat")
                      (Thread/currentThread))
             @original-thread)))

    (let [original-thread (atom nil)]
      (is (= @(go-off (reset! original-thread (Thread/currentThread))
                      (<!? (d/success-deferred "cat"))
                      (Thread/currentThread))
             @original-thread)))

    (testing "Taking from already realized value doesn't cause remaining body to run twice"
      (let [blow-up-counter (atom 0)
            blow-up-fn      (fn [& _] (is (= 1 (swap! blow-up-counter inc))))]
        @(go-off (<!? "cat")
                 (blow-up-fn))))
    ;; Sleep is here to make sure that the secondary invocation of `blow-up-fn` that was happening has
    ;; had time to report it's failure before the test finishes
    (Thread/sleep 500)))

(deftest deferred-interactions
  (testing "timeouts"
    (is (= ::timeout @(go-off (<!? (d/timeout! (d/deferred) 10 ::timeout)))))
    (is (= ::timeout @(d/timeout! (go-off (<!? (d/deferred))) 10 ::timeout)))
    (is (thrown? TimeoutException @(go-off (<!? (d/timeout! (d/deferred) 11)))))
    (is (thrown? TimeoutException @(d/timeout! (go-off (<!? (d/deferred))) 12))))

  (testing "alt"
    (is (= ::timeout @(go-off (<!? (d/alt (d/deferred) (d/timeout! (d/deferred) 13 ::timeout))))))
    (is (= ::timeout @(d/alt (go-off (<!? (d/deferred))) (d/timeout! (d/deferred) 14 ::timeout))))
    (is (= 1 @(go-off (<!? (d/alt (d/deferred) (d/success-deferred 1))))))
    (is (= 1 @(d/alt (go-off (<!? (d/deferred))) (d/success-deferred 1))))))

(deftest go-off-specify-executor-pool
  (let [prefix          "go-off-custom-executor"
        cnt             (atom 0)
        custom-executor (ex/utilization-executor 0.95 Integer/MAX_VALUE
                                                 {:thread-factory (ex/thread-factory
                                                                    #(str prefix (swap! cnt inc))
                                                                    (deliver (promise) nil))
                                                  :stats-callback (constantly nil)})]
    (try (is (str/starts-with? @(go-off-with custom-executor (.getName (Thread/currentThread))) prefix)
             "Running on custom executor, thread naming should be respected.")
         @(go-off-with custom-executor (.getName (Thread/currentThread)))
         (finally (.shutdown custom-executor)))))

(deftest go-off-streams
  (let [test-stream (s/stream)
        test-d      (go-off [(<!? test-stream)
                             (<!? test-stream)
                             (<!? test-stream)
                             (<!? test-stream)
                             (<!? test-stream)])]
    (dotimes [n 3]
      (s/put! test-stream n))
    (s/close! test-stream)
    (is (= @test-d [0 1 2 nil nil]))))

(deftest ^:ignore-dropped-errors ^:benchmark benchmark-go-off
  (bench "invoke comp x1"
         ((comp inc) 0))
  (bench "go-off x1"
         @(go-off (inc (<!? 0))))
  (bench "go-off deferred x1"
         @(go-off (inc (<!? (d/success-deferred 0)))))
  (bench "go-off future 200 x1"
         @(go-off (inc (<!? (d/future (Thread/sleep 200) 0)))))
  (bench "invoke comp x2"
         ((comp inc inc) 0))
  (bench "go-off x2"
         @(go-off (inc (<!? (inc (<!? 0))))))
  (bench "go-off deferred x2"
         @(go-off (inc (<!? (inc (<!? (d/success-deferred 0)))))))
  (bench "go-off future 200 x2"
         @(go-off (inc (<!? (inc (<!? (d/future (Thread/sleep 200) 0)))))))
  (bench "invoke comp x5"
         ((comp inc inc inc inc inc) 0))
  (bench "go-off x5"
         @(go-off (inc (<!? (inc (<!? (inc (<!? (inc (<!? (inc (<!? 0))))))))))))
  (bench "go-off deferred x5"
         @(go-off (inc (<!? (inc (<!? (inc (<!? (inc (<!? (inc (<!? (d/success-deferred 0))))))))))))
         (bench "go-off future 200 x5"
                @(go-off (inc (<!? (inc (<!? (inc (<!? (inc (<!? (inc (<!? (d/future (Thread/sleep 200) 0)))))))))))))))

(instrument-tests-with-dropped-error-detection!)
