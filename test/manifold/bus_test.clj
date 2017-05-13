(ns manifold.bus-test
  (:require
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.stream :as s]
    [manifold.deferred :as d]
    [manifold.bus :as b]))

(deftest test-bus
  (let [b (b/event-bus)]
    (is (= false @(b/publish! b :foo 1)))
    (is (= false @(b/publish! b :bar 2)))
    (let [s (b/subscribe b :foo)
          d (b/publish! b :foo 2)]
      (is (= 2 @(s/take! s)))
      (is (= true @d))
      (s/close! s)
      (is (= false @(b/publish! b :foo 2))))))

(deftest test-topic-equality
  (let [b (b/event-bus)
        s (b/subscribe b (int 1))
        d (b/publish! b (long 1) 42)]
    (is (= 42 @(s/take! s)))
    (is (= true @d))))

(deftest test-timeout-subscriptions
  (let [b (b/event-bus)
        drop-after-ms 20
        e 5
        c #(let [s (s/stream)]
             (s/connect % s {:timeout drop-after-ms})
             s)
        fast (c (b/subscribe b :topic-1))
        slow (c (b/subscribe b :topic-1))
        results (atom {})
        consumer (fn [id ms] #(do (swap! results assoc id %) (Thread/sleep ms)))
        publish! #(let [start (System/nanoTime)]
                    (-> (b/publish! b :topic-1 %)
                        (d/chain (fn [_]
                                   (double (/ (- (System/nanoTime) start) 1000 1000))))
                        (deref 100 ::timeout)))]
    (s/consume (consumer :fast 0) fast)
    (s/consume (consumer :slow 50) slow)

    (is (>= (+ drop-after-ms e) (publish! 1)))
    (is (= {:fast 1, :slow 1} @results))

    (is (>= (+ drop-after-ms e) (publish! 2)))
    (is (= {:fast 2, :slow 1} @results))
    (is (s/drained? slow))))
