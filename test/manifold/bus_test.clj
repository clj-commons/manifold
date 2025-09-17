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

(instrument-tests-with-dropped-error-detection!)
