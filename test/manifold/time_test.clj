(ns manifold.time-test
  (:require
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.deferred :as d]
    [manifold.time :as t]))

(deftest test-in
  (let [n (atom 0)]
    @(t/in 1 #(swap! n inc))
    (is (= 1 @n))))

(deftest test-every
  (let [n (atom 0)
        f (t/every 100 0 #(swap! n inc))]
    (Thread/sleep 10)
    (is (= 1 @n))
    (Thread/sleep 100)
    (is (= 2 @n))
    (f)
    (Thread/sleep 100)
    (is (= 2 @n))))

(deftest test-mock-clock
  (let [c (t/mock-clock 0)
        n (atom 0)
        inc #(swap! n inc)]
    (t/with-clock c

      (t/in 1 inc)
      (t/advance c 1)
      (is (= 1 @n))

      (t/in 0 inc)
      (is (= 2 @n))

      (t/in 1 inc)
      (t/in 1 inc)
      (t/advance c 1)
      (is (= 4 @n))

      (let [cancel (t/every 5 1 inc)]
        (is (= 4 @n))
        (t/advance c 1)
        (is (= 5 @n))
        (t/advance c 1)
        (is (= 5 @n))
        (t/advance c 4)
        (is (= 6 @n))

        (cancel)
        (t/advance c 5)
        (is (= 6 @n))))))
