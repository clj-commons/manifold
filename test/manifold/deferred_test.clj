(ns manifold.deferred-test
  (:refer-clojure
    :exclude (realized? future))
  (:require
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.deferred :refer :all :exclude [loop]]))

(defmacro future' [& body]
  `(future
     (Thread/sleep 10)
     (try
       ~@body
       (catch Exception e#
         (.printStackTrace e#)))))

(defn capture-success
  ([result]
     (capture-success result true))
  ([result expected-return-value]
     (let [p (promise)]
       (on-realized result
         #(do (deliver p %) expected-return-value)
         (fn [_] (throw (Exception. "ERROR"))))
       p)))

(defn capture-error
  ([result]
     (capture-error result true))
  ([result expected-return-value]
     (let [p (promise)]
       (on-realized result
         (fn [_] (throw (Exception. "SUCCESS")))
         #(do (deliver p %) expected-return-value))
       p)))

(deftest test-let-flow
  (is (= 5
        @(let [z (future 1)]
           (let-flow [x (future (future z))
                      y (future (+ z x))]
             (future (+ x x y z)))))))

(deftest test-deferred
  ;; success!
  (let [d (deferred)]
    (is (= true (success! d 1)))
    (is (= 1 @(capture-success d)))
    (is (= 1 @d)))

  ;; claim and success!
  (let [d (deferred)
        token (claim! d)]
    (is token)
    (is (= false (success! d 1)))
    (is (= true (success! d 1 token)))
    (is (= 1 @(capture-success d)))
    (is (= 1 @d)))

  ;; error!
  (let [d (deferred)
        ex (IllegalStateException. "boom")]
    (is (= true (error! d ex)))
    (is (= ex @(capture-error d ::return)))
    (is (thrown? IllegalStateException @d)))

  ;; claim and error!
  (let [d (deferred)
        ex (IllegalStateException. "boom")
        token (claim! d)]
    (is token)
    (is (= false (error! d ex)))
    (is (= true (error! d ex token)))
    (is (= ex @(capture-error d ::return)))
    (is (thrown? IllegalStateException (deref d 1000 ::timeout))))

  ;; test deref with delayed result
  (let [d (deferred)]
    (future' (success! d 1))
    (is (= 1 (deref d 1000 ::timeout))))

  ;; test deref with delayed error result
  (let [d (deferred)]
    (future' (error! d (IllegalStateException. "boom")))
    (is (thrown? IllegalStateException (deref d 1000 ::timeout))))

  ;; multiple callbacks w/ success
  (let [n 50
        d (deferred)
        callback-values (->> (range n)
                          (map (fn [_] (future (capture-success d))))
                          (map deref)
                          doall)]
    (is (= true (success! d 1)))
    (is (= 1 (deref d 1000 ::timeout)))
    (is (= (repeat n 1) (map deref callback-values))))

  ;; multiple callbacks w/ error
  (let [n 50
        d (deferred)
        callback-values (->> (range n)
                          (map (fn [_] (future (capture-error d))))
                          (map deref)
                          doall)
        ex (Exception.)]
    (is (= true (error! d ex)))
    (is (thrown? Exception (deref d 1000 ::timeout)))
    (is (= (repeat n ex) (map deref callback-values))))

  ;; cancel listeners
  (let [l (listener (constantly :foo) nil)
        d (deferred)]
    (is (= false (cancel-listener! d l)))
    (is (= true (add-listener! d l)))
    (is (= true (cancel-listener! d l)))
    (is (= true (success! d :foo)))
    (is (= :foo @(capture-success d)))
    (is (= false (cancel-listener! d l)))))

;;;

(deftest ^:benchmark benchmark-chain
  (bench "invoke comp x3"
    ((comp inc inc inc) 0))
  (bench "chain x3"
    @(chain 0 inc inc inc))
  (bench "invoke comp x5"
    ((comp inc inc inc inc inc) 0))
  (bench "chain x5"
    @(chain 0 inc inc inc inc inc)))

(deftest ^:benchmark benchmark-deferred
  (bench "create deferred"
    (deferred))
  (bench "add-listener and success"
    (let [d (deferred)]
      (add-listener! d (listener (fn [_]) nil))
      (success! d 1)))
  (bench "add-listener, claim, and success!"
    (let [d (deferred)]
      (add-listener! d (listener (fn [_]) nil))
      (success! d 1 (claim! d))))
  (bench "add-listener!, cancel, add-listener! and success"
    (let [d (deferred)]
      (let [callback (listener (fn [_]) nil)]
        (add-listener! d callback)
        (cancel-listener! d callback))
      (add-listener! d (listener (fn [_]) nil))
      (success! d 1)))
  (bench "multi-add-listener! and success"
    (let [d (deferred)]
      (add-listener! d (listener (fn [_]) nil))
      (add-listener! d (listener (fn [_]) nil))
      (success! d 1)))
  (bench "multi-add-listener!, cancel, and success"
    (let [d (deferred)]
      (add-listener! d (listener (fn [_]) nil))
      (let [callback (listener (fn [_]) nil)]
        (add-listener! d callback)
        (cancel-listener! d callback))
      (success! d 1)))
  (bench "success! and add-listener!"
    (let [d (deferred)]
      (success! d 1)
      (add-listener! d (listener (fn [_]) nil))))
  (bench "claim, success!, and add-listener!"
    (let [d (deferred)]
      (success! d 1 (claim! d))
      (add-listener! d (listener (fn [_]) nil))))
  (bench "success! and deref"
    (let [d (deferred)]
      (success! d 1)
      @d))
  (bench "deliver and deref"
    (let [d (deferred)]
      (deliver d 1)
      @d)))
