(ns eventual.promise-test
  (:refer-clojure :exclude [promise realized? future])
  (:require
    [clojure.test :refer :all]
    [eventual.test-utils :refer :all]
    [eventual.promise :refer :all]))

(defmacro defer [& body]
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
     (let [p (clojure.core/promise)]
       (on-realized result
         #(do (deliver p %) expected-return-value)
         (fn [_] (throw (Exception. "ERROR"))))
       p)))

(defn capture-error
  ([result]
     (capture-error result true))
  ([result expected-return-value]
     (let [p (clojure.core/promise)]
       (on-realized result
         (fn [_] (throw (Exception. "SUCCESS")))
         #(do (deliver p %) expected-return-value))
       p)))

(deftest test-promise
  ;; success!
  (let [p (promise)]
    (is (= true (success! p 1)))
    (is (= 1 @(capture-success p)))
    (is (= 1 @p)))

  ;; claim and success!
  (let [p (promise)
        token (claim! p)]
    (is token)
    (is (= false (success! p 1)))
    (is (= true (success! p 1 token)))
    (is (= 1 @(capture-success p)))
    (is (= 1 @p)))

  ;; error!
  (let [p (promise)
        ex (IllegalStateException. "boom")]
    (is (= true (error! p ex)))
    (is (= ex @(capture-error p ::return)))
    (is (thrown? IllegalStateException @p)))

  ;; claim and error!
  (let [p (promise)
        ex (IllegalStateException. "boom")
        token (claim! p)]
    (is token)
    (is (= false (error! p ex)))
    (is (= true (error! p ex token)))
    (is (= ex @(capture-error p ::return)))
    (is (thrown? IllegalStateException (deref p 1000 ::timeout))))

  ;; test deref with delayed result
  (let [p (promise)]
    (defer (success! p 1))
    (is (= 1 (deref p 1000 ::timeout))))

  ;; test deref with delayed error result
  (let [p (promise)]
    (defer (error! p (IllegalStateException. "boom")))
    (is (thrown? IllegalStateException (deref p 1000 ::timeout))))

  ;; multiple callbacks w/ success
  (let [n 50
        p (promise)
        callback-values (->> (range n)
                          (map (fn [_] (future (capture-success p))))
                          (map deref)
                          doall)]
    (is (= true (success! p 1)))
    (is (= 1 (deref p 1000 ::timeout)))
    (is (= (repeat n 1) (map deref callback-values))))

  ;; multiple callbacks w/ error
  (let [n 50
        p (promise)
        callback-values (->> (range n)
                          (map (fn [_] (future (capture-error p))))
                          (map deref)
                          doall)
        ex (Exception.)]
    (is (= true (error! p ex)))
    (is (thrown? Exception (deref p 1000 ::timeout)))
    (is (= (repeat n ex) (map deref callback-values))))

  ;; cancel listeners
  (let [l (listener (constantly :foo) nil)
        p (promise)]
    (is (= false (cancel-listener! p l)))
    (is (= true (add-listener! p l)))
    (is (= true (cancel-listener! p l)))
    (is (= true (success! p :foo)))
    (is (= :foo @(capture-success p)))
    (is (= false (cancel-listener! p l)))))

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

(deftest ^:benchmark benchmark-promise
  (bench "create promise"
    (promise))
  (bench "add-listener and success"
    (let [p (promise)]
      (add-listener! p (listener (fn [_]) nil))
      (success! p 1)))
  (bench "add-listener, claim, and success!"
    (let [p (promise)]
      (add-listener! p (listener (fn [_]) nil))
      (success! p 1 (claim! p))))
  (bench "add-listener!, cancel, add-listener! and success"
    (let [p (promise)]
      (let [callback (listener (fn [_]) nil)]
        (add-listener! p callback)
        (cancel-listener! p callback))
      (add-listener! p (listener (fn [_]) nil))
      (success! p 1)))
  (bench "multi-add-listener! and success"
    (let [p (promise)]
      (add-listener! p (listener (fn [_]) nil))
      (add-listener! p (listener (fn [_]) nil))
      (success! p 1)))
  (bench "multi-add-listener!, cancel, and success"
    (let [p (promise)]
      (add-listener! p (listener (fn [_]) nil))
      (let [callback (listener (fn [_]) nil)]
        (add-listener! p callback)
        (cancel-listener! p callback))
      (success! p 1)))
  (bench "success! and add-listener!"
    (let [p (promise)]
      (success! p 1)
      (add-listener! p (listener (fn [_]) nil))))
  (bench "claim, success!, and add-listener!"
    (let [p (promise)]
      (success! p 1 (claim! p))
      (add-listener! p (listener (fn [_]) nil))))
  (bench "success! and deref"
    (let [p (promise)]
      (success! p 1)
      @p))
  (bench "deliver and deref"
    (let [p (promise)]
      (deliver p 1)
      @p)))
