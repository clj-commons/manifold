(ns manifold.deferred-test
  (:refer-clojure
    :exclude (realized? future loop))
  (:require
    [manifold.utils :as utils]
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.deferred :refer :all]))

(defmacro future' [& body]
  `(future
     (Thread/sleep 10)
     (try
       ~@body
       (catch Exception e#
         (.printStackTrace e#)))))

(defn future-error
  [ex]
  (future
    (Thread/sleep 10)
    (throw ex)))

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
        @(let [z (clojure.core/future 1)]
           (let-flow [x (future (clojure.core/future z))
                      y (future (+ z x))]
             (future (+ x x y z))))))

  (is (= 2
        @(let [d (deferred)]
           (let-flow [[x] (future' [1])]
             (let-flow [[x'] (future' [(inc x)])
                        y (future' true)]
               (when y x')))))))

(deftest test-chain-errors
  (let [boom (fn [n] (throw (ex-info "" {:n n})))]
    (doseq [b [boom (fn [n] (future (boom n)))]]
      (dorun
        (for [i (range 10)
              j (range 10)]
          (let [fs (concat (repeat i inc) [boom] (repeat j inc))]
            (is (= i
                  @(-> (apply chain 0 fs)
                     (catch (fn [e] (:n (ex-data e)))))))))))))

(deftest test-chain
  (dorun
    (for [i (range 10)
          j (range i)]
      (let [fs (take i (cycle [inc #(* % 2)]))
            fs' (-> fs
                  vec
                  (update-in [j] (fn [f] #(future %))))]
        (is
          (= (reduce #(%2 %1) 0 fs)
            @(apply chain 0 fs)
            @(apply chain' 0 fs)))))))

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
    (is (= false (cancel-listener! d l))))

  ;; deref
  (let [d (deferred)]
    (is (= :foo (deref d 10 :foo)))
    (success! d 1)
    (is (= 1 @d))
    (is (= 1 (deref d 10 :foo)))))

(deftest test-loop
  ;; body produces a non-deferred value
  (is @(capture-success
         (loop [] true)))

  ;; body raises exception
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                 (loop [] (throw ex))))))

  ;; body produces a realized result
  (is @(capture-success
         (loop [] (success-deferred true))))

  ;; body produces a realized error result
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                 (loop [] (error-deferred ex))))))

  ;; body produces a delayed result
  (is @(capture-success
         (loop [] (future' true))))

  ;; body produces a delayed error result
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                 (loop [] (future-error ex)))))))

(deftest test-coercion
  (is (= 1 (-> 1 clojure.core/future ->deferred deref)))

  (utils/when-class java.util.concurrent.CompletableFuture
    (let [f (java.util.concurrent.CompletableFuture.)]
      (.obtrudeValue f 1)
      (is (= 1 (-> f ->deferred deref))))

    (let [f (java.util.concurrent.CompletableFuture.)]
      (.obtrudeException f (Exception.))
      (is (thrown? Exception (-> f ->deferred deref))))))

;;;

(deftest ^:benchmark benchmark-chain
  (bench "invoke comp x1"
    ((comp inc) 0))
  (bench "chain x1"
    @(chain 0 inc))
  (bench "chain' x1"
    @(chain' 0 inc))
  (bench "invoke comp x2"
    ((comp inc inc) 0))
  (bench "chain x2"
    @(chain 0 inc inc))
  (bench "chain' x2"
    @(chain' 0 inc inc))
  (bench "invoke comp x5"
    ((comp inc inc inc inc inc) 0))
  (bench "chain x5"
    @(chain 0 inc inc inc inc inc))
  (bench "chain' x5"
    @(chain' 0 inc inc inc inc inc)))

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

(deftest ^:stress test-error-leak-detection

  (error-deferred (Throwable.))
  (System/gc)

  (dotimes [_ 2e3]
    (error! (deferred) (Throwable.)))
  (System/gc))
