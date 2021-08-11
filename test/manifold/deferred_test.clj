(ns manifold.deferred-test
  (:refer-clojure
    :exclude (realized? future loop))
  (:require
    [manifold.utils :as utils]
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.deferred :as d]
    [manifold.executor :as ex]))

(defmacro future' [& body]
  `(d/future
     (Thread/sleep 10)
     (try
       ~@body
       (catch Exception e#
         (.printStackTrace e#)))))

(defn future-error
  [ex]
  (d/future
    (Thread/sleep 10)
    (throw ex)))

(defn capture-success
  ([result]
    (capture-success result true))
  ([result expected-return-value]
    (let [p (promise)]
      (d/on-realized result
        #(do (deliver p %) expected-return-value)
        (fn [_] (throw (Exception. "ERROR"))))
      p)))

(defn capture-error
  ([result]
    (capture-error result true))
  ([result expected-return-value]
    (let [p (promise)]
      (d/on-realized result
        (fn [_] (throw (Exception. "SUCCESS")))
        #(do (deliver p %) expected-return-value))
      p)))

(deftest test-catch
  (is (thrown? ArithmeticException
        @(-> 0
           (d/chain #(/ 1 %))
           (d/catch IllegalStateException (constantly :foo)))))

  (is (thrown? ArithmeticException
        @(-> 0
           d/future
           (d/chain #(/ 1 %))
           (d/catch IllegalStateException (constantly :foo)))))

  (is (= :foo
        @(-> 0
           (d/chain #(/ 1 %))
           (d/catch ArithmeticException (constantly :foo)))))

  (let [d (d/deferred)]
    (d/future (Thread/sleep 100) (d/error! d :bar))
    (is (= :foo @(d/catch d (constantly :foo)))))

  (is (= :foo
        @(-> (d/error-deferred :bar)
           (d/catch (constantly :foo)))))

  (is (= :foo
        @(-> 0
           d/future
           (d/chain #(/ 1 %))
           (d/catch ArithmeticException (constantly :foo))))))

(def ^:dynamic *test-dynamic-var*)

(let [execute-pool-promise
      (delay
        (let [cnt (atom 0)]
          (ex/utilization-executor 0.95 Integer/MAX_VALUE
                                   {:thread-factory (ex/thread-factory
                                                      #(str "manifold-test-execute-" (swap! cnt inc))
                                                      (deliver (promise) nil))
                                    :stats-callback (constantly nil)})))]
  (defn test-execute-pool []
    @execute-pool-promise))

(deftest test-let-flow

  (let [flag (atom false)]
    @(let [z (clojure.core/future 1)]
       (d/let-flow [x (d/future (clojure.core/future z))
                    _ (d/future (Thread/sleep 1000) (reset! flag true))
                    y (d/future (+ z x))]
         (d/future (+ x x y z))))
    (is (= true @flag)))

  (is (= 5
        @(let [z (clojure.core/future 1)]
           (d/let-flow [x (d/future (clojure.core/future z))
                        y (d/future (+ z x))]
             (d/future (+ x x y z))))))

  (is (= 2
        @(let [d (d/deferred)]
           (d/let-flow [[x] (future' [1])]
             (d/let-flow [[x'] (future' [(inc x)])
                          y (future' true)]
               (when y x'))))))

  (testing "let-flow callbacks happen on different executor retain thread bindings"
    (let [d                (d/deferred (test-execute-pool))
          test-internal-fn (fn [] (let [x *test-dynamic-var*]
                                    (d/future (Thread/sleep 100) (d/success! d x))))]
      (binding [*test-dynamic-var* "cat"]
        (test-internal-fn)
        (is (= ["cat" "cat" "cat"]
               @(d/let-flow [a d
                             b (do a *test-dynamic-var*)]
                  [a b *test-dynamic-var*]))))))

  (let [start          (System/currentTimeMillis)
        future-timeout (d/future (Thread/sleep 500) "b")
        expected       (d/future (Thread/sleep 5) "cat")]
    @(d/let-flow [x (d/alt future-timeout expected)]
       x)

    (is (>= 300 (- (System/currentTimeMillis) start))
        "Alt in let-flow should only take as long as the first deferred to finish."))

  (is (every? #(= "cat" %)
              (for [i (range 50)]
                (let [future-timeout (d/future (Thread/sleep 100) "b")
                      expected       (d/future (Thread/sleep 5) "cat")]
                  @(d/let-flow [x (d/alt future-timeout expected)]
                     x))))
      "Resolution of deferreds in alt inside a let-flow should always be consistent.")

  (let [start          (System/currentTimeMillis)
        future-timeout (d/future (Thread/sleep 300) "b")
        expected       (d/future (Thread/sleep 5) "cat")]
    (is (= "cat"
           @(d/let-flow [x (d/alt future-timeout expected)
                         y (d/alt x future-timeout)]
              (d/alt future-timeout y)))
        "Alts referencing newly introduced symbols shouldn't cause compiler errors.")
    (is (>= 200 (- (System/currentTimeMillis) start))
        "Alt in body should only take as long as the first deferred to finish."))

  (is (= ::timeout
         @(d/let-flow [x (d/timeout! (d/future (Thread/sleep 1000) "cat") 50 ::timeout)]
            x))
      "Timeouts introduced in let-flow should be respected.")

  (let [start (System/currentTimeMillis)
        slow  (d/future (Thread/sleep 300) "slow")
        fast  (d/future (Thread/sleep 5) "fast")]
    (is (= "fast"
           @(d/let-flow [x "cat"]
              (d/let-flow [z (d/alt slow fast)]
                z)))
        "let-flow's should behave identically inside the body of another let-flow")
    (is (= "fast"
           @(d/let-flow [x "cat"
                         y (d/let-flow [z (d/alt slow fast)]
                             z)]
              y))
        "let-flow's should behave identically inside the bindings of another let-flow")
    (is (>= 200 (- (System/currentTimeMillis) start))
        "let-flow's should behave identically inside another let-flow")))

(deftest test-chain-errors
  (let [boom (fn [n] (throw (ex-info "" {:n n})))]
    (doseq [b [boom (fn [n] (d/future (boom n)))]]
      (dorun
        (for [i (range 10)
              j (range 10)]
          (let [fs (concat (repeat i inc) [boom] (repeat j inc))]
            (is (= i
                  @(-> (apply d/chain 0 fs)
                     (d/catch (fn [e] (:n (ex-data e)))))
                  @(-> (apply d/chain' 0 fs)
                     (d/catch' (fn [e] (:n (ex-data e)))))))))))))

(deftest test-chain
  (dorun
    (for [i (range 10)
          j (range i)]
      (let [fs (take i (cycle [inc #(* % 2)]))
            fs' (-> fs
                  vec
                  (update-in [j] (fn [f] #(d/future (f %)))))]
        (is
          (= (reduce #(%2 %1) 0 fs)
            @(apply d/chain 0 fs')
            @(apply d/chain' 0 fs')))))))

(deftest test-deferred
  ;; success!
  (let [d (d/deferred)]
    (is (= true (d/success! d 1)))
    (is (= 1 @(capture-success d)))
    (is (= 1 @d)))

  ;; claim and success!
  (let [d (d/deferred)
        token (d/claim! d)]
    (is token)
    (is (= false (d/success! d 1)))
    (is (= true (d/success! d 1 token)))
    (is (= 1 @(capture-success d)))
    (is (= 1 @d)))

  ;; error!
  (let [d (d/deferred)
        ex (IllegalStateException. "boom")]
    (is (= true (d/error! d ex)))
    (is (= ex @(capture-error d ::return)))
    (is (thrown? IllegalStateException @d)))

  ;; claim and error!
  (let [d (d/deferred)
        ex (IllegalStateException. "boom")
        token (d/claim! d)]
    (is token)
    (is (= false (d/error! d ex)))
    (is (= true (d/error! d ex token)))
    (is (= ex @(capture-error d ::return)))
    (is (thrown? IllegalStateException (deref d 1000 ::timeout))))

  ;; test deref with delayed result
  (let [d (d/deferred)]
    (future' (d/success! d 1))
    (is (= 1 (deref d 1000 ::timeout))))

  ;; test deref with delayed error result
  (let [d (d/deferred)]
    (future' (d/error! d (IllegalStateException. "boom")))
    (is (thrown? IllegalStateException (deref d 1000 ::timeout))))

  ;; test deref with non-Throwable error result
  (are [d timeout]
    (= :bar
       (-> (is (thrown? clojure.lang.ExceptionInfo
                 (if timeout (deref d 1000 ::timeout) @d)))
         ex-data
         :error))

    (doto (d/deferred) (d/error! :bar)) true

    (doto (d/deferred) (as-> d (future' (d/error! d :bar)))) true

    (d/error-deferred :bar) true

    (d/error-deferred :bar) false)

  ;; multiple callbacks w/ success
  (let [n 50
        d (d/deferred)
        callback-values (->> (range n)
                          (map (fn [_] (d/future (capture-success d))))
                          (map deref)
                          doall)]
    (is (= true (d/success! d 1)))
    (is (= 1 (deref d 1000 ::timeout)))
    (is (= (repeat n 1) (map deref callback-values))))

  ;; multiple callbacks w/ error
  (let [n 50
        d (d/deferred)
        callback-values (->> (range n)
                          (map (fn [_] (d/future (capture-error d))))
                          (map deref)
                          doall)
        ex (Exception.)]
    (is (= true (d/error! d ex)))
    (is (thrown? Exception (deref d 1000 ::timeout)))
    (is (= (repeat n ex) (map deref callback-values))))

  ;; cancel listeners
  (let [l (d/listener (constantly :foo) nil)
        d (d/deferred)]
    (is (= false (d/cancel-listener! d l)))
    (is (= true (d/add-listener! d l)))
    (is (= true (d/cancel-listener! d l)))
    (is (= true (d/success! d :foo)))
    (is (= :foo @(capture-success d)))
    (is (= false (d/cancel-listener! d l))))

  ;; deref
  (let [d (d/deferred)]
    (is (= :foo (deref d 10 :foo)))
    (d/success! d 1)
    (is (= 1 @d))
    (is (= 1 (deref d 10 :foo)))))

(deftest test-loop
  ;; body produces a non-deferred value
  (is @(capture-success
         (d/loop [] true)))

  ;; body raises exception
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                 (d/loop [] (throw ex))))))

  ;; body produces a realized result
  (is @(capture-success
         (d/loop [] (d/success-deferred true))))

  ;; body produces a realized error result
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                 (d/loop [] (d/error-deferred ex))))))

  ;; body produces a delayed result
  (is @(capture-success
         (d/loop [] (future' true))))

  ;; body produces a delayed error result
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                 (d/loop [] (future-error ex))))))

  ;; destructuring works for loop parameters
  (is (= 1 @(capture-success
              (d/loop [{:keys [a]} {:a 1}] a))))
  (is @(capture-success
         (d/loop [[x & xs] [1 2 3]] (or (= x 3) (d/recur xs))))))

(deftest test-coercion
  (is (= 1 (-> 1 clojure.core/future d/->deferred deref)))

  (utils/when-class java.util.concurrent.CompletableFuture
    (let [f (java.util.concurrent.CompletableFuture.)]
      (.obtrudeValue f 1)
      (is (= 1 (-> f d/->deferred deref))))

    (let [f (java.util.concurrent.CompletableFuture.)]
      (.obtrudeException f (Exception.))
      (is (thrown? Exception (-> f d/->deferred deref))))))

(deftest test-finally
  (let [target-d (d/deferred)
        d (d/deferred)
        fd (d/finally
             d
             (fn []
               (d/success! target-d ::delivered)))]
    (d/error! d (Exception.))
    (is (= ::delivered (deref target-d 0 ::not-delivered)))))

(deftest test-alt
  (is (#{1 2 3} @(d/alt 1 2 3)))
  (is (= 2 @(d/alt (d/future (Thread/sleep 10) 1) 2)))

  (is (= 2 @(d/alt (d/future (Thread/sleep 10) (throw (Exception. "boom"))) 2)))

  (is (thrown-with-msg? Exception #"boom"
        @(d/alt (d/future (throw (Exception. "boom"))) (d/future (Thread/sleep 10)))))

  (testing "uniformly distributed"
    (let [results (atom {})
          ;; within 10%
          n 1e4, r 10, eps (* n 0.1)
          f #(/ (% n eps) r)]
      (dotimes [_ n]
        @(d/chain (apply d/alt (range r))
                  #(swap! results update % (fnil inc 0))))
      (doseq [[i times] @results]
        (is (<= (f -) times (f +)))))))

;;;

(deftest ^:benchmark benchmark-chain
  (bench "invoke comp x1"
    ((comp inc) 0))
  (bench "chain x1"
    @(d/chain 0 inc))
  (bench "chain' x1"
    @(d/chain' 0 inc))
  (bench "invoke comp x2"
    ((comp inc inc) 0))
  (bench "chain x2"
    @(d/chain 0 inc inc))
  (bench "chain' x2"
    @(d/chain' 0 inc inc))
  (bench "invoke comp x5"
    ((comp inc inc inc inc inc) 0))
  (bench "chain x5"
    @(d/chain 0 inc inc inc inc inc))
  (bench "chain' x5"
    @(d/chain' 0 inc inc inc inc inc)))

(deftest ^:benchmark benchmark-deferred
  (bench "create deferred"
    (d/deferred))
  (bench "add-listener and success"
    (let [d (d/deferred)]
      (d/add-listener! d (d/listener (fn [_]) nil))
      (d/success! d 1)))
  (bench "add-listener, claim, and success!"
    (let [d (d/deferred)]
      (d/add-listener! d (d/listener (fn [_]) nil))
      (d/success! d 1 (d/claim! d))))
  (bench "add-listener!, cancel, add-listener! and success"
    (let [d (d/deferred)]
      (let [callback (d/listener (fn [_]) nil)]
        (d/add-listener! d callback)
        (d/cancel-listener! d callback))
      (d/add-listener! d (d/listener (fn [_]) nil))
      (d/success! d 1)))
  (bench "multi-add-listener! and success"
    (let [d (d/deferred)]
      (d/add-listener! d (d/listener (fn [_]) nil))
      (d/add-listener! d (d/listener (fn [_]) nil))
      (d/success! d 1)))
  (bench "multi-add-listener!, cancel, and success"
    (let [d (d/deferred)]
      (d/add-listener! d (d/listener (fn [_]) nil))
      (let [callback (d/listener (fn [_]) nil)]
        (d/add-listener! d callback)
        (d/cancel-listener! d callback))
      (d/success! d 1)))
  (bench "success! and add-listener!"
    (let [d (d/deferred)]
      (d/success! d 1)
      (d/add-listener! d (d/listener (fn [_]) nil))))
  (bench "claim, success!, and add-listener!"
    (let [d (d/deferred)]
      (d/success! d 1 (d/claim! d))
      (d/add-listener! d (d/listener (fn [_]) nil))))
  (bench "success! and deref"
    (let [d (d/deferred)]
      (d/success! d 1)
      @d))
  (bench "deliver and deref"
    (let [d (d/deferred)]
      (deliver d 1)
      @d)))

(deftest ^:stress test-error-leak-detection

  (d/error-deferred (Throwable.))
  (System/gc)

  (dotimes [_ 2e3]
    (d/error! (d/deferred) (Throwable.)))
  (System/gc))

(deftest ^:stress test-deferred-chain
  (dotimes [_ 1e4]
    (let [d (d/deferred)
          result (d/future
                   (last
                     (take 1e4
                       (iterate
                         #(let [d' (d/deferred)]
                            (d/connect % d')
                            d')
                         d))))]
      (Thread/sleep (rand-int 10))
      (d/success! d 1)
      (is (= 1 @@result)))))

