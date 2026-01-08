(ns manifold.deferred-test
  (:refer-clojure :exclude (realized? future loop))
  (:require
    [clojure.test :refer :all]
    [manifold.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.debug :as debug]
    [manifold.deferred :as d]
    [manifold.executor :as ex])
  (:import
    (java.util.concurrent
      CompletableFuture
      CompletionStage
      TimeoutException)
    (manifold.deferred IDeferred)))

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

(defn capture-callback-thread
  ([d attach]
   (capture-callback-thread d attach (fn [_ _])))
  ([d attach realize!]
   (let [t (d/deferred)]
     (-> d
         (attach (fn [& _]
                   (d/success! t (Thread/currentThread))))
         ;; Silence dropped error detection for finally + error cases
         (d/catch' identity))
     (realize! d [attach realize!])
     @(d/timeout! t 100 ::timeout))))

(deftest test-executors
  (let [ex (ex/fixed-thread-executor 1)]
    (doseq [make-deferred [#'d/deferred
                           #'d/success-deferred
                           #'d/error-deferred]]
      (doseq [[attach realize!] [[#'d/chain    #'d/success!]
                                 [#'d/chain'   #'d/success!]
                                 [#'d/catch    #'d/error!]
                                 [#'d/catch'   #'d/error!]
                                 [#'d/finally  #'d/success!]
                                 [#'d/finally  #'d/error!]
                                 [#'d/finally' #'d/success!]
                                 [#'d/finally' #'d/error!]]]
        (when (condp = make-deferred
                #'d/deferred true
                #'d/success-deferred (or (= realize! #'d/success!)
                                         (= attach #'d/finally)
                                         (= attach #'d/finally'))
                #'d/error-deferred (or (= realize! #'d/error!)
                                       (= attach #'d/finally)
                                       (= attach #'d/finally'))
                false)
          (testing (str "Using " (:name (meta attach)) ":")
            (testing "Deferreds without an executor invoke callbacks on the thread which realizes them."
              (let [d (if (= make-deferred #'d/deferred)
                        (make-deferred)
                        (make-deferred ::value))]
                (is (= (Thread/currentThread)
                       (capture-callback-thread d attach realize!)))
                (when (= make-deferred #'d/deferred) ; all other deferred types are immediately realized anyway
                  (testing "This is also the case for callbacks attached after realization."
                    (is (= (Thread/currentThread)
                           (capture-callback-thread d attach)))))))
            (testing "Deferreds with an executor invoke callbacks on a thread from that executor."
              (let [d (if (= make-deferred #'d/deferred)
                        (make-deferred ex)
                        (make-deferred ::value ex))
                    t (capture-callback-thread d attach realize!)]
                (when (is (instance? Thread t))
                  (is (not= t (Thread/currentThread)))
                  (is (re-find #"manifold-pool" (.getName t))))
                (when (= make-deferred #'d/deferred) ; all other deferred types are immediately realized anyway
                  (testing "This is also the case for callbacks attached after realization."
                    (let [t (capture-callback-thread d attach)]
                      (when (is (instance? Thread t))
                        (is (not= t (Thread/currentThread)))
                        (is (re-find #"manifold-pool" (.getName t)))))))))))))))

(def ^:dynamic *test-dynamic-var*)

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
    (let [d (d/deferred (ex/fixed-thread-executor 1))
          test-internal-fn (fn [] (let [x *test-dynamic-var*]
                                    (d/future (Thread/sleep 100) (d/success! d x))))]
      (binding [*test-dynamic-var* "cat"]
        (test-internal-fn)
        (is (= ["cat" "cat" "cat"]
               @(d/let-flow [a d
                             b (do a *test-dynamic-var*)]
                  [a b *test-dynamic-var*])))))))

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
      (let [fs  (take i (cycle [inc #(* % 2)]))
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
  (let [d     (d/deferred)
        token (d/claim! d)]
    (is token)
    (is (= false (d/success! d 1)))
    (is (= true (d/success! d 1 token)))
    (is (= 1 @(capture-success d)))
    (is (= 1 @d)))

  ;; error!
  (let [d  (d/deferred)
        ex (IllegalStateException. "boom")]
    (is (= true (d/error! d ex)))
    (is (= ex @(capture-error d ::return)))
    (is (thrown? IllegalStateException @d)))

  ;; claim and error!
  (let [d     (d/deferred)
        ex    (IllegalStateException. "boom")
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
  (let [n               50
        d               (d/deferred)
        callback-values (->> (range n)
                             (map (fn [_] (d/future (capture-success d))))
                             (map deref)
                             doall)]
    (is (= true (d/success! d 1)))
    (is (= 1 (deref d 1000 ::timeout)))
    (is (= (repeat n 1) (map deref callback-values))))

  ;; multiple callbacks w/ error
  (let [n               50
        d               (d/deferred)
        callback-values (->> (range n)
                             (map (fn [_] (d/future (capture-error d))))
                             (map deref)
                             doall)
        ex              (Exception.)]
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

(deftest test-timeout
  (testing "exception by default"
    (let [d (d/deferred)
          t (d/timeout! d 1)]
      (is (identical? d t))
      (is (thrown-with-msg? TimeoutException
                            #"^timed out after 1 milliseconds$"
                            (deref d 100 ::error)))))

  (testing "custom default value"
    (let [d (d/deferred)
          t (d/timeout! d 1 ::timeout)]
      (is (identical? d t))
      (is (deref (capture-success d ::timeout) 100 ::error))))

  (testing "error before timeout"
    (let [ex (Exception.)
          d (d/deferred)
          t (d/timeout! d 1000)]
      (d/error! d ex)
      (is (= ex (deref (capture-error t) 10 ::error))))))

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
  (is (= 2 (-> (clojure.core/future 1) d/->deferred (d/chain inc) deref)))
  (is (= 2 (-> (promise) (deliver 1) d/->deferred (d/chain inc) deref)))

  (let [f (CompletableFuture.)]
    (.obtrudeValue f 1)
    (is (= 2 (-> f d/->deferred (d/chain inc) deref))))

  (let [f (CompletableFuture.)]
    (.obtrudeException f (Exception.))
    (is (thrown? Exception (-> f d/->deferred deref)))))

(deftest test-finally
  (let [target-d (d/deferred)
        d        (d/deferred)
        fd       (-> d
                     (d/finally
                       (fn []
                         (d/success! target-d ::delivered)))
                     ;; to silence dropped error detection
                     (d/catch identity))]
    (d/error! d (Exception.))
    (is (= ::delivered (deref target-d 0 ::not-delivered)))))

(deftest test-alt
  (is (#{1 2 3} @(d/alt 1 2 3)))
  (let [d (d/deferred)
        a (d/alt d 2)]
    (d/success! d 1)
    (is (= 2 @a)))

  (let [d (d/deferred)
        a (d/alt d 2)]
    (doto d
      (d/error! (Exception. "boom 1"))
      ;; to silence dropped error detection
      (d/catch identity))
    (is (= 2 @a)))

  (let [e (d/error-deferred (Exception. "boom 2"))
        d (d/deferred)
        a (d/alt e d)]
    (d/success! d 1)
    (is (thrown-with-msg? Exception #"boom" @a)))

  (testing "uniformly distributed"
    (let [results (atom {})
          ;; within 10%
          n       1e4, r 10, eps (* n 0.15)
          f       #(/ (% n eps) r)]
      (dotimes [_ n]
        @(d/chain (apply d/alt (range r))
                  #(swap! results update % (fnil inc 0))))
      (doseq [[i times] @results]
        (is (<= (f -) times (f +)))))))

;;;

(deftest ^:ignore-dropped-errors ^:benchmark benchmark-chain
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

(deftest ^:ignore-dropped-errors ^:benchmark benchmark-deferred
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

(deftest ^:ignore-dropped-errors ^:stress test-error-leak-detection
  (testing "error-deferred always detects dropped errors"
    (expect-dropped-errors 1
      (d/error-deferred (Throwable.))))

  (testing "regular deferreds detect errors on every debug/*leak-aware-deferred-rate*'th instance (1024 by default)"
    (expect-dropped-errors 2
      ;; Explicitly restating the (current) default here for clarity
      (binding [debug/*leak-aware-deferred-rate* 1024]
        (dotimes [_ 2048]
          (d/error! (d/deferred) (Throwable.)))))))

(deftest ^:ignore-dropped-errors ^:stress test-deferred-chain
  (dotimes [_ 1e4]
    (let [d      (d/deferred)
          result (d/future
                   (last
                     (take 1e4
                           (iterate
                             #(let [d' (d/deferred)]
                                (d/connect % d')
                                d')
                             d))))]
      (Thread/sleep ^long (rand-int 10))
      (d/success! d 1)
      (is (= 1 @@result)))))

;; Promesa adds CompletionStage to the print-method hierarchy, which can cause
;; problems if neither is preferred over the other
(deftest promesa-print-method-test
  (testing "print-method hierarchy compatibility with promesa")
  (try
    (let [print-method-dispatch-vals (-> print-method methods keys set)]
      (is (= IDeferred
             (get print-method-dispatch-vals IDeferred ::missing)))
      (is (= ::missing
             (get print-method-dispatch-vals CompletionStage ::missing)))

      (let [d (d/deferred)]
        (is (instance? IDeferred d))
        (is (instance? CompletionStage d))

        (testing "no conflicts - CompletionStage not dispatchable"
          (pr-str d))

        (testing "no conflicts - preferred hierarchy established"
          (defmethod print-method CompletionStage [o ^java.io.Writer w]
            :noop)

          (pr-str d))))

    (finally
      (remove-method print-method CompletionStage))))

(instrument-tests-with-dropped-error-detection!)
