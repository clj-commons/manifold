(ns manifold.stream-test
  (:require
    [clojure.tools.logging :as log]
    [clojure.core.async :as async]
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.stream :as s]
    [manifold.stream.default :as sd]
    [manifold.utils :as utils]
    [manifold.deferred :as d]
    [manifold.executor :as ex])
  (:import
    [java.util.concurrent
     Executors
     BlockingQueue
     ArrayBlockingQueue
     SynchronousQueue
     TimeUnit]))

(defn run-sink-source-test [gen]
  (let [x      (gen)
        sink   (s/->sink x)
        source (s/->source x)
        vs     (range 1e4)]

    (reset-meta! sink nil)
    (is (= nil (meta sink)))
    (reset-meta! sink {1 2})
    (alter-meta! sink assoc 3 4)
    (is (= {1 2 3 4} (meta sink)))

    (reset-meta! source nil)
    (is (= nil (meta source)))
    (reset-meta! source {1 2})
    (alter-meta! source assoc 3 4)
    (is (= {1 2 3 4} (meta source)))

    (is (thrown? ClassCastException (with-meta source {})))
    (is (thrown? ClassCastException (with-meta sink {})))

    (future
      (doseq [x vs]
        (try
          @(s/put! sink x)
          (catch Throwable e
            (log/error e "")))))
    (is (= vs (repeatedly (count vs) #(deref (s/take! source)))))

    #_(future
        (doseq [x vs]
          (try
            (s/put! sink x)
            (catch Throwable e
              (log/error e "")))))
    #_(is (= vs (repeatedly (count vs) #(deref (s/take! source)))))

    (future
      (doseq [x vs]
        (try
          @(s/try-put! sink x 100 ::timeout)
          (catch Throwable e
            (log/error e "")))))
    (is (= vs (repeatedly (count vs) #(deref (s/take! source)))))

    (future
      (doseq [x vs]
        (try
          @(s/put! sink x)
          (catch Throwable e
            (log/error e "")))))
    (is (= vs (s/stream->seq source 100)))))

(defn splice-into-stream [gen]
  #(let [x (gen)
         s (s/stream)]
     (s/connect x s nil)
     (s/splice x s)))

(def executor (ex/fixed-thread-executor 8))

(deftest test-streams

  (run-sink-source-test s/stream)
  (run-sink-source-test #(s/stream 1 nil executor))
  (run-sink-source-test #(async/chan 100))
  (run-sink-source-test #(ArrayBlockingQueue. 100))
  (run-sink-source-test #(SynchronousQueue.))

  (run-sink-source-test (splice-into-stream s/stream))
  (run-sink-source-test (splice-into-stream #(s/stream 1 nil executor)))
  (run-sink-source-test (splice-into-stream #(ArrayBlockingQueue. 100)))
  (run-sink-source-test (splice-into-stream #(async/chan 100))))

(deftest test-sources
  (doseq [f [#(java.util.ArrayList. ^java.util.List %)
             #(.iterator ^java.util.List %)
             #(-> % (java.util.ArrayList.) .stream)]]
    (when f
      (= (range 100) (-> (range 100) f s/->source s/stream->seq)))
    (when f
      (= (range 100) (-> (range 100) f s/->source (s/stream->seq 10))))))

;;;

(deftest test-pending-takes-and-puts-cleaned-up
  (let [timeout     1
        default-val ::default
        timeout-val ::timeout]
    (testing "take one more than the max number of allowed pending takes"
      (let [pending-s (sd/stream)]
        (dotimes [_ sd/max-consumers]
          (s/try-take! pending-s default-val timeout timeout-val))
        (is (= timeout-val @(s/try-take! pending-s default-val timeout timeout-val))
            "Should timeout and deliver timeout-val instead of failing and returning default-val")))
    (testing "put one more than the max number of allowed pending puts"
      (let [pending-s (sd/stream)]
        (dotimes [_ sd/max-producers]
          (s/try-put! pending-s ::x timeout timeout-val))
        (is (= timeout-val @(s/try-put! pending-s ::x timeout timeout-val))
            "Should timeout and deliver timeout-val")))))

(deftest test-deliver-pending-takes-on-close
  (let [input-s  (s/stream)
        result-s (s/stream)
        end-s    (s/stream)]
    (dotimes [n 5]
      (doto (Thread.
              (fn []
                (loop []
                  (when-let [x @(s/take! input-s)]
                    (s/put! result-s "result")
                    (recur)))
                (s/put! end-s "end")))
        (.start)))

    (is (= false (s/closed? input-s)))
    (is (= false (s/closed? result-s)))
    (is (= false (s/closed? end-s)))

    (dotimes [n 10] (s/put! input-s "input"))

    (is (= (repeat 10 "result") (take 10 (s/stream->seq result-s 1000))))

    (s/close! input-s)

    (is (= true (s/closed? input-s)))

    (is (= (repeat 5 "end") (take 5 (s/stream->seq end-s 1000))))

    (s/close! result-s)
    (s/close! end-s)

    (is (= true (s/drained? input-s)))
    (is (= true (s/drained? result-s)))
    (is (= true (s/drained? end-s)))))

(deftest test-closed-and-drained
  (let [s (s/stream)]
    (s/put! s 1)
    (is (= false (s/closed? s)))

    (s/close! s)

    (is (= false @(s/put! s 2)))
    (is (= true (s/closed? s)))
    (is (= false (s/drained? s)))
    (is (= 1 @(s/take! s)))
    (is (= nil @(s/take! s)))
    (is (= true (s/drained? s)))))

(deftest test-transducers
  (let [s (s/stream 0
                    (comp
                      (map inc)
                      (filter even?)
                      (take 3)))]
    (s/put-all! s (range 10))
    (is (= [2 4 6] (s/stream->seq s))))

  (are [xform input]
    (= (s/stream->seq (s/transform xform (s/->source input)))
       (transduce xform conj [] input))

    (mapcat #(repeat 3 %)) (range 10)

    (map inc) (range 10)

    (map inc) (vec (range 10))

    (comp (map inc) (filter even?)) (range 10)

    (comp (map inc) (take 5)) (range 10)

    (partition-all 5) (range 12)

    (comp (partition-all 5) (map count)) (range 13)
    ))

(deftest test-accumulating-transducer-with-multiple-consumers

  ;; This tests a very particular code path while closing
  ;; streams with a transducer and multiple consumers.

  ;; When closing a transformed stream with multiple consumers
  ;; and an accumulated transducer state, one consumer must
  ;; receive the last message.  The last message should not be
  ;; discarded, and the consumers should not be abandoned.

  ;; The consumers need to start listening before messages
  ;; are available, and there should be more than one
  ;; consumer remaining at the time the stream is closed.

  (let [s (s/stream 0 (partition-all 3))

        d (-> (d/zip (s/take! s :drained)
                     (s/take! s :drained)
                     (s/take! s :drained))

              (d/chain (partial into #{})))]

    (-> (s/put-all! s (range 5))
        (d/finally (partial s/close! s)))

    (is (= #{[0 1 2] [3 4] :drained}
           (deref d 100 :incomplete!)))))


(deftest test-reduce
  (let [inputs (range 1e2)]
    (is
      (= (reduce + inputs)
         @(s/reduce + (s/->source inputs))))
    (is
      (= (reduce + 1 inputs)
         @(s/reduce + 1 (s/->source inputs)))))

  (let [inputs (range 10)
        accf   (fn [acc el]
                 (if (= el 5) (reduced :large) el))
        s      (s/->source inputs)]
    (is (= :large
           (reduce accf 0 inputs)
           @(s/reduce accf 0 s)))
    (is (not (s/drained? s)))
    (is (= 6 @(s/try-take! s 1)))))

(deftest test-zip
  (let [inputs (partition-all 1e4 (range 3e4))]
    (is
      (= (apply map vector inputs)
         (->> inputs
              (map s/->source)
              (apply s/zip)
              s/stream->seq)))))

(deftest test-lazily-partition-by
  (let [inputs (range 1e2)
        f      #(long (/ % 10))]
    (is
      (= (partition-by f inputs)
         (->> inputs
              s/->source
              (s/lazily-partition-by f)
              s/stream->seq
              (map (comp doall s/stream->seq)))))))

(defn test-batch [metric max-size]
  (let [inputs  (repeat 10 metric)
        outputs (partition-all (quot max-size metric) inputs)]
    (is
      (= outputs
         (->> inputs
              s/->source
              (s/batch identity max-size 1e4)
              s/stream->seq)))))

(deftest test-batch-default
  (test-batch 1 5))

(deftest test-batch-with-metric
  (test-batch 2 5))

(deftest test-batch-with-oversized-message
  (let [inputs  [1 1 9]
        outputs [[1 1] [9]]]
    (is
      (= outputs
         (->> inputs
              s/->source
              (s/batch identity 2 1e4)
              s/stream->seq)))))

(deftest test-concat
  (let [inputs (range 1e2)
        f      #(long (/ % 10))]
    (is
      (= inputs
         (->> inputs
              s/->source
              (s/lazily-partition-by f)
              s/concat
              s/stream->seq)))))

(deftest test-buffer
  (let [s (s/buffered-stream identity 10)]

    (let [a (s/put! s 9)
          b (s/put! s 2)]
      (is (d/realized? a))
      (is (= true @a))
      (is (not (d/realized? b)))
      (is (= 9 @(s/take! s)))
      (is (d/realized? b))
      (is (= true @b))
      (let [c (s/put! s 12)
            d (s/put! s 1)]
        (is (not (or (d/realized? c) (d/realized? d))))
        (is (= 2 @(s/take! s)))
        (is (not (or (d/realized? c) (d/realized? d))))
        (is (= 12 @(s/take! s)))
        (is (d/realized? d))
        (is (= true @d))
        (is (= 1 @(s/take! s)))))))

(deftest test-operations
  (are [seq-f stream-f f input]

    (apply =

           ;; seq version
           (seq-f f input)

           ;; single operation
           (->> (s/->source input)
                (stream-f f)
                s/stream->seq)

           ;; three simultaneous operations
           (let [src  (s/stream)
                 f    #(->> src
                            (stream-f f)
                            (s/buffer (count input)))
                 dsts (doall (repeatedly 3 f))]
             (d/chain (s/put-all! src input)
                      (fn [_] (s/close! src)))
             (map s/stream->seq dsts)))

    map s/map inc (range 10)

    filter s/filter even? (range 10)

    mapcat s/mapcat list (range 10)

    reductions s/reductions + (range 10)

    #(reductions %1 1 %2) #(s/reductions %1 1 %2) + (range 10)))

(defn dechunk [s]
  (lazy-seq
    (when-let [[x] (seq s)]
      (cons x (dechunk (rest s))))))

(deftest test-cleanup
  (let [cnt (atom 0)
        f   (fn [idx]
              (swap! cnt inc)
              (d/future
                (range (* idx 10) (+ 10 (* idx 10)))))]
    (is (= (range 10)
           (->> (range)
                dechunk
                (map f)
                s/->source
                s/realize-each
                (s/map s/->source)
                s/concat
                (s/transform (take 10))
                s/stream->seq)))
    #_(is (= 1 @cnt))))

(deftest test-drain-into
  (let [n      100
        src    (s/->source (range n))
        dst    (s/stream)
        result (s/drain-into src dst)]
    (is (= (range n) (->> dst s/stream->seq (take n))))
    (is (= true @result))))

(deftest test-consume
  (let [src    (s/->source [1 2 3])
        values (atom [])
        result (-> (s/consume #(swap! values conj %) src)
                   (d/chain #(do (swap! values conj ::done) %)))]
    (is (= true @result))
    (is (= [1 2 3 ::done] @values))))

(deftest test-consume-async
  (let [src    (s/->source [1 2])
        values (atom [])
        result (s/consume-async #(do (swap! values conj %)
                                     (d/success-deferred (= (count @values) 1)))
                                src)]
    (is (true? (deref result 100 ::not-done)))
    (is (= [1 2] @values))))

(deftest test-periodically
  (testing "produces with delay"
    (let [s (s/periodically 20 0 (constantly 1))]
      (Thread/sleep 30)
      (s/close! s)
      ;; will produces 2 items here no matter the sleep amount
      ;; as the periodically stream has a buffer of 1
      (is (= [1 1] (s/stream->seq s)))))

  (testing "doesn't fail on nil"
    (let [s (s/periodically 100 0 (constantly nil))]
      (Thread/sleep 150)
      (s/close! s)
      (is (= [nil nil] (s/stream->seq s))))))

(deftest test-try-put
  (testing "times out"
    (let [s          (s/stream)
          put-result (s/try-put! s :value 10 ::timeout)]
      (is (= ::timeout (deref put-result 15 ::wrong))))))

(deftest test-error-handling

  (binding [log/*logger-factory* clojure.tools.logging.impl/disabled-logger-factory]

    (let [s  (s/stream)
          s' (s/map #(/ 1 %) s)]
      (is (not (s/closed? s)))
      (is (not (s/drained? s')))
      (is (= false @(s/put-all! s [0 1])))
      (is (s/closed? s))
      (is (s/drained? s')))

    (let [s  (s/stream)
          s' (s/map #(d/future (/ 1 %)) s)]
      (is (not (s/closed? s)))
      (is (not (s/drained? s')))
      (is (= true @(s/put! s 0)))
      (is (not (s/closed? s)))
      (is (not (s/drained? s'))))

    (let [s  (s/stream)
          s' (->> s
                  (s/map #(d/future (/ 1 %)))
                  s/realize-each)]
      (is (not (s/closed? s)))
      (is (not (s/drained? s')))
      (s/put-all! s (range 10))
      (is (nil? @(s/take! s')))
      (is (s/drained? s')))))

(deftest test-connect-timeout
  (let [src  (s/stream)
        sink (s/stream)]

    (s/connect src sink {:timeout 10})
    (s/put-all! src (range 10))
    (Thread/sleep 100)

    (is (s/closed? sink))
    (is (s/closed? src))))

(deftest test-window-streams
  (testing "dropping-stream"
    (let [s          (s/->source (range 11))
          dropping-s (s/dropping-stream 10 s)]
      (is (= (range 10)
             (s/stream->seq dropping-s)))))

  (testing "sliding-stream"
    (let [in         (s/stream)
          sliding-s (s/sliding-stream 3 in)]
      (testing "passthrough within buffer size"
        @(s/put! in 1)
        (is (= 1 @(s/try-take! sliding-s 0))))
      (testing "discards oldest elements when blocked"
        @(s/put-all! in [1 2 3 4])
        (is (= 2 @(s/try-take! sliding-s 0)))
        (is (= 3 @(s/try-take! sliding-s 0)))
        (is (= 4 @(s/try-take! sliding-s 0))))
      (testing "propagates closes"
        (s/close! in)
        (is (= ::closed @(s/take! sliding-s ::closed)))))))
;;;

(deftest ^:stress stress-buffered-stream
  (let [s (s/buffered-stream identity 100)]
    (future
      (dotimes [_ 1e6]
        @(s/put! s (rand-int 200)))
      (s/close! s))
    (-> s s/stream->seq dorun)))

;;;

(defn blocking-queue-benchmark [^BlockingQueue q]
  (future
    (dotimes [i 1e3]
      (.put q i)))
  (dotimes [i 1e3]
    (.take q)))

(defn core-async-benchmark [ch]
  (async/go
    (dotimes [i 1e3]
      (async/>! ch i)))
  (dotimes [i 1e3]
    (async/<!! ch)))

(defn core-async-blocking-benchmark [ch]
  (future
    (dotimes [i 1e3]
      (async/>!! ch i)))
  (dotimes [i 1e3]
    (async/<!! ch)))

(defn stream-benchmark [s]
  (future
    (dotimes [i 1e3]
      @(s/put! s i)))
  (dotimes [_ 1e3]
    @(s/take! s)))

(deftest ^:benchmark benchmark-conveyance
  (let [s  (s/stream)
        s' (reduce
             (fn [s _]
               (let [s' (s/stream)]
                 (s/connect s s')
                 s'))
             s
             (range 1e3))]
    (bench "convey messages through 1e3 stream chain"
      (s/put! s 1)
      @(s/take! s')))
  (let [c  (async/chan)
        c' (reduce
             (fn [c _]
               (let [c' (async/chan)]
                 (async/pipe c c')
                 c'))
             c
             (range 1e3))]
    (bench "convey messages through 1e3 core.async channel chain"
      (async/go (async/>! c 1))
      (async/<!! c'))))

(deftest ^:benchmark benchmark-map
  (let [s  (s/stream)
        s' (reduce
             (fn [s _] (s/map inc s))
             s
             (range 1e3))]
    (bench "map messages through 1e3 stream chain"
      (s/put! s 1)
      @(s/take! s')))
  (let [c  (async/chan)
        c' (reduce
             (fn [c _] (async/map< inc c))
             c
             (range 1e3))]
    (bench "map  messages through 1e3 core.async channel chain"
      (async/go (async/>! c 1))
      (async/<!! c'))))

(deftest ^:benchmark benchmark-alternatives
  (let [q (ArrayBlockingQueue. 1024)]
    (bench "blocking queue throughput w/ 1024 buffer"
      (blocking-queue-benchmark q)))
  (let [q (ArrayBlockingQueue. 1)]
    (bench "blocking queue throughput w/ 1 buffer"
      (blocking-queue-benchmark q)))
  (let [q (SynchronousQueue.)]
    (bench "blocking queue throughput w/ no buffer"
      (blocking-queue-benchmark q)))
  (let [ch (async/chan 1024)]
    (bench "core.async channel throughput w/ 1024 buffer"
      (core-async-benchmark ch))
    (bench "core.async blocking channel throughput w/ 1024 buffer"
      (core-async-blocking-benchmark ch)))
  (let [ch (async/chan 1)]
    (bench "core.async put then take"
      (async/>!! ch 1)
      (async/<!! ch))
    (bench "core.async channel throughput w/ 1 buffer"
      (core-async-benchmark ch))
    (bench "core.async blocking channel throughput w/ 1 buffer"
      (core-async-blocking-benchmark ch)))
  (let [ch (async/chan)]
    (bench "core.async channel throughput w/ no buffer"
      (core-async-benchmark ch))
    (bench "core.async blocking channel throughput w/ no buffer"
      (core-async-blocking-benchmark ch))))

(deftest ^:benchmark benchmark-streams
  (let [s (s/stream 1024)]
    (bench "stream throughput w/ 1024 buffer"
      (stream-benchmark s)))
  (let [s (s/stream 1)]
    (bench "stream throughput w/ 1 buffer"
      (stream-benchmark s)))
  (let [s (s/stream)]
    (bench "stream throughput w/ no buffer"
      (stream-benchmark s)))
  (let [s (s/stream 1)]
    (bench "put! then take!"
      (s/put! s 1)
      (s/take! s)))
  (let [s (s/stream 1)]
    (bench "take! then put!"
      (s/take! s)
      (s/put! s 1)))
  (let [s (s/stream 1)]
    (s/consume (fn [_]) s)
    (bench "put! with consume"
      (s/put! s 1))))
