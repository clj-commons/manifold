(ns manifold.stream-test
  (:require
    [clojure.tools.logging :as log]
    [clojure.core.async :as async]
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold
     [stream :as s]
     [utils :as utils]
     [deferred :as d]
     [executor :as ex]])
  (:import
    [java.util.concurrent
     Executors
     BlockingQueue
     ArrayBlockingQueue
     SynchronousQueue
     TimeUnit]))

(defn run-sink-source-test [gen]
  (let [x (gen)
        sink (s/->sink x)
        source (s/->source x)
        vs (range 1e4)]

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

    (future
      (doseq [x vs]
        (try
          @(s/put! sink x)
          (catch Throwable e
            (log/error e "")))))
    (is (= vs (repeatedly (count vs) #(deref (s/take! source)))))

    (future
      (doseq [x vs]
        (try
          (s/put! sink x)
          (catch Throwable e
            (log/error e "")))))
    (is (= vs (repeatedly (count vs) #(deref (s/take! source)))))

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
          (s/put! sink x)
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
  (run-sink-source-test #(ArrayBlockingQueue. 10))
  (run-sink-source-test #(SynchronousQueue.))

  (run-sink-source-test (splice-into-stream s/stream))
  (run-sink-source-test (splice-into-stream #(s/stream 1 nil executor)))
  (run-sink-source-test (splice-into-stream #(ArrayBlockingQueue. 100)))
  (run-sink-source-test (splice-into-stream #(async/chan 100))))

(deftest test-sources
  (doseq [f [#(java.util.ArrayList. ^java.util.List %)
             #(.iterator ^java.util.List %)
             (utils/when-class java.util.stream.BasicStream
               (run-source-test #(-> % java.util.ArrayList. .stream)))]]
    (when f
      (= (range 100) (-> (range 100) f s/->source s/stream->seq))))

  )

;;;

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

    (is (= false  @(s/put! s 2)))
    (is (= true   (s/closed? s)))
    (is (= false  (s/drained? s)))
    (is (= 1      @(s/take! s)))
    (is (= nil    @(s/take! s)))
    (is (= true   (s/drained? s)))))

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

    (map inc) (range 10)

    (map inc) (vec (range 10))

    (comp (map inc) (filter even?)) (range 10)

    (comp (map inc) (take 5)) (range 10)))

(deftest test-reduce
  (let [inputs (range 1e2)]
    (is
      (= (reduce + inputs)
        @(s/reduce + (s/->source inputs))))
    (is
      (= (reduce + 1 inputs)
        @(s/reduce + 1 (s/->source inputs))))))

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
        f #(long (/ % 10))]
    (is
      (= (partition-by f inputs)
        (->> inputs
          s/->source
          (s/lazily-partition-by f)
          s/stream->seq
          (map (comp doall s/stream->seq)))))))

(deftest test-concat
  (let [inputs (range 1e2)
        f #(long (/ % 10))]
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
      (is (realized? a))
      (is (= true @a))
      (is (not (realized? b)))
      (is (= 9 @(s/take! s)))
      (is (realized? b))
      (is (= true @b))
      (let [c (s/put! s 12)
            d (s/put! s 1)]
        (is (not (or (realized? c) (realized? d))))
        (is (= 2 @(s/take! s)))
        (is (not (or (realized? c) (realized? d))))
        (is (= 12 @(s/take! s)))
        (is (realized? d))
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
      (let [src (s/stream)
            f #(->> src
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
        f (fn [idx]
            (swap! cnt inc)
            (future
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

(deftest test-error-handling

  (binding [log/*logger-factory* clojure.tools.logging.impl/disabled-logger-factory]

    (let [s (s/stream)
          s' (s/map #(/ 1 %) s)]
      (is (not (s/closed? s)))
      (is (not (s/drained? s')))
      (is (= false @(s/put-all! s [0 1])))
      (is (s/closed? s))
      (is (s/drained? s')))

    (let [s (s/stream)
          s' (s/map #(d/future (/ 1 %)) s)]
      (is (not (s/closed? s)))
      (is (not (s/drained? s')))
      (is (= true @(s/put! s 0)))
      (is (not (s/closed? s)))
      (is (not (s/drained? s'))))

    (let [s (s/stream)
          s' (->> s
               (s/map #(d/future (/ 1 %)))
               s/realize-each)]
      (is (not (s/closed? s)))
      (is (not (s/drained? s')))
      (s/put-all! s (range 10))
      (is (nil? @(s/take! s')))
      (is (s/drained? s')))))

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
