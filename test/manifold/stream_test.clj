(ns manifold.stream-test
  (:require
    [clojure.core.async :as async]
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.stream :as s]
    [manifold.deferred :as d])
  (:import
    [java.util.concurrent
     BlockingQueue
     ArrayBlockingQueue
     SynchronousQueue
     TimeUnit]))

(defn run-stream-test [gen]
  (let [s (s/->stream (gen))
        vs (range 10)]

    (future
      (doseq [x vs]
        (s/put! s x)))
    (is (= vs (repeatedly (count vs) #(deref (s/take! s)))))

    (future
      (doseq [x vs]
        (s/put! s x)))
    (is (= vs (s/stream->lazy-seq s 100)))

    (future
      (doseq [x vs]
        (s/put! s x))
      (s/close! s))
    (is (= vs (s/stream->lazy-seq s)))))

(deftest test-streams
  (run-stream-test s/stream)
  (run-stream-test #(async/chan 100))
  (run-stream-test #(ArrayBlockingQueue. 100))
  (run-stream-test #(let [s (s/stream)
                          s' (s/stream)]
                      (s/connect s s' nil)
                      (s/splice s s')))
  (run-stream-test #(let [s (s/->stream (ArrayBlockingQueue. 100))
                          s' (s/stream)]
                      (s/connect s s' nil)
                      (s/splice s s'))))


;;;

(deftest test-zip
  (let [inputs (partition 30 (range 90))]
    (is
      (= (apply map vector inputs)
        (->> inputs
          (map s/lazy-seq->stream)
          (apply s/zip)
          s/stream->lazy-seq)))))

(deftest test-operations
  (are [seq-f stream-f f input]

    (= (seq-f f input)
      (->> (s/lazy-seq->stream input)
        (stream-f f)
        s/stream->lazy-seq))

    map s/map inc (range 10)
    filter s/filter even? (range 10)))

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
      @(s/put! s i))
    (s/close! s))
  (loop []
    (let [x @(s/take! s)]
      (when x
        (recur)))))

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
