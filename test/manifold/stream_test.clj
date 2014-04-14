(ns manifold.stream-test
  (:require
    [clojure.core.async :as async]
    [clojure.test :refer :all]
    [manifold.test-utils :refer :all]
    [manifold.stream :refer :all])
  (:import
    [java.util.concurrent
     BlockingQueue
     ArrayBlockingQueue
     SynchronousQueue
     TimeUnit]))

(defn run-stream-test [gen]
  (let [s (->stream (gen))
        vs (range 2e3)]

   (future
     (doseq [x vs]
       (put! s x)))
   (is (= vs (repeatedly (count vs) #(deref (take! s)))))

   (future
     (doseq [x vs]
       (put! s x)))
   (is (= vs (stream->lazy-seq s 100)))

   (future
     (doseq [x vs]
       (put! s x))
     (close! s))
   (is (= vs (stream->lazy-seq s)))))

(deftest test-streams
  (run-stream-test stream)
  (run-stream-test #(async/chan 100))
  (run-stream-test #(ArrayBlockingQueue. 100)))

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

(defn stream-benchmark [s]
  (future
    (dotimes [i 1e3]
      @(put! s i))
    (close! s))
  (loop []
    (let [x @(take! s)]
      (when x
        (recur)))))

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
      (core-async-benchmark ch)))
  (let [ch (async/chan 1)]
    (bench "core.async channel throughput w/ 1 buffer"
      (core-async-benchmark ch)))
  (let [ch (async/chan)]
    (bench "core.async channel throughput w/ no buffer"
      (core-async-benchmark ch))))

(deftest ^:benchmark benchmark-streams
  (let [s (stream 1024)]
    (bench "stream throughput w/ 1024 buffer"
      (stream-benchmark s)))
  (let [s (stream 1)]
    (bench "stream throughput w/ 1 buffer"
      (stream-benchmark s)))
  (let [s (stream)]
    (bench "stream throughput w/ no buffer"
      (stream-benchmark s)))
  (let [s (stream 1)]
    (bench "put! then take!"
      (put! s 1)
      (take! s))))
