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
        vs (range 100)]

   #_(future
     (doseq [x vs]
       @(put! s x)))
   #_(is (= vs (repeatedly (count vs) #(deref (take! s)))))

   (future
     (doseq [x vs]
       @(put! s x)))
   (is (= vs (stream->lazy-seq s 100)))

    #_(doseq [x vs]
      (put! s x))
    #_(close! s)
    #_(is (= vs (stream->lazy-seq s)))))

(deftest test-streams
  #_(run-stream-test stream)
  (run-stream-test async/chan))

;;;

(defn blocking-queue-benchmark [^BlockingQueue q]
  (future
    (dotimes [i 1e6]
      (.put q i)))
  (loop []
    (let [x (.poll q 1 TimeUnit/MILLISECONDS)]
      (when x
        (recur)))))

(defn stream-benchmark [s]
  (future
    (dotimes [i 1e6]
      @(put! s i))
    (close! s))
  (loop []
    (let [x @(take! s)]
      (when x
        (recur)))))

(deftest ^:benchmark benchmark-alternatives
  (bench "blocking queue throughput w/ 1024 buffer"
    (blocking-queue-benchmark (ArrayBlockingQueue. 1024)))
  (bench "blocking queue throughput w/ 1 buffer"
    (blocking-queue-benchmark (ArrayBlockingQueue. 1)))
  (bench "blocking queue throughput w/ no buffer"
    (blocking-queue-benchmark (SynchronousQueue.))))

(deftest ^:benchmark benchmark-streams
  (bench "stream throughput w/ 1024 buffer"
    (stream-benchmark (stream 1024)))
  (bench "stream throughput w/ 1 buffer"
    (stream-benchmark (stream 1)))
  (bench "stream throughput w/ no buffer"
    (stream-benchmark (stream)))
  (let [s (stream 1)]
    (bench "put! then take!"
      (put! s 1)
      (take! s))))
