(ns manifold.stream.async
  (:require
    [manifold.deferred :as d]
    [clojure.core.async :as a]
    [manifold.stream :as s]
    [manifold.utils :as utils])
  (:import
    [java.util.concurrent.atomic
     AtomicReference
     AtomicInteger]
    [java.util.concurrent
     BlockingQueue
     LinkedBlockingQueue]
    [manifold.stream
     IEventSink
     IEventSource
     IStream]))

(deftype CoreAsyncStream
  [ch
   ^:volatile-mutable closed?
   ^BlockingQueue close-callbacks
   ^BlockingQueue drained-callbacks
   ^AtomicInteger pending-puts
   ^AtomicReference last-put
   ^AtomicReference last-take]

  IStream
  (isSynchronous [_] false)
  (isClosed [_]
    closed?)

  IEventSink
  (close [this]
    (locking this
      (if-not closed?
        (do
          (set! closed? true)
          (utils/invoke-callbacks close-callbacks)
          (when (zero? (.get pending-puts))
            (a/close! ch))
          true)
        false)))

  (onClosed [this f]
    (locking this
      (if closed?
        (f)
        (.add close-callbacks f))))

  (put [this x blocking?]

    (assert (not (nil? x)) "core.async channel cannot take `nil` as a message")

    (cond
      closed?
      (if blocking?
        false
        (d/success-deferred false))

      blocking?
      (try
        (.incrementAndGet pending-puts)
        (a/>!! ch x)
        true
        (finally
          (when (zero? (.decrementAndGet pending-puts))
            (locking this
              (when closed?
                (a/close! ch))))))

      :else
      (let [d  (d/deferred)
            d' (.getAndSet last-put d)
            f  (fn [_]
                 (a/go
                   (try
                     (a/>! ch x)
                     (utils/without-overflow
                       (d/success! d true))
                     (finally
                       (when (zero? (.decrementAndGet pending-puts))
                         (locking this
                           (when (s/closed? this)
                             (a/close! ch))))))))]
        (.incrementAndGet pending-puts)
        (if (realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (put [this x blocking? timeout timeout-val]

    (if (nil? timeout)
      (.put this x blocking?)
      (assert (not (nil? x)) "core.async channel cannot take `nil` as a message"))

    (if closed?

      (if blocking?
        false
        (d/success-deferred false))

      (let [d  (d/deferred)
            d' (.getAndSet last-put d)
            f  (fn [_]
                 (a/go
                   (try
                     (let [result (a/alt!
                                    [ch x] true
                                    (a/timeout timeout) timeout-val
                                    :priority true)]
                       (utils/without-overflow
                         (d/success! d result)))
                     (finally
                       (when (zero? (.decrementAndGet pending-puts))
                         (locking this
                           (when (s/closed? this)
                             (a/close! ch))))))))]
        (.incrementAndGet pending-puts)
        (if (realized? d')
          (f nil)
          (d/on-realized d' f f))
        (if blocking?
          @d
          d))))

  IEventSource
  (take [this blocking? default-val]
    (if blocking?

      (let [x (a/<!! ch)]
        (if (nil? x)
          (do
            (s/close! this)
            default-val)
          x))

      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (a/go
                   (let [x (a/<! ch)]
                     (utils/without-overflow
                       (d/success! d
                         (if (nil? x)
                           (do
                             (s/close! this)
                             (utils/invoke-callbacks drained-callbacks)
                             default-val)
                           x))))))]
        (if (realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (take [this blocking? default-val timeout timeout-val]
    (let [d  (d/deferred)
          d' (.getAndSet last-take d)
          f  (fn [_]
               (a/go
                 (let [result (a/alt!
                                ch ([x] (if (nil? x)
                                          (do
                                            (s/close! this)
                                            (utils/invoke-callbacks drained-callbacks)
                                            default-val)
                                          x))
                                (a/timeout timeout) timeout-val
                                :priority true)]
                   (utils/without-overflow
                     (d/success! d result)))))]
      (if (realized? d')
        (f nil)
        (d/on-realized d' f f))
      (if blocking?
        @d
        d)))

  (setBackpressure [this enabled?])

  (connect [this sink options]))

(extend-protocol s/Streamable

  clojure.core.async.impl.channels.ManyToManyChannel
  (to-stream [ch]
    (CoreAsyncStream.
      ch
      false
      (LinkedBlockingQueue.)
      (LinkedBlockingQueue.)
      (AtomicInteger. 0)
      (AtomicReference. (d/success-deferred true))
      (AtomicReference. (d/success-deferred true)))))
