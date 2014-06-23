(ns manifold.stream.async
  (:require
    [manifold.deferred :as d]
    [clojure.core.async :as a]
    [manifold.stream.graph :as g]
    [manifold.stream :as s]
    [manifold.utils :as utils])
  (:import
    [java.util.concurrent.atomic
     AtomicReference
     AtomicBoolean
     AtomicInteger]
    [java.lang.ref
     WeakReference]
    [java.util.concurrent
     BlockingQueue
     LinkedBlockingQueue]
    [java.util.concurrent.locks
     Lock]
    [manifold.stream
     IEventSink
     IEventSource
     IStream]))

(deftype CoreAsyncSource
  [ch
   ^AtomicBoolean drained?
   ^BlockingQueue drained-callbacks
   ^AtomicReference last-take
   ^:volatile-mutable weak-handle
   ^Lock lock]

  IStream

  (isSynchronous [_] false)

  (description [_]
    {:type "core.async"})

  (weakHandle [this reference-queue]
    (utils/with-lock lock
      (or weak-handle
        (do
          (set! weak-handle (WeakReference. this reference-queue))
          weak-handle))))

  (close [_]
    nil)

  (downstream [this]
    (g/downstream this))

  IEventSource

  (isDrained [this]
    (.get drained?))

  (onDrained [this callback]
    (utils/with-lock lock
      (if (.get drained?)
        (callback)
        (.add drained-callbacks callback))))

  (take [this blocking? default-val]
    (if blocking?

      (let [x (a/<!! ch)]
        (if (nil? x)
          (utils/with-lock lock
            (.set drained? true)
            (utils/invoke-callbacks drained-callbacks)
            default-val)
          x))

      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (a/go
                   (let [x (a/<! ch)]
                     (d/success! d
                       (if (nil? x)
                         (utils/with-lock lock
                           (.set drained? true)
                           (utils/invoke-callbacks drained-callbacks)
                           default-val)
                         x)))))]
        (if (d/realized? d')
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
                                          (utils/with-lock lock
                                            (.set drained? true)
                                            (utils/invoke-callbacks drained-callbacks)
                                            default-val)
                                          x))
                                (a/timeout timeout) timeout-val
                                :priority true)]
                   (utils/without-overflow
                     (d/success! d result)))))]
      (if (d/realized? d')
        (f nil)
        (d/on-realized d' f f))
      (if blocking?
        @d
        d)))

  (connector [this sink]
    nil))

(deftype CoreAsyncSink
  [ch
   ^AtomicBoolean closed?
   ^BlockingQueue close-callbacks
   ^AtomicReference last-put
   ^Lock lock]

  IStream
  (isSynchronous [_] false)

  (description [_]
    {:type "core.async"})

  (downstream [this]
    nil)

  (close [this]
    (utils/with-lock lock
      (if (.get closed?)
        false
        (do
          (.set closed? true)
          (utils/invoke-callbacks close-callbacks)
          (let [d (.get last-put)
                f (fn [_] (a/close! ch))]
            (d/on-realized d
              (fn [_] (a/close! ch))
              nil)
            true)))))

  (weakHandle [_ _]
    nil)

  IEventSink

  (isClosed [_]
    (.get closed?))

  (onClosed [this f]
    (utils/with-lock lock
      (if (.get closed?)
        (f)
        (.add close-callbacks f))))

  (put [this x blocking?]

    (assert (not (nil? x)) "core.async channel cannot take `nil` as a message")

    (utils/with-lock lock
      (cond
        (.get closed?)
        (if blocking?
          false
          (d/success-deferred false))

        blocking?
        (try
          (a/>!! ch x)
          true)

        :else
        (let [d  (d/deferred)
              d' (.getAndSet last-put d)
              f  (fn [_]
                   (a/go
                     (a/>! ch x)
                     (d/success! d true)))]
          (if (d/realized? d')
            (f nil)
            (d/on-realized d' f f))
          d))))

  (put [this x blocking? timeout timeout-val]

    (if (nil? timeout)
      (.put this x blocking?)
      (assert (not (nil? x)) "core.async channel cannot take `nil` as a message"))

    (utils/with-lock lock

      (if (.get closed?)

        (if blocking?
          false
          (d/success-deferred false))

        (let [d  (d/deferred)
              d' (.getAndSet last-put d)
              f  (fn [_]
                   (a/go
                     (let [result (a/alt!
                                    [ch x] true
                                    (a/timeout timeout) timeout-val
                                    :priority true)]
                       (d/success! d result))))]
          (if (d/realized? d')
            (f nil)
            (d/on-realized d' f f))
          (if blocking?
            @d
            d))))))

(extend-protocol s/Sinkable

  clojure.core.async.impl.channels.ManyToManyChannel
  (to-sink [ch]
    (CoreAsyncSink.
      ch
      (AtomicBoolean. false)
      (LinkedBlockingQueue.)
      (AtomicReference. (d/success-deferred true))
      (utils/mutex))))

(extend-protocol s/Sourceable

  clojure.core.async.impl.channels.ManyToManyChannel
  (to-source [ch]
    (CoreAsyncSource.
      ch
      (AtomicBoolean. false)
      (LinkedBlockingQueue.)
      (AtomicReference. (d/success-deferred true))
      nil
      (utils/mutex))))
