(ns manifold.stream.queue
  (:require
    [manifold.stream.graph :as g]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [manifold.utils :as utils])
  (:import
    [java.lang.ref
     WeakReference]
    [java.util.concurrent.locks
     Lock]
    [java.util.concurrent.atomic
     AtomicReference
     AtomicInteger
     AtomicBoolean]
    [java.util.concurrent
     BlockingQueue
     LinkedBlockingQueue
     TimeUnit]
    [manifold.stream
     IEventSink
     IEventSource
     IStream]))

(deftype BlockingQueueSource
  [^BlockingQueue queue
   ^AtomicReference last-take
   lock
   ^:volatile-mutable weak-handle]

  IStream
  (isSynchronous [_]
    true)

  (description [_]
    {:type (.getCanonicalName (class queue))
     :buffer-size (.size queue)})

  (close [_]
    nil)

  (weakHandle [this reference-queue]
    (utils/with-lock lock
      (or weak-handle
        (do
          (set! weak-handle (WeakReference. this reference-queue))
          weak-handle))))

  (downstream [this]
    (g/downstream this))

  IEventSource
  (onDrained [this f]
    )

  (isDrained [_]
    false)

  (take [this blocking? default-val]
    (if blocking?

      (.take queue)

      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (let [x (.poll queue)]
                   (if (nil? x)

                     (utils/wait-for
                       (d/success! d (.take queue)))

                     (d/success! d x))))]
        (if (d/realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (take [this blocking? default-val timeout timeout-val]
    (if blocking?

      (let [x (.poll queue timeout TimeUnit/MILLISECONDS)]
        (if (nil? x)
          timeout-val
          x))

      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (let [x (.poll queue)]
                   (if (nil? x)

                     (utils/wait-for
                       (d/success! d
                         (let [x (.poll queue timeout TimeUnit/MILLISECONDS)]
                           (if (nil? x)
                             timeout-val
                             x))))

                     (d/success! d x))))]
        (if (d/realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (connector [this sink]
    nil))

(deftype BlockingQueueSink
  [^BlockingQueue queue
   ^BlockingQueue closed-callbacks
   ^AtomicBoolean closed?
   ^AtomicReference last-put
   ^Lock lock]

  IStream
  (isSynchronous [_]
    true)

  (description [_]
    (let [size (.size queue)]
      {:type (.getCanonicalName (class queue))
       :buffer-capacity (+ (.remainingCapacity queue) size)
       :buffer-size size}))

  (downstream [this]
    nil)

  (weakHandle [_ _]
    nil)

  (close [this]
    (utils/with-lock lock
      (if (.get closed?)
        false
        (do
          (.set closed? true)
          (utils/invoke-callbacks closed-callbacks)
          true))))

  IEventSink

  (onClosed [this f]
    (utils/with-lock lock
      (if (.get closed?)
        (f)
        (.add closed-callbacks f))))

  (isClosed [_]
    (.get closed?))

  (put [this x blocking?]

    (assert (not (nil? x)) "BlockingQueue cannot take `nil` as a message")

    (if blocking?

      (do
        (.put queue x)
        true)

      (let [d  (d/deferred)
            d' (.getAndSet last-put d)
            f  (fn [_]
                 (utils/with-lock lock
                   (try
                     (or
                       (and (.get closed?)
                         (d/success! d false))

                       (and (.offer queue x)
                         (d/success! d true))

                       (utils/wait-for
                         (d/success! d
                           (do
                             (.put queue x)
                             true)))))))]
        (if (d/realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (put [this x blocking? timeout timeout-val]

    (assert (not (nil? x)) "BlockingQueue cannot take `nil` as a message")

    (if blocking?

      (.offer queue x timeout TimeUnit/MILLISECONDS)

      (let [d  (d/deferred)
            d' (.getAndSet last-put d)
            f  (fn [_]
                 (utils/with-lock lock
                   (try
                     (or
                       (and (.get closed?)
                         (d/success! d false))

                       (and (.offer queue x)
                         (d/success! d true))

                       (utils/wait-for
                         (d/success! d
                           (if (.offer queue x timeout TimeUnit/MILLISECONDS)
                             true
                             false)))))))]
        (if (d/realized? d')
          (f nil)
          (d/on-realized d' f f))
        d))))

;;;

(extend-protocol s/Sinkable

  BlockingQueue
  (to-sink [queue]
    (BlockingQueueSink.
      queue
      (LinkedBlockingQueue.)
      (AtomicBoolean. false)
      (AtomicReference. (d/success-deferred true))
      (utils/mutex))))

(extend-protocol s/Sourceable

  BlockingQueue
  (to-source [queue]
    (BlockingQueueSource.
      queue
      (AtomicReference. (d/success-deferred true))
      (utils/mutex)
      nil)))
