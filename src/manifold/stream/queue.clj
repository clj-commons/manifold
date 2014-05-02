(ns manifold.stream.queue
  (:require
    [manifold.stream.graph :as g]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [manifold.utils :as utils])
  (:import
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

(deftype BlockingQueueStream
  [^BlockingQueue queue
   ^:volatile-mutable closed?
   ^AtomicBoolean drained?
   ^BlockingQueue closed-callbacks
   ^BlockingQueue drained-callbacks
   ^AtomicInteger pending-puts
   ^AtomicReference last-put
   ^AtomicReference last-take
   lock]

  IStream
  (isSynchronous [_]
    true)

  (description [_]
    {:type (.getCanonicalName (class queue))
     :buffer-size (.size queue)})

  IEventSink
  (downstream [this]
    (g/downstream this))

  (onClosed [this f]
    (utils/with-lock lock
      (if closed?
        (f)
        (.add closed-callbacks f))))

  (isClosed [_]
    closed?)

  (close [this]
    (utils/with-lock lock
      (if-not closed?
        (do
          (set! closed? true)
          (let [f (fn [_]
                    (utils/with-lock lock
                      (.offer queue ::drained)))]
            (d/on-realized (.get last-put) f f))
          (utils/invoke-callbacks closed-callbacks)
          true)
        false)))

  (put [this x blocking?]

    (assert (not (nil? x)) "BlockingQueue cannot take `nil` as a message")

    (if blocking?

      (.put queue x)

      (let [d  (d/deferred)
            d' (.getAndSet last-put d)
            f  (fn [_]
                 (utils/with-lock lock
                   (try
                     (or
                       (and closed?
                         (.decrementAndGet pending-puts)
                         (d/success! d false))

                      (and (.offer queue x)
                        (.decrementAndGet pending-puts)
                        (d/success! d true))

                      (utils/wait-for
                        (.put queue x)
                        (.decrementAndGet pending-puts)
                        (d/success! d true))))))]
        (.incrementAndGet pending-puts)
        (if (realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (put [this x blocking? timeout timeout-val]

    (if (nil? timeout)
      (.put this x blocking?)
      (assert (not (nil? x)) "BlockingQueue cannot take `nil` as a message"))

    (let [d  (d/deferred)
          d' (.getAndSet last-put d)
          f  (fn [_]
               (utils/with-lock lock
                 (or
                   (and closed?
                     (.decrementAndGet pending-puts)
                     (d/success! d false))

                   (and (.offer queue x)
                     (.decrementAndGet pending-puts)
                     (d/success! d true))

                   (utils/wait-for
                     (d/success! d
                       (if (let [x (.offer queue x timeout TimeUnit/MILLISECONDS)]
                             (.decrementAndGet pending-puts)
                             x)
                         true
                         timeout-val))))))]
      (.incrementAndGet pending-puts)
      (if (realized? d')
        (f nil)
        (d/on-realized d' f f))
      (if blocking?
        @d
        d)))

  IEventSource
  (onDrained [this f]
    (utils/with-lock lock
      (if (.get drained?)
        (f)
        (.add drained-callbacks f))))

  (isDrained [_]
    (.get drained?))

  (take [this blocking? default-val]
    (.take this blocking? default-val nil nil))

  (take [this blocking? default-val timeout timeout-val]
    (if blocking?
      (if (.get drained?)
        default-val
        (if-let [msg (if timeout
                       (.poll queue timeout TimeUnit/MILLISECONDS)
                       (.take queue))]
          (if (identical? ::drained msg)
            (utils/with-lock lock
              (.set drained? true)
              (.offer queue ::drained)
              (utils/invoke-callbacks drained-callbacks)
              default-val)
            msg)
          timeout-val))
      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (utils/with-lock lock
                   (or
                     (and (.get drained?)
                       (d/success! d default-val))

                     (let [msg (.poll queue)]
                       (if (nil? msg)

                         (when (and closed? (zero? (.get pending-puts)))
                           (.set drained? true)
                           (utils/invoke-callbacks drained-callbacks)
                           (d/success! d default-val))

                         (d/success! d
                           (if (identical? msg ::drained)
                             (do
                               (.offer queue ::drained)
                               default-val)
                             msg))))

                     (utils/wait-for
                       (d/success! d
                         (if-let [msg (if timeout
                                        (.poll queue timeout TimeUnit/MILLISECONDS)
                                        (.take queue))]
                           (if (identical? msg ::drained)
                             (utils/with-lock lock
                               (.set drained? true)
                               (.offer queue ::drained)
                               (utils/invoke-callbacks drained-callbacks)
                               default-val)
                             msg)
                           timeout-val))))))]
        (if (realized? d')
          (f nil)
          (d/on-realized d' f f))
        (if blocking?
          @d
          d))))

  (connector [this sink]
    nil))


(extend-protocol s/Streamable

  BlockingQueue
  (to-stream [queue]
    (BlockingQueueStream.
      queue
      false
      (AtomicBoolean. false)
      (LinkedBlockingQueue.)
      (LinkedBlockingQueue.)
      (AtomicInteger. 0)
      (AtomicReference. (d/success-deferred true))
      (AtomicReference. (d/success-deferred true))
      (utils/mutex))))
