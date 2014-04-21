(ns manifold.stream.queue
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [manifold.utils :as utils])
  (:import
    [java.util.concurrent.atomic
     AtomicReference
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
   ^AtomicReference last-put
   ^AtomicReference last-take]

  IStream
  (isSynchronous [_]
    true)

  IEventSink
  (onClosed [this f]
    (locking this
      (if closed?
        (f)
        (.add closed-callbacks f))))
  (isClosed [_]
    closed?)

  (close [this]
    (locking this
      (if-not closed?
        (do
          (set! closed? true)
          (let [f (fn [_] (.offer queue ::closed))]
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
                 (locking this
                   (or
                     (and closed?
                       (d/success! d false))

                     (and (.offer queue x)
                       (d/success! d true))

                     (utils/defer
                       (.put queue x)
                       (d/success! d true)))))]
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
               (locking this
                 (or
                   (and closed?
                     (d/success! d false))

                   (and (.offer queue x)
                     (d/success! d true))

                   (utils/defer
                     (d/success! d
                       (if (.offer queue x timeout TimeUnit/MILLISECONDS)
                         true
                         timeout-val))))))]
      (if (realized? d')
        (f nil)
        (d/on-realized d' f f))
      (if blocking?
        @d
        d)))

  IEventSource
  (onDrained [this f]
    (locking this
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
          (if (identical? ::closed msg)
            (do
              (.offer queue ::closed)
              (.set drained? true)
              (utils/invoke-callbacks drained-callbacks)
              default-val)
            msg)
          timeout-val))
      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (locking this
                   (or
                     (and (.get drained?)
                       (d/success! d default-val))

                     (when-let [msg (.poll queue)]
                       (d/success! d
                         (if (identical? msg ::closed)
                           (do
                             (.offer queue ::closed)
                             default-val)
                           msg)))

                     (utils/defer
                       (d/success! d
                         (if-let [msg (if timeout
                                        (.poll queue timeout TimeUnit/MILLISECONDS)
                                        (.take queue))]
                           (if (identical? msg ::closed)
                             (do
                               (.offer queue ::closed)
                               (.set drained? true)
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
  (setBackpressure [this enabled?]
    )
  (connect [this sink options]))


(extend-protocol s/Streamable

  BlockingQueue
  (to-stream [queue]
    (BlockingQueueStream.
      queue
      false
      (AtomicBoolean. false)
      (LinkedBlockingQueue.)
      (LinkedBlockingQueue.)
      (AtomicReference. (d/success-deferred true))
      (AtomicReference. (d/success-deferred true)))))
