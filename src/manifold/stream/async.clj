(ns manifold.stream.async
  (:require
    [manifold.deferred :as d]
    [clojure.core.async :as a]
    [manifold.stream
     [graph :as g]
     [core :as s]]
    [manifold
     [executor :as executor]
     [utils :as utils]])
  (:import
    [java.util.concurrent.atomic
     AtomicReference]))

(s/def-source CoreAsyncSource
  [ch
   ^AtomicReference last-take]

  (isSynchronous [_] false)

  (description [this]
    {:source? true
     :drained? (s/drained? this)
     :type "core.async"})

  (close [_]
    (a/close! ch))

  (take [this default-val blocking?]
    (if blocking?

      (let [x (a/<!! ch)]
        (if (nil? x)
          (do
            (.markDrained this)
            default-val)
          x))

      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (a/take! ch
                   (fn [msg]
                     (d/success! d
                       (if (nil? msg)
                         (do
                           (.markDrained this)
                           default-val)
                         msg)))))]
        (if (d/realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (take [this default-val blocking? timeout timeout-val]
    (let [d  (d/deferred)
          d' (.getAndSet last-take d)

          ;; if I don't take this out of the goroutine, core.async OOMs on compilation
          mark-drained #(.markDrained this)
          f  (fn [_]
               (a/go
                 (let [result (a/alt!
                                ch ([x] (if (nil? x)
                                          (do
                                            (mark-drained)
                                            default-val)
                                          x))
                                (a/timeout timeout) timeout-val
                                :priority true)]
                   (d/success! d result))))]
      (if (d/realized? d')
        (f nil)
        (d/on-realized d' f f))
      (if blocking?
        @d
        d))))

(s/def-sink CoreAsyncSink
  [ch
   ^AtomicReference last-put]

  (isSynchronous [_] false)

  (description [this]
    {:sink? true
     :closed? (s/closed? this)
     :type "core.async"})

  (close [this]
    (utils/with-lock lock
      (if (s/closed? this)
        false
        (do
          (.markClosed this)
          (let [d (.get last-put)
                f (fn [_] (a/close! ch))]
            (d/on-realized d
              (fn [_] (a/close! ch))
              nil)
            true)))))

  (put [this x blocking?]

    (assert (not (nil? x)) "core.async channel cannot take `nil` as a message")

    (utils/with-lock lock
      (cond
        (s/closed? this)
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
                     (d/success! d
                       (boolean
                         (a/>! ch x)))))]
          (if (d/realized? d')
            (f nil)
            (d/on-realized d' f f))
          d))))

  (put [this x blocking? timeout timeout-val]

    (if (nil? timeout)
      (.put this x blocking?)
      (assert (not (nil? x)) "core.async channel cannot take `nil` as a message"))

    (utils/with-lock lock

      (if (s/closed? this)

        (if blocking?
          false
          (d/success-deferred false))

        (let [d  (d/deferred)
              d' (.getAndSet last-put d)
              f  (fn [_]
                   (a/go
                     (let [result (a/alt!
                                    [[ch x]] true
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
    (->CoreAsyncSink
      ch
      (AtomicReference. (d/success-deferred true)))))

(extend-protocol s/Sourceable

  clojure.core.async.impl.channels.ManyToManyChannel
  (to-source [ch]
    (->CoreAsyncSource
      ch
      (AtomicReference. (d/success-deferred true)))))
