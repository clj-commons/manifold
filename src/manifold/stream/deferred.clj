(ns manifold.stream.deferred
  (:require
    [manifold.deferred :as d]
    [manifold.stream.core :as s])
  (:import
    [manifold.deferred
     IDeferred]
    [java.util.concurrent.atomic
     AtomicReference]
    [clojure.lang
     IPending]
    [java.util.concurrent
     Future]))

(s/def-sink DeferredSink
  [d]

  (isSynchronous [_]
    false)

  (description [_]
    {:type "deferred"})

  (put [this x blocking?]
    (if (d/success! d x)
      (do
        (.markClosed this)
        (if blocking?
          true
          (d/success-deferred true)))
      (do
        (.markClosed this)
        (if blocking?
          false
          (d/success-deferred false)))))

  (put [this x blocking? timeout timeout-val]
    (.put this x blocking?)))

(s/def-source DeferredSource
  [^AtomicReference d]

  (isSynchronous [_]
    false)

  (description [_]
    {:type "deferred"})

  (take [this default-val blocking?]
    (let [d (.getAndSet d ::none)]
      (if (identical? ::none d)
        (if blocking?
          default-val
          (d/success-deferred default-val))
        (do
          (.markDrained this)
          (if blocking?
            @d
            d)))))

  (take [this default-val blocking? timeout timeout-val]
    (let [d (.take this false ::none)]
      (if (identical? d ::none)
        (if blocking?
          default-val
          (d/success-deferred default-val))
        (do
          (.markDrained this)
          (let [d' (d/deferred)]
            (d/connect d d')
            (d/timeout! d' timeout timeout-val)))))))

(extend-protocol s/Sourceable

  IDeferred
  (to-source [d]
    (->DeferredSource
      (AtomicReference. d))))

(extend-protocol s/Sinkable

  IDeferred
  (to-sink [d]
    (->DeferredSink d)))
