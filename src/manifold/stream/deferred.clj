(ns manifold.stream.deferred
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s])
  (:import
    [manifold.deferred
     IDeferred]
    [java.util.concurrent.atomic
     AtomicReference]
    [clojure.lang
     IPending]
    [java.util.concurrent
     Future]))

(s/def-source DeferredSource
  [^AtomicReference d]

  (isSynchronous [_]
    false)

  (description [_]
    {:type "deferred"})

  (take [this blocking? default-val]
    (let [d (.getAndSet d ::none)]
      (if (identical? ::none d)
        default-val
        (do
          (.markDrained this)
          (if blocking?
            @d
            d)))))

  (take [this blocking? default-val timeout timeout-val]
    (let [d (.take this false ::none)]
      (if (identical? d ::none)
        default-val
        (do
          (.markDrained this)
          (let [d' (d/deferred)]
            (d/connect d d')
            (d/timeout! d' timeout timeout-val)))))))

(extend-protocol s/Sourceable

  IDeferred
  (to-source [d]
    (create-DeferredSource
      (AtomicReference. d))))
