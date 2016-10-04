(ns manifold.stream.iterator
  (:require
    [clojure.tools.logging :as log]
    [manifold.deferred :as d]
    [manifold.utils :as utils]
    [manifold.stream
     [core :as s]
     [graph :as g]]
    [manifold.time :as time])
  (:import
    [java.util
     Iterator]
    [java.util.concurrent.atomic
     AtomicReference]))

(s/def-source IteratorSource
  [^Iterator iterator
   ^AtomicReference last-take]

  (isSynchronous [_]
    true)

  (close [_]
    (if (instance? java.io.Closeable iterator)
      (.close ^java.io.Closeable iterator)))

  (description [this]
    {:type "iterator"
     :drained? (s/drained? this)})

  (take [this default-val blocking?]
    (if blocking?

      (if (.hasNext iterator)
        (.next iterator)
        (do
          (.markDrained this)
          default-val))

      (let [d  (d/deferred)
            d' (.getAndSet last-take d)
            f  (fn [_]
                 (utils/wait-for
                   (when-let [token (d/claim! d)]
                     (if (.hasNext iterator)
                       (d/success! d (.next iterator) token)
                       (do
                         (.markDrained this)
                         (d/success! d default-val token))))))]
        (if (d/realized? d')
          (f nil)
          (d/on-realized d' f f))
        d)))

  (take [this default-val blocking? timeout timeout-val]
    (if (nil? timeout)
      (.take this blocking? default-val)
      (let [d (-> (.take this default-val false)
                (d/timeout! timeout timeout-val))]
        (if blocking?
          @d
          d)))))

(extend-protocol s/Sourceable

  java.util.Iterator
  (to-source [iterator]
    (->IteratorSource
      iterator
      (AtomicReference. (d/success-deferred true)))))

(utils/when-class java.util.stream.BaseStream

  (extend-protocol s/Sourceable

    java.util.stream.BaseStream
    (to-source [stream]
      (->IteratorSource
        (.iterator ^java.util.stream.BaseStream stream)
        (AtomicReference. (d/success-deferred true))))))
