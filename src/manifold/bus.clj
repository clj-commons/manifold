(ns manifold.bus
  (:require
    [manifold
     [stream :as s]
     [deferred :as d]])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]
    [java.lang.reflect
     Array]))

(set! *unchecked-math* true)

(definterface IEventBus
  (subscribe [topic])
  (publish [topic message])
  (isActive [topic]))

(definline publish!
  "Publishes a message on the bus, returning a deferred result representing the message
   being accepted by all subscribers.  To prevent one slow consumer from blocking all
   the others, use `manifold.stream/buffer`."
  [bus topic message]
  `(.publish ~(with-meta bus {:tag "manifold.bus.IEventBus"}) ~topic ~message))

(definline subscribe
  "Returns a stream which consumes all messages from `topic`."
  [bus topic]
  `(.subscribe ~(with-meta bus {:tag "manifold.bus.IEventBus"}) ~topic))

(definline active?
  "Returns `true` if there are any subscribers to `topic`."
  [bus topic]
  `(.isActive ~(with-meta bus {:tag "manifold.bus.IEventBus"}) ~topic))

(defn- conj' [ary x]
  (if (nil? ary)
    (object-array [x])
    (let [len (Array/getLength ary)
          ary' (object-array (inc len))]
      (System/arraycopy ary 0 ary' 0 len)
      (aset ^objects ary' len x)
      ary')))

(defn- disj' [^objects ary x]
  (let [len (Array/getLength ary)]
    (if-let [idx (loop [i 0]
                   (if (<= len i)
                     nil
                     (if (identical? x (aget ary i))
                       i
                       (recur (inc i)))))]
      (if (== 1 len)
        nil
        (let [ary' (object-array (dec len))]
          (System/arraycopy ary 0 ary' 0 idx)
          (System/arraycopy ary (inc idx) ary' idx (- len idx 1))
          ary')))))

(defn event-bus
  "Returns an event bus that can be used with `publish!` and `subscribe`."
  []
  (let [topic->subscribers (ConcurrentHashMap.)]
    (reify IEventBus
      (subscribe [_ topic]
        (let [s (s/stream)]

          ;; CAS to add
          (loop []
            (let [subscribers (.get topic->subscribers topic)
                  subscribers' (conj' subscribers s)]
              (if (nil? subscribers)
                (when (.putIfAbsent topic->subscribers topic subscribers')
                  (recur))
                (when-not (.replace topic->subscribers subscribers subscribers')
                  (recur)))))

          ;; CAS to remove
          (s/on-closed s
            (fn []
              (loop []
                (let [subscribers (.get topic->subscribers topic)
                      subscribers' (disj' subscribers s)]
                  (if (nil? subscribers')
                    (when-not (.remove topic->subscribers topic subscribers)
                      (recur))
                    (when-not (.replace topic->subscribers subscribers subscribers')
                      (recur)))))))
          (s/source-only s)))

      (publish [_ topic message]
        (let [subscribers (.get topic->subscribers topic)]
          (if (nil? subscribers)
            (d/success-deferred false)
            (-> (apply d/zip (map #(s/put! % message) subscribers))
              (d/chain (fn [_] true))))))

      (isActive [_ topic]
        (boolean (.get topic->subscribers topic))))))
