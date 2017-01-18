(ns
  ^{:author "Zach Tellman"
    :doc "An implementation of an event bus, where publishers and subscribers can interact via topics."}
  manifold.bus
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
  (snapshot [])
  (subscribe [topic])
  (downstream [topic])
  (publish [topic message])
  (isActive [topic]))

(definline publish!
  "Publishes a message on the bus, returning a deferred result representing the message
   being accepted by all subscribers.  To prevent one slow consumer from blocking all
   the others, use `manifold.stream/buffer`, or `manifold.stream/connect` with a timeout
   specified."
  [bus topic message]
  `(.publish ~(with-meta bus {:tag "manifold.bus.IEventBus"}) ~topic ~message))

(definline subscribe
  "Returns a stream which consumes all messages from `topic`."
  [bus topic]
  `(.subscribe ~(with-meta bus {:tag "manifold.bus.IEventBus"}) ~topic))

(definline downstream
  "Returns a list of all streams subscribed to `topic`."
  [bus topic]
  `(.downstream ~(with-meta bus {:tag "manifold.bus.IEventBus"}) ~topic))

(definline active?
  "Returns `true` if there are any subscribers to `topic`."
  [bus topic]
  `(.isActive ~(with-meta bus {:tag "manifold.bus.IEventBus"}) ~topic))

(definline topic->subscribers
  [bus]
  `(.snapshot ~(with-meta bus {:tag "manifold.bus.IEventBus"})))

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
      (let [idx (long idx)]
        (if (== 1 len)
          nil
          (let [ary' (object-array (dec len))]
            (System/arraycopy ary 0 ary' 0 idx)
            (System/arraycopy ary (inc idx) ary' idx (- len idx 1))
            ary')))
      ary)))

(deftype Wrapper [x]
  Object
  (hashCode [_] (hash x))
  (equals [_ o] (= x (.x ^Wrapper o))))

(defn- wrap [x]
  (Wrapper. x))

(defn- unwrap [w]
  (.x ^Wrapper w))

(defn event-bus
  "Returns an event bus that can be used with `publish!` and `subscribe`."
  ([]
    (event-bus s/stream))
  ([stream-generator]
    (let [topic->subscribers (ConcurrentHashMap.)]
      (reify IEventBus

        (snapshot [_]
          (->> topic->subscribers
            (map
              (fn [[topic subscribers]]
                (clojure.lang.MapEntry. (unwrap topic) (into [] subscribers))))
            (into {})))

        (subscribe [_ topic]
          (let [s (stream-generator)]

            ;; CAS to add
            (loop []
              (let [subscribers (.get topic->subscribers (wrap topic))
                    subscribers' (conj' subscribers s)]
                (if (nil? subscribers)
                  (when (.putIfAbsent topic->subscribers (wrap topic) subscribers')
                    (recur))
                  (when-not (.replace topic->subscribers (wrap topic) subscribers subscribers')
                    (recur)))))

            ;; CAS to remove
            (s/on-closed s
              (fn []
                (loop []
                  (let [subscribers (.get topic->subscribers (wrap topic))
                        subscribers' (disj' subscribers s)]
                    (if (nil? subscribers')
                      (when-not (.remove topic->subscribers (wrap topic) subscribers)
                        (recur))
                      (when-not (.replace topic->subscribers (wrap topic) subscribers subscribers')
                        (recur)))))))

            (s/source-only s)))

        (publish [_ topic message]
          (let [subscribers (.get topic->subscribers (wrap topic))]
            (if (nil? subscribers)
              (d/success-deferred false)
              (-> (apply d/zip' (map #(s/put! % message) subscribers))
                (d/chain' (fn [_] true))))))

        (downstream [_ topic]
          (seq (.get topic->subscribers (wrap topic))))

        (isActive [_ topic]
          (boolean (.get topic->subscribers (wrap topic))))))))
