(ns manifold.stream
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce partition partition-all])
  (:require
    [clojure.core :as core]
    [manifold.deferred :as d]
    [manifold.utils :as utils]
    [manifold.time :as time]
    [clojure.tools.logging :as log])
  (:import
    [java.util.concurrent
     CopyOnWriteArrayList
     ConcurrentHashMap
     BlockingQueue
     ArrayBlockingQueue
     LinkedBlockingQueue
     ConcurrentLinkedQueue
     TimeUnit]
    [java.util
     LinkedList
     Iterator]))

;;;

(defn- exists? [x]
  (try
    #_(Class/forName x)
    false
    (catch Throwable e
      false)))

(defprotocol Streamable
  (^:private to-stream [_] "Provides a conversion mechanism to manifold streams."))

(when-not (exists? "manifold.stream.IStream")
  (definterface IStream
    (description [])
    (isSynchronous [])))

(when-not (exists? "manifold.stream.IEventSink")
  (definterface IEventSink
   (put [x blocking?])
   (put [x blocking? timeout timeout-val])
   (close [])
   (downstream [])
   (isClosed [])
   (onClosed [callback])))

(when-not (exists? "manifold.stream.IEventSource")
  (definterface IEventSource
    (take [default-val blocking?])
    (take [default-val blocking? timeout timeout-val])
    (isDrained [])
    (onDrained [callback])
    (connector [sink])))

(let [^ConcurrentHashMap classes (ConcurrentHashMap.)]
  (add-watch #'Streamable ::memoization (fn [& _] (.clear classes)))
  (defn streamable?
    "Returns true if the object can be turned into a Manifold stream."
    [x]
    (if (nil? x)
      false
      (let [cls (class x)
            val (.get classes cls)]
        (if (nil? val)
          (let [val (satisfies? Streamable x)]
            (.put classes cls val)
            val)
          val)))))

(defn ->stream
  "Converts, is possible, the object to a Manifold stream, or `nil` if not possible."
  [x]
  (cond
    (instance? IStream x) x
    (streamable? x) (to-stream x)
    :else nil))

;;;

(definline stream?
  "Returns true if the object is a Manifold stream."
  [x]
  `(instance? IStream ~x))

(definline description
  "Returns a description of the stream."
  [x]
  `(.description ~(with-meta x {:tag "manifold.stream.IStream"})))

(definline synchronous?
  "Returns true if the underlying abstraction behaves synchronously, using thread blocking
   to provide backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "manifold.stream.IStream"})))

(definline close!
  "Closes an event sink, so that it can't accept any more messages."
  [sink]
  `(.close ~(with-meta sink {:tag "manifold.stream.IEventSink"})))

(definline closed?
  "Returns true if the event sink is closed."
  [sink]
  `(.isClosed ~(with-meta sink {:tag "manifold.stream.IEventSink"})))

(definline on-closed
  "Registers a no-arg callback which is invoked when the sink is closed."
  [sink callback]
  `(.onClosed ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~callback))

(definline drained?
  "Returns true if the event source is drained."
  [source]
  `(.isDrained ~(with-meta source {:tag "manifold.stream.IEventSource"})))

(definline on-drained
  "Registers a no-arg callback which is invoked when the source is drained."
  [source callback]
  `(.onDrained ~(with-meta source {:tag "manifold.stream.IEventSource"}) ~callback))

(defn put!
  "Puts a value into a sink, returning a deferred that yields `true` if it succeeds,
   and `false` if it fails.  Guaranteed to be non-blocking."
  {:inline (fn [sink x]
             `(.put ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~x false))}
  ([^IEventSink sink x]
     (.put sink x false)))

(defn put-all!
  "Puts all values into the sink, returning a deferred that yields `true` if all puts
   are successful, or `false` otherwise.  Guaranteed to be non-blocking."
  [^IEventSink sink msgs]
  (loop [msgs msgs]
    (if (empty? msgs)
      (d/success-deferred true)
      (let [d (put! sink (first msgs))]
        (if (d/realized? d)
          (let [x (try
                    (if @d
                      nil
                      (d/success-deferred false))
                    (catch Throwable e
                      (d/error-deferred e)))]
            (if (nil? x)
              (recur (rest msgs))
              x))
          (d/chain d
            (fn [_]
              (put-all! sink (rest msgs)))))))))

(defn try-put!
  "Puts a value into a stream if the put can successfully be completed in `timeout`
   milliseconds.  Returns a promiise that yields `true` if it succeeds, and `false`
   if it fails or times out.  Guaranteed to be non-blocking.

   A special `timeout-val` may be specified, if it is important to differentiate
   between failure due to timeout and other failures."
  {:inline (fn
             ([sink x timeout]
                `(.put ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~x false ~timeout false))
             ([sink x timeout timeout-val]
                `(.put ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~x false ~timeout ~timeout-val)))}
  ([^IEventSink sink x ^double timeout]
     (.put sink x false timeout false))
  ([^IEventSink sink x ^double timeout timeout-val]
     (.put sink x false timeout timeout-val)))

(defn take!
  "Takes a value from a stream, returning a deferred that yields the value when it
   is available, or `nil` if the take fails.  Guaranteed to be non-blocking.

   A special `default-val` may be specified, if it is important to differentiate
   between actual `nil` values and failures."
  {:inline (fn
             ([source]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) false nil))
             ([source default-val]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) false ~default-val)))}
  ([^IEventSource source]
     (.take source false nil))
  ([^IEventSource source default-val]
     (.take source false default-val)))

(defn try-take!
  "Takes a value from a stream, returning a deferred that yields the value if it is
   available within `timeout` milliseconds, or `nil` if it fails or times out.
   Guaranteed to be non-blocking.

   Special `timeout-val` and `default-val` values may be specified, if it is
   important to differentiate between actual `nil` values and failures."
  {:inline (fn
             ([source timeout]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) false nil ~timeout nil))
             ([source default-val timeout timeout-val]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) false ~default-val ~timeout ~timeout-val)))}
  ([^IEventSource source ^double timeout]
     (.take source false nil timeout nil))
  ([^IEventSource source default-val ^double timeout timeout-val]
     (.take source false default-val timeout timeout-val)))

;;;

(require '[manifold.stream.graph])

(defn connect
  {:arglists
   '[[src dst]
     [src
      dst
      {:keys [upstream?
              downstream?
              dst'
              timeout
              description]
       :or {upstream? false
            downstream? true}}]]}
  ([src dst]
     (connect src dst nil))
  ([^IEventSource src
    ^IEventSink dst
    options]
     (let [src (->stream src)
           dst (->stream dst)]
       (if (and src dst)
         (manifold.stream.graph/connect src dst options)
         (throw (IllegalArgumentException. "both arguments to 'connect' must be stream-able"))))))

;;;

(require '[manifold.stream.core])

(defn stream
  "Returns a Manifold stream with a configurable `buffer-size`.  If a capacity is specified,
   `put!` will yield `true` when the message is in the buffer.  Otherwise it will only yield
   `true` once it has been consumed."
  ([]
     (manifold.stream.core/stream))
  ([buffer-size]
     (manifold.stream.core/stream buffer-size)))

(defn stream* [options]
  (manifold.stream.core/stream* options))

;;;

(deftype SplicedStream
  [^IEventSink sink
   ^IEventSource source]

  IStream
  (isSynchronous [_]
    (or (synchronous? sink)
      (synchronous? source)))

  IEventSink
  (downstream [_]
    (.downstream sink))
  (put [_ x blocking?]
    (.put sink x blocking?))
  (put [_ x blocking? timeout timeout-val]
    (.put sink x blocking? timeout timeout-val))
  (close [_]
    (.close sink))
  (isClosed [_]
    (.isClosed sink))
  (onClosed [_ callback]
    (.onClosed sink callback))

  IEventSource
  (take [_ default-val blocking?]
    (.take source default-val blocking?))
  (take [_ default-val blocking? timeout timeout-val]
    (.take source default-val blocking? timeout timeout-val))
  (isDrained [_]
    (.isDrained source))
  (onDrained [_ callback]
    (.onDrained source callback))
  (connector [_ sink]
    (.connector source sink)))

(defn splice
  "Slices together two halves of a stream."
  [sink source]
  (SplicedStream. sink source))

;;;

(deftype Callback
  [f
   ^IEventSink downstream
   constant-response]
  IStream
  (isSynchronous [_]
    false)
  IEventSink
  (downstream [_]
    nil)
  (put [_ x _]
    (try
      (let [rsp (f x)]
        (if (nil? constant-response)
          rsp
          constant-response))
      (catch Throwable e
        (d/error-deferred e))))
  (put [_ x _ _ _]
    (try
      (let [rsp (f x)]
        (if (nil? constant-response)
          rsp
          constant-response))
      (catch Throwable e
        (d/error-deferred e))))
  (close [_]
    (when downstream
      (.close downstream)))
  (isClosed [_]
    (if downstream
      (.isClosed downstream)
      false))
  (onClosed [_ callback]
    (when downstream
      (.onClosed downstream callback))))

(let [result (d/success-deferred true)]
  (defn consume
    [callback source]
    (connect source (Callback. callback nil result) nil)))

(defn connect-via
  ([src callback dst]
     (connect-via src callback dst nil))
  ([src callback dst options]
     (let [dst (->stream dst)]
       (if dst
         (connect
           src
           (Callback. callback dst nil)
           (let [options' {:dst' dst}]
             (if options
               (merge options options')
               options')))
         (throw (IllegalArgumentException. "both arguments to 'connect-via' must be stream-able"))))))

;;;

(defn stream->lazy-seq
  "Transforms a stream into a lazy sequence.  If a `timeout-interval` is defined, the sequence will terminate
   if `timeout-interval` milliseconds elapses without a new event."
  ([s]
     (lazy-seq
       (let [x @(take! s ::none)]
         (when-not (identical? ::none x)
           (cons x (stream->lazy-seq s))))))
  ([s timeout-interval]
     (lazy-seq
       (let [x @(try-take! s ::none timeout-interval ::none)]
         (when-not (identical? ::none x)
           (cons x (stream->lazy-seq s timeout-interval)))))))

(defn lazy-seq->stream
  "Transforms a lazy-sequence into a stream."
  ([s]
     (let [s' (stream)]
       (utils/future
         (try
           (loop [s s]
             (if (empty? s)
               (close! s')
               (let [x (first s)]
                 (.put ^IEventSink s' x true)
                 (recur (rest s)))))
           (catch Throwable e
             (.printStackTrace e)
             (log/error e "error in lazy-seq->stream")
             (close! s'))))
       s')))

(defn- periodically-
  [stream period initial-delay f]
  (let [cancel (promise)]
    (deliver cancel
      (time/every period initial-delay
        (fn []
          (try
            (let [d (put! stream (f))]
              (if (realized? d)
                (when-not @d
                  (do
                    (@cancel)
                    (close! stream)))
                (do
                  (@cancel)
                  (d/chain d
                    (fn [x]
                      (if-not x
                        (close! stream)
                        (periodically- stream period (- period (rem (System/currentTimeMillis) period)) f)))))))
            (catch Throwable e
              (@cancel)
              (close! stream)
              (log/error e "error in 'periodically' callback"))))))))

(defn periodically
  "Creates a stream which emits the result of invoking `(f)` every `period` milliseconds."
  ([period initial-delay f]
     (let [s (stream 1)]
       (periodically- s period initial-delay f)
       s))
  ([period f]
     (periodically period (- period (rem (System/currentTimeMillis) period)) f)))

;;;

(utils/when-core-async
  (require 'manifold.stream.async))

(require 'manifold.stream.queue)

;;;

(declare zip)

(defn map
  ([f ^IEventSource s]
     (let [s' (stream)]
       (connect-via s
         (fn [msg]
           (put! s' (f msg)))
         s')
       s'))
  ([f s & rest]
     (apply zip
       #(apply f %)
       s
       rest)))

(let [some-drained? (partial some #{::drained})]
  (defn zip
    ([a]
       (map vector a))
    ([a & rest]
       (let [srcs (list* a rest)
             intermediates (repeatedly (count srcs) stream)
             dst (stream)]

         (doseq [[a b] (core/map list srcs intermediates)]
           (connect-via a #(put! b %) b))

         ((fn this []
            (d/chain
              (->> intermediates
                (core/map #(take! % ::drained))
                (apply d/zip))
              (fn [msgs]
                (if (some-drained? msgs)
                  (do (close! dst) false)
                  (put! dst msgs)))
              (fn [result]
                (when result
                  (utils/without-overflow (this)))))))
         dst))))

(let [response (d/success-deferred true)]
  (defn filter
    [pred s]
    (let [s' (stream)]
      (connect-via s
        (fn [msg]
          (if (pred msg)
            (put! s' msg)
            response))
        s')
      s')))

(defn mapcat
  [f s]
  (let [s' (stream)]
    (connect-via s
      (fn [msg]
        ))))
