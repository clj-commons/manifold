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
    [java.lang.ref
     WeakReference]
    [java.util.concurrent
     CopyOnWriteArrayList
     ConcurrentHashMap
     BlockingQueue
     ArrayBlockingQueue
     LinkedBlockingQueue
     ConcurrentLinkedQueue
     TimeUnit]
    [java.util.concurrent.atomic
     AtomicReference]
    [java.util
     LinkedList
     Iterator]))

;;;

(defprotocol Sinkable
  (^:private to-sink [_] "Provides a conversion mechanism to Manifold sinks."))

(defprotocol Sourceable
  (^:private to-source [_] "Provides a conversion mechanism to Manifold source."))

(definterface IStream
  (description [])
  (isSynchronous [])
  (downstream [])
  (weakHandle [reference-queue])
  (close []))

(definterface IEventSink
  (put [x blocking?])
  (put [x blocking? timeout timeout-val])
  (isClosed [])
  (onClosed [callback]))

(definterface IEventSource
  (take [default-val blocking?])
  (take [default-val blocking? timeout timeout-val])
  (isDrained [])
  (onDrained [callback])
  (connector [sink]))

(let [f (utils/fast-satisfies #'Sinkable)]
  (defn sinkable? [x]
    (or
      (instance? IEventSink x)
      (f x))))

(let [f (utils/fast-satisfies #'Sourceable)]
  (defn sourceable? [x]
    (or
      (instance? IEventSource x)
      (f x))))

(defn ->sink
  "Converts, is possible, the object to a Manifold sink, or `nil` if not possible."
  [x]
  (cond
    (instance? IEventSink x) x
    (sinkable? x) (to-sink x)
    :else nil))

(defn ->source
  "Converts, is possible, the object to a Manifold source, or `nil` if not possible."
  [x]
  (cond
    (instance? IEventSource x) x
    (sourceable? x) (to-source x)
    :else nil))

(deftype SinkProxy [^IEventSink sink]
  IStream
  (description [_]
    (.description ^IStream sink))
  (isSynchronous [_]
    (.isSynchronous ^IStream sink))
  (downstream [_]
    (.downstream ^IStream sink))
  (close [_]
    (.close ^IStream sink))
  (weakHandle [_ ref-queue]
    (.weakHandle ^IStream sink ref-queue))
  IEventSink
  (put [_ x blocking?]
    (.put sink x blocking?))
  (put [_ x blocking? timeout timeout-val]
    (.put sink x blocking? timeout timeout-val))
  (isClosed [_]
    (.isClosed sink))
  (onClosed [_ callback]
    (.onClosed sink callback)))

(declare connect)

(deftype SourceProxy [^IEventSource source]
  IStream
  (description [_]
    (.description ^IStream source))
  (isSynchronous [_]
    (.isSynchronous ^IStream source))
  (downstream [_]
    (.downstream ^IStream source))
  (close [_]
    (.close ^IStream source))
  (weakHandle [_ ref-queue]
    (.weakHandle ^IStream source ref-queue))
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
    (fn [_ sink options]
      (connect source sink options))))

(defn source-only
  "Returns a view of the stream which is only a source."
  [s]
  (SourceProxy. s))

(defn sink-only
  "Returns a view of the stream which is only a sink."
  [s]
  (SinkProxy. s))

;;;

(definline stream?
  "Returns true if the object is a Manifold stream."
  [x]
  `(instance? IStream ~x))

(definline description
  "Returns a description of the stream."
  [x]
  `(.description ~(with-meta x {:tag "manifold.stream.IStream"})))

(definline downstream
  "Returns all sinks downstream of the given source as a sequence of 2-tuples, with the
   first element containing the connection's description, and the second element containing
   the sink."
  [x]
  `(.downstream ~(with-meta x {:tag "manifold.stream.IStream"})))

(definline weak-handle
  "Returns a weak reference that can be used to construct topologies of streams."
  [x]
  `(.weakHandle ~(with-meta x {:tag "manifold.stream.IStream"}) nil))

(definline synchronous?
  "Returns true if the underlying abstraction behaves synchronously, using thread blocking
   to provide backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "manifold.stream.IStream"})))

(definline close!
  "Closes an event sink, so that it can't accept any more messages."
  [sink]
  `(.close ~(with-meta sink {:tag "manifold.stream.IStream"})))

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
  (d/loop [msgs msgs]
    (if (empty? msgs)
      true
      (d/chain (put! sink (first msgs))
        (fn [result]
          (if result
            (d/recur (rest msgs))
            false))))))

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
              timeout
              description]
       :or {upstream? false
            downstream? true}}]]}
  ([source sink]
     (connect source sink nil))
  ([^IEventSource source
    ^IEventSink sink
    options]
     (let [source (->source source)
           sink (->sink sink)
           connector (and source sink (.connector ^IEventSource source sink))]
       (cond

         connector
         (connector source sink options)

         (and source sink)
         (manifold.stream.graph/connect source sink options)

         (not source)
         (throw (IllegalArgumentException. "invalid source passed into 'connect'"))

         (not sink)
         (throw (IllegalArgumentException. "invalid sink passed into 'connect'"))))))

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
  (description [_]
    {:type "splice"
     :sink (.description ^IStream sink)
     :source (.description ^IStream source)})
  (downstream [_]
    (.downstream ^IStream source))
  (close [_]
    (.close ^IStream source)
    (.close ^IStream sink))
  (weakHandle [_ ref-queue]
    (.weakHandle ^IStream source ref-queue))

  IEventSink
  (put [_ x blocking?]
    (.put sink x blocking?))
  (put [_ x blocking? timeout timeout-val]
    (.put sink x blocking? timeout timeout-val))
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
  (let [sink (->sink sink)
        source (->source source)]
    (cond
      (and sink source)
      (SplicedStream. sink source)

      (not sink)
      (throw (IllegalArgumentException. "invalid sink passed into 'splice'"))

      (not source)
      (throw (IllegalArgumentException. "invalid source passed into 'splice'")))))

;;;

(deftype Callback
  [f
   ^IEventSink downstream
   constant-response]
  IStream
  (isSynchronous [_]
    false)
  (close [_]
    (when downstream
      (.close ^IStream downstream)))
  (weakHandle [_ ref-queue]
    (if downstream
      (.weakHandle ^IStream downstream ref-queue)
      (throw (IllegalArgumentException.))))
  (description [_]
    {:type "callback"})
  (downstream [_]
    (when downstream [downstream]))
  IEventSink
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
  (isClosed [_]
    (if downstream
      (.isClosed downstream)
      false))
  (onClosed [_ callback]
    (when downstream
      (.onClosed downstream callback))))

(let [result (d/success-deferred true)]
  (defn consume
    "Feeds all messages from `source` into `callback`."
    [callback source]
    (connect source (Callback. callback nil result) nil)))

(defn connect-via
  "Feeds all messages from `src` into `callback`, with the understanding that they will eventually
   be propagated into `dst` in some form."
  ([src callback dst]
     (connect-via src callback dst nil))
  ([src callback dst options]
     (let [dst (->sink dst)]
       (if dst
         (connect
           src
           (Callback. callback dst nil)
           (merge options {:dst' dst}))
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
       (source-only s'))))

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
       (source-only s)))
  ([period f]
     (periodically period (- period (rem (System/currentTimeMillis) period)) f)))

;;;

(utils/when-core-async
  (require 'manifold.stream.async))

(require 'manifold.stream.queue)

;;;

(declare zip)

(defn map
  "Equivalent to Clojure's `map`, but for streams instead of sequences."
  ([f ^IEventSource s]
     (let [s' (stream)]
       (connect-via s
         (fn [msg]
           (put! s' (f msg)))
         s'
         {:description {:op "map"}})
       (source-only s')))
  ([f s & rest]
     (apply zip
       #(apply f %)
       s
       rest)))

(let [some-drained? (partial some #{::drained})]
  (defn zip
    "Takes n-many streams, and returns a single stream which will emit n-tuples representing
     a message from each stream."
    ([a]
       (map vector a))
    ([a & rest]
       (let [srcs (list* a rest)
             intermediates (repeatedly (count srcs) stream)
             dst (stream)]

         (doseq [[a b] (core/map list srcs intermediates)]
           (connect-via a #(put! b %) b {:description {:op "zip"}}))

         (d/loop []
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
                 (d/recur)))))

         (source-only dst)))))

(let [response (d/success-deferred true)]
  (defn filter
    "Equivalent to Clojure's `filter`, but for streams instead of sequences."
    [pred s]
    (let [s' (stream)]
      (connect-via s
        (fn [msg]
          (if (pred msg)
            (put! s' msg)
            response))
        s'
        {:description {:op "filter"}})
      (source-only s'))))

(defn mapcat
  "Equivalent to Clojure's `mapcat`, but for streams instead of sequences."
  [f s]
  (let [s' (stream)]
    (connect-via s
      (fn [msg]
        (d/loop [s (f msg)]
          (when-not (empty? s)
            (d/chain (put! s' (first s))
              (fn [_]
                (d/recur (rest s)))))))
      s'
      {:description {:op "mapcat"}})
    (source-only s')))

;;;

(defn buffer-stream
  "A stream which will buffer at most `limit` data, where the size of each message
   is defined by `(metric message)`."
  [metric limit]
  (let [buf (stream Integer/MAX_VALUE)
        buffer-size (atom 0)
        last-put (AtomicReference. (d/success-deferred true))
        buf+ (fn [n]
               (loop []
                 (let [buf @buffer-size
                       buf' (+ buf n)]
                   (if (compare-and-set! buffer-size buf buf')
                     (cond
                       (< buf limit buf')
                       (d/success!
                         (.getAndSet last-put (d/deferred))
                         true)

                       (< buf' limit buf)
                       (d/success! (.get last-put) true))
                     (recur)))))
        handle (atom nil)]

    (reify
      IStream
      (isSynchronous [_]
        false)
      (downstream [this]
        (manifold.stream.graph/downstream this))
      (close [_]
        (.close ^IStream buf))
      (description [_]
        (merge
          (description buf)
          {:buffer-size @buffer-size
           :buffer-capacity limit}))
      (weakHandle [this ref-queue]
        (or @handle
          (do
            (compare-and-set! handle nil (WeakReference. this ref-queue))
            @handle)))

      IEventSink
      (put [_ x blocking?]
        (.put ^IEventSink buf x blocking?)
        (buf+ (metric x))
        (.get last-put))
      (put [_ x blocking? timeout timeout-val]
        ;; TODO: this doesn't really time out, because that would
        ;; require consume-side filtering of messages
        (.put ^IEventSink buf x blocking? timeout timeout-val)
        (buf+ (metric x))
        (.get last-put))
      (isClosed [_]
        (.isClosed ^IEventSink buf))
      (onClosed [_ callback]
        (.onClosed ^IEventSink buf callback))

      IEventSource
      (take [_ default-val blocking?]
        (d/chain (.take ^IEventSource buf default-val blocking?)
          (fn [x]
            (buf+ (- (metric x)))
            x)))
      (take [_ default-val blocking? timeout timeout-val]
        (d/chain (.take ^IEventSource buf default-val blocking? timeout ::timeout)
          (fn [x]
            (if (identical? ::timeout x)
              timeout-val
              (do
                (buf+ (- (metric x)))
                x)))))
      (isDrained [_]
        (.isDrained ^IEventSource buf))
      (onDrained [_ callback]
        (.onDrained ^IEventSource buf callback))
      (connector [_ sink]
        (.connector ^IEventSource buf sink)))))

(defn buffer
  "Takes a stream, and returns a stream which is a buffered view of that stream.  The buffer
   size may either be measured in messages, or if a `metric` is defined, by the sum of `metric`
   mapped over all messages currently buffered."
  ([limit s]
     (let [s' (stream limit)]
       (connect s s')
       (source-only s')))
  ([metric limit s]
     (let [s' (buffer-stream metric limit)]
       (connect s s')
       (source-only s'))))

(defn batch
  "Batches messages, either into groups of fixed size, or according to upper bounds on size and
   latency, in milliseconds."
  ([batch-size s]
     (batch -1 batch-size s))
  ([max-latency max-size s]
     (assert (pos? max-size))

     (let [buf (stream)
           s' (stream)]

       (connect-via s #(put! buf %) s'
         {:upstream? true
          :downstream? false
          :description {:op "batch"}})
       (on-drained s #(close! buf))

       (d/loop [msgs [], earliest-message -1]
         (if (= max-size (count msgs))

           (d/chain (put! s' msgs)
             (fn [_]
               (d/recur [] -1)))

           (d/chain (if (or
                          (neg? max-latency)
                          (neg? earliest-message)
                          (empty? msgs))
                      (take! buf ::none)
                      (try-take! buf
                        ::none
                        (- max-latency (- (System/currentTimeMillis) earliest-message))
                        ::none))
             (fn [msg]
               (if (identical? ::none msg)
                 (d/chain (when-not (empty? msgs)
                            (put! s' msgs))
                   (fn [_]
                     (if (drained? s)
                       (close! s')
                       (d/recur [] -1))))
                 (d/recur
                   (conj msgs msg)
                   (if (neg? earliest-message)
                     (System/currentTimeMillis)
                     earliest-message)))))))

       (source-only s'))))
