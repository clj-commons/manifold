(ns manifold.stream
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce partition partition-all concat partition-by])
  (:require
    [clojure.core :as clj]
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
     AtomicReference
     AtomicLong]
    [java.util
     LinkedList
     Iterator]))

;;;

(defprotocol Sinkable
  (^:private to-sink [_] "Provides a conversion mechanism to Manifold sinks."))

(defprotocol Sourceable
  (^:private to-source [_] "Provides a conversion mechanism to Manifold source."))

(definterface IEventStream
  (description [])
  (isSynchronous [])
  (downstream [])
  (weakHandle [reference-queue])
  (close []))

(definterface IEventSink
  (put [x blocking?])
  (put [x blocking? timeout timeout-val])
  (markClosed [])
  (isClosed [])
  (onClosed [callback]))

(definterface IEventSource
  (take [default-val blocking?])
  (take [default-val blocking? timeout timeout-val])
  (markDrained [])
  (isDrained [])
  (onDrained [callback])
  (connector [sink]))

(defmethod print-method IEventStream [o ^java.io.Writer w]
  (let [sink? (instance? IEventSink o)
        source? (instance? IEventSource o)]
    (.write w
      (str
        "<< "
        (cond
          (and source? sink?)
          "stream"

          source?
          "source"

          sink?
          "sink")
        ": " (pr-str (.description ^IEventStream o)) " >>"))))

(def ^:private default-stream-impls
  `((~'downstream [this#] (manifold.stream.graph/downstream this#))
    (~'weakHandle [this# ref-queue#]
      (utils/with-lock ~'lock
        (or ~'__weakHandle
          (set! ~'__weakHandle (WeakReference. this# ref-queue#)))))
    (~'close [this#])))

(def ^:private sink-params
  '[lock
    ^:volatile-mutable __isClosed
    ^java.util.LinkedList __closedCallbacks
    ^:volatile-mutable __weakHandle])

(def ^:private default-sink-impls
  `((~'close [this#] (.markClosed this#))
    (~'isClosed [this#] ~'__isClosed)
    (~'onClosed [this# callback#]
      (utils/with-lock ~'lock
        (if ~'__isClosed
          (callback#)
          (.add ~'__closedCallbacks callback#))))
    (~'markClosed [this#]
      (utils/with-lock ~'lock
        (set! ~'__isClosed true)
        (utils/invoke-callbacks ~'__closedCallbacks)))))

(def ^:private source-params
  '[lock
    ^:volatile-mutable __isDrained
    ^java.util.LinkedList __drainedCallbacks
    ^:volatile-mutable __weakHandle])

(def ^:private default-source-impls
  `((~'isDrained [this#] ~'__isDrained)
    (~'onDrained [this# callback#]
      (utils/with-lock ~'lock
        (if ~'__isDrained
          (callback#)
          (.add ~'__drainedCallbacks callback#))))
    (~'markDrained [this#]
      (utils/with-lock ~'lock
        (set! ~'__isDrained true)
        (utils/invoke-callbacks ~'__drainedCallbacks)))
    (~'connector [this# _#] nil)))

(defn- merged-body [& bodies]
  (let [bs (apply clj/concat bodies)]
    (->> bs
      (clj/map #(vector [(first %) (clj/count (second %))] %))
      (into {})
      vals)))

(defmacro def-source [name params & body]
  `(do
     (deftype ~name
       ~(vec (distinct (clj/concat params source-params)))
       manifold.stream.IEventStream
       manifold.stream.IEventSource
       ~@(merged-body default-stream-impls default-source-impls body))

     (defn ~(with-meta (symbol (str "->" name)) {:private true})
       [~@(clj/map #(with-meta % nil) params)]
       (new ~name ~@params (utils/mutex) false (LinkedList.) nil))))

(defmacro def-sink [name params & body]
  `(do
     (deftype ~name
       ~(vec (distinct (clj/concat params sink-params)))
       manifold.stream.IEventStream
       manifold.stream.IEventSink
       ~@(merged-body default-stream-impls default-sink-impls body))

     (defn ~(with-meta (symbol (str "->" name)) {:private true})
       [~@(clj/map #(with-meta % nil) params)]
       (new ~name ~@params (utils/mutex) false (LinkedList.) nil))))

(defmacro def-sink+source [name params & body]
  `(do
     (deftype ~name
       ~(vec (distinct (clj/concat params source-params sink-params)))
       manifold.stream.IEventStream
       manifold.stream.IEventSink
       manifold.stream.IEventSource
       ~@(merged-body default-stream-impls default-sink-impls default-source-impls body))

     (defn ~(with-meta (symbol (str "->" name)) {:private true})
       [~@(clj/map #(with-meta % nil) params)]
       (new ~name ~@params (utils/mutex) false (LinkedList.) nil false (LinkedList.)))))

;;;

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
  "Converts, if possible, the object to a Manifold sink, or `default-val` if it cannot.  If no
   default value is given, an `IllegalArgumentException` is thrown."
  ([x]
     (let [x' (->sink x ::none)]
       (if (identical? ::none x')
         (throw
           (IllegalArgumentException.
             (str "cannot convert " (.getCanonicalName (class x)) " to sink")))
         x')))
  ([x default-val]
     (cond
       (instance? IEventSink x) x
       (sinkable? x) (to-sink x)
       :else default-val)))

(defn ->source
  "Converts, if possible, the object to a Manifold sink, or `default-val` if it cannot.  If no
   default value is given, an `IllegalArgumentException` is thrown."
  ([x]
     (let [x' (->source x ::none)]
       (if (identical? ::none x')
         (throw
           (IllegalArgumentException.
             (str "cannot convert " (.getCanonicalName (class x)) " to source")))
         x')))
  ([x default-val]
     (cond
       (instance? IEventSource x) x
       (sourceable? x) (to-source x)
       :else default-val)))

(deftype SinkProxy [^IEventSink sink]
  IEventStream
  (description [_]
    (.description ^IEventStream sink))
  (isSynchronous [_]
    (.isSynchronous ^IEventStream sink))
  (downstream [_]
    (.downstream ^IEventStream sink))
  (close [_]
    (.close ^IEventStream sink))
  (weakHandle [_ ref-queue]
    (.weakHandle ^IEventStream sink ref-queue))
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
  clojure.lang.Seqable
  (seq [_] (seq source))
  IEventStream
  (description [_]
    (.description ^IEventStream source))
  (isSynchronous [_]
    (.isSynchronous ^IEventStream source))
  (downstream [_]
    (.downstream ^IEventStream source))
  (close [_]
    (.close ^IEventStream source))
  (weakHandle [_ ref-queue]
    (.weakHandle ^IEventStream source ref-queue))
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
  `(instance? IEventStream ~x))

(definline source?
  "Returns true if the object is a Manifold source"
  [x]
  `(instance? IEventSource ~x))

(definline sink?
  "Returns true if the object is a Manifold source"
  [x]
  `(instance? IEventSink ~x))

(definline description
  "Returns a description of the stream."
  [x]
  `(.description ~(with-meta x {:tag "manifold.stream.IEventStream"})))

(definline downstream
  "Returns all sinks downstream of the given source as a sequence of 2-tuples, with the
   first element containing the connection's description, and the second element containing
   the sink."
  [x]
  `(.downstream ~(with-meta x {:tag "manifold.stream.IEventStream"})))

(definline weak-handle
  "Returns a weak reference that can be used to construct topologies of streams."
  [x]
  `(.weakHandle ~(with-meta x {:tag "manifold.stream.IEventStream"}) nil))

(definline synchronous?
  "Returns true if the underlying abstraction behaves synchronously, using thread blocking
   to provide backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "manifold.stream.IEventStream"})))

(definline close!
  "Closes an event sink, so that it can't accept any more messages."
  [sink]
  `(.close ~(with-meta sink {:tag "manifold.stream.IEventStream"})))

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
      (d/chain' (put! sink (first msgs))
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
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) nil false))
             ([source default-val]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) ~default-val false)))}
  ([^IEventSource source]
     (.take source nil false))
  ([^IEventSource source default-val]
     (.take source default-val false)))

(defn try-take!
  "Takes a value from a stream, returning a deferred that yields the value if it is
   available within `timeout` milliseconds, or `nil` if it fails or times out.
   Guaranteed to be non-blocking.

   Special `timeout-val` and `default-val` values may be specified, if it is
   important to differentiate between actual `nil` values and failures."
  {:inline (fn
             ([source timeout]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) nil false ~timeout nil))
             ([source default-val timeout timeout-val]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) ~default-val false ~timeout ~timeout-val)))}
  ([^IEventSource source ^double timeout]
     (.take source nil false timeout nil))
  ([^IEventSource source default-val ^double timeout timeout-val]
     (.take source default-val false timeout timeout-val)))

;;;

(require '[manifold.stream.graph])

(defn connect
  "Connects a source to a sink, propagating all messages from the former into the latter.

   Optionally takes a map of parameters, which include:


   `upstream?` - if closing the sink should always close the source, even if there are other
   sinks downstream of the source.  Defaults to `false`.  Note that if the sink is the only
   thing downstream of the source, the source will always be closed, unless it is permanent.

   `downstream?` - if closing the source will close the sink.  Defaults to `true`.

   `timeout` - if defined, the maximum time, in milliseconds, that will be spent trying to
    put a message into the sink before closing it.  Useful when there are multiple sinks
    downstream of a source, and you want to avoid a single backed up sink from blocking
    all the others.

    `description` - describes the connection, useful for traversing the stream topology via
    `downstream`."
  {:arglists
   '[[source sink]
     [source
      sink
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
           connector (.connector ^IEventSource source sink)]
       (if connector
         (connector source sink options)
         (manifold.stream.graph/connect source sink options))
       nil)))

;;;

(require '[manifold.stream.core])

(defn stream
  "Returns a Manifold stream with a configurable `buffer-size`.  If a capacity is specified,
   `put!` will yield `true` when the message is in the buffer.  Otherwise it will only yield
   `true` once it has been consumed."
  ([]
     (manifold.stream.core/stream))
  ([buffer-size]
     (manifold.stream.core/stream buffer-size))
  ([buffer-size xform]
     (manifold.stream.core/stream buffer-size xform))
  ([buffer-size xform executor]
     (manifold.stream.core/stream buffer-size xform executor)))

(defn stream* [options]
  (manifold.stream.core/stream* options))

;;;

(deftype SplicedStream
  [^IEventSink sink
   ^IEventSource source]

  IEventStream
  (isSynchronous [_]
    (or (synchronous? sink)
      (synchronous? source)))
  (description [_]
    {:type "splice"
     :sink (.description ^IEventStream sink)
     :source (.description ^IEventStream source)})
  (downstream [_]
    (.downstream ^IEventStream source))
  (close [_]
    (.close ^IEventStream source)
    (.close ^IEventStream sink))
  (weakHandle [_ ref-queue]
    (.weakHandle ^IEventStream source ref-queue))

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
  (SplicedStream. (->sink sink) (->source source)))

;;;

(deftype Callback
  [f
   ^IEventSink downstream
   constant-response]
  IEventStream
  (isSynchronous [_]
    false)
  (close [_]
    (when downstream
      (.close ^IEventStream downstream)))
  (weakHandle [_ ref-queue]
    (if downstream
      (.weakHandle ^IEventStream downstream ref-queue)
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
        (log/error e "error in consume")
        (d/error-deferred e))))
  (put [_ x _ _ _]
    (try
      (let [rsp (f x)]
        (if (nil? constant-response)
          rsp
          constant-response))
      (catch Throwable e
        (log/error e "error in consume")
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
       (connect
         src
         (Callback. callback dst nil)
         (merge options {:dst' dst})))))

(defn- connect-via-proxy
  ([src proxy dst]
     (connect-via-proxy src proxy dst nil))
  ([src proxy dst options]
     (let [result (connect-via src #(put! proxy %) dst
                    (assoc options :downstream? false))]
       (on-drained src #(close! proxy))
       result)))

;;;

(defn stream->seq
  "Transforms a stream into a lazy sequence.  If a `timeout-interval` is defined, the sequence will terminate
   if `timeout-interval` milliseconds elapses without a new event."
  ([s]
     (lazy-seq
       (let [x @(take! s ::none)]
         (when-not (identical? ::none x)
           (cons x (stream->seq s))))))
  ([s timeout-interval]
     (lazy-seq
       (let [x @(try-take! s ::none timeout-interval ::none)]
         (when-not (identical? ::none x)
           (cons x (stream->seq s timeout-interval)))))))

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
                  (d/chain' d
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

(require 'manifold.stream.seq)

(require 'manifold.stream.deferred)

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
     (map #(apply f %)
       (apply zip s rest))))

(let [some-drained? (partial some #{::drained})]
  (defn zip
    "Takes n-many streams, and returns a single stream which will emit n-tuples representing
     a message from each stream."
    ([a]
       (map vector a))
    ([a & rest]
       (let [srcs (list* a rest)
             intermediates (clj/repeatedly (count srcs) stream)
             dst (stream)]

         (doseq [[a b] (clj/map list srcs intermediates)]
           (connect-via a #(put! b %) b {:description {:op "zip"}}))

         (d/loop []
           (d/chain'
             (->> intermediates
               (clj/map #(take! % ::drained))
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

(defn reductions
  "Equivalent to Clojure's `reductions`, but for streams instead of sequences."
  ([f s]
     (reductions f ::none s))
  ([f initial-value s]
     (let [s' (stream)
           val (atom initial-value)]
       (d/chain' (if (identical? ::none initial-value)
                   true
                   (put! s' initial-value))
         (fn [_]
           (connect-via s
             (fn [msg]
               (if (identical? ::none @val)
                 (do
                   (reset! val msg)
                   (put! s' msg))
                 (-> msg
                   (d/chain'
                     (partial f @val)
                     (fn [x]
                       (reset! val x)
                       (put! s' x)))
                   (d/catch (fn [e]
                              (log/error e "error in reductions")
                              (close! s)
                              false)))))
             s')))
       s')))

(defn reduce
  "Equivalent to Clojure's `reduce`, but returns a deferred representing the return value."
  ([f s]
     (reduce f ::none s))
  ([f initial-value s]
     (let [d (d/deferred)]
       (d/chain' (if (identical? ::none initial-value)
                   (take! s ::none)
                   initial-value)
         (fn [initial-value]
           (if (identical? ::none initial-value)
             (f)
             (d/loop [val initial-value]
               (-> (take! s ::none)
                 (d/chain' (fn [x]
                             (if (identical? ::none x)
                               (d/success! d val)
                               (d/recur (f val x)))))
                 (d/catch Throwable (fn [e] (d/error! d val))))))))
       d)))

(defn mapcat
  "Equivalent to Clojure's `mapcat`, but for streams instead of sequences."
  [f s]
  (let [s' (stream)]
    (connect-via s
      (fn [msg]
        (d/loop [s (f msg)]
          (when-not (empty? s)
            (d/chain' (put! s' (first s))
              (fn [_]
                (d/recur (rest s)))))))
      s'
      {:description {:op "mapcat"}})
    (source-only s')))

(defn partition-by
  "Equivalent to Clojure's `partition-by`, but returns a stream of streams."
  [f s]
  (let [in (stream)
        out (stream)]

    (connect-via-proxy s in out {:description {:op "partition-by"}})

    ;; TODO: how is this represented in the topology?
    (d/loop [prev ::x, s' nil]
      (d/chain' (take! in ::none)
        (fn [msg]
          (if (identical? ::none msg)
            (do
              (when s' (close! s'))
              (close! out))
            (let [curr (try
                         (f msg)
                         (catch Throwable e
                           (close! in)
                           (close! out)
                           (log/error e "error in partition-by")
                           ::error))]
              (when-not (identical? ::error curr)
                (if (= prev curr)
                  (d/chain' (put! s' msg)
                    (fn [_] (d/recur curr s')))
                  (let [s'' (stream)]
                    (when s' (close! s'))
                    (d/chain' (put! out s'')
                      (fn [_] (put! s'' msg))
                      (fn [_] (d/recur curr s'')))))))))))

    out))

(defn concat
  "Takes a stream of streams, and flattens it into a single stream."
  [s]
  (let [in (stream)
        out (stream)]

    (connect-via-proxy s in out {:description {:op "concat"}})

    (d/loop []
      (d/chain' (take! in ::none)
        (fn [s']
          (if (identical? ::none s')
            (do
              (close! out)
              s')
            (d/loop []
              (d/chain' (take! s' ::none)
                (fn [msg]
                  (if (identical? ::none msg)
                    msg
                    (put! out msg)))
                (fn [result]
                  (case result
                    false (close! in)
                    ::none nil
                    (d/recur)))))))
        (fn [result]
          (when-not (identical? ::none result)
            (d/recur)))))
    out))

;;;

(defn buffered-stream
  "A stream which will buffer at most `limit` data, where the size of each message
   is defined by `(metric message)`."
  ([buffer-size]
     (buffered-stream (constantly 1) buffer-size))
  ([metric limit]
     (buffered-stream metric limit nil))
  ([metric limit description]
     (let [buf (stream Integer/MAX_VALUE)
           buffer-size (AtomicLong. 0)
           last-put (AtomicReference. (d/success-deferred true))
           buf+ (fn [^long n]
                  (loop []
                    (let [buf (.get buffer-size)
                          buf' (unchecked-add buf n)]
                      (if (.compareAndSet buffer-size buf buf')
                        (cond
                          (and (<= buf limit) (< limit buf'))
                          (d/success! (.getAndSet last-put (d/deferred)) true)

                          (and (< limit buf) (<= buf' limit))
                          (d/success! (.get last-put) true))
                        (recur)))))
           handle (atom nil)]

       (reify
         IEventStream
         (isSynchronous [_]
           false)
         (downstream [this]
           (manifold.stream.graph/downstream this))
         (close [_]
           (.close ^IEventStream buf))
         (description [_]
           (merge
             (manifold.stream/description buf)
             description
             {:buffer-size (.get buffer-size)
              :buffer-capacity limit}))
         (weakHandle [this ref-queue]
           (or @handle
             (do
               (compare-and-set! handle nil (WeakReference. this ref-queue))
               @handle)))

         IEventSink
         (put [_ x blocking?]
           (buf+ (metric x))
           (.put ^IEventSink buf x blocking?)
           (let [val (.get last-put)]
             (if blocking?
               @val
               val)))
         (put [_ x blocking? timeout timeout-val]
           ;; TODO: this doesn't really time out, because that would
           ;; require consume-side filtering of messages
           (buf+ (metric x))
           (.put ^IEventSink buf x blocking? timeout timeout-val)
           (let [val (.get last-put)]
             (if blocking?
               @val
               val)))
         (isClosed [_]
           (.isClosed ^IEventSink buf))
         (onClosed [_ callback]
           (.onClosed ^IEventSink buf callback))

         IEventSource
         (take [_ default-val blocking?]
           (let [val (d/chain' (.take ^IEventSource buf default-val blocking?)
                       (fn [x]
                         (if (identical? default-val x)
                           x
                           (do
                             (buf+ (- (metric x)))
                             x))))]
             (if blocking?
               @val
               val)))
         (take [_ default-val blocking? timeout timeout-val]
           (let [val (d/chain' (.take ^IEventSource buf default-val blocking? timeout ::timeout)
                       (fn [x]
                         (cond

                           (identical? ::timeout x)
                           timeout-val

                           (identical? default-val x)
                           x

                           :else
                           (do
                             (buf+ (- (metric x)))
                             x))))]
             (if blocking?
               @val
               val)))
         (isDrained [_]
           (.isDrained ^IEventSource buf))
         (onDrained [_ callback]
           (.onDrained ^IEventSource buf callback))
         (connector [_ sink]
           (.connector ^IEventSource buf sink))))))


(defn buffer
  "Takes a stream, and returns a stream which is a buffered view of that stream.  The buffer
   size may either be measured in messages, or if a `metric` is defined, by the sum of `metric`
   mapped over all messages currently buffered."
  ([limit s]
     (let [s' (buffered-stream limit)]
       (connect s s')
       (source-only s')))
  ([metric limit s]
     (let [s' (buffered-stream metric limit)]
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

       (connect-via-proxy s buf s' {:upstream? true, :description {:op "batch"}})

       (d/loop [msgs [], earliest-message -1]
         (if (= max-size (count msgs))

           (d/chain' (put! s' msgs)
             (fn [_]
               (d/recur [] -1)))

           (d/chain' (if (or
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
                 (d/chain' (when-not (empty? msgs)
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

;;;

(alter-meta! #'->Callback assoc :private true)
(alter-meta! #'->SinkProxy assoc :private true)
(alter-meta! #'->SourceProxy assoc :private true)
(alter-meta! #'->SplicedStream assoc :private true)
