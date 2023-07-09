(ns manifold.stream
  {:author "Zach Tellman"
   :doc    "Methods for creating, transforming, and interacting with asynchronous streams of values."}
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce concat])
  (:require
    [clojure.core :as clj]
    [manifold.deferred :as d]
    [potemkin.types :refer [deftype+]]
    [clj-commons.primitive-math :as p]
    [manifold.utils :as utils]
    [manifold.time :as time]
    [manifold.stream
     [core :as core]
     [default :as default]
     random-access
     iterator
     queue
     seq
     deferred]
    [clojure.tools.logging :as log])
  (:import
    [manifold.stream.core
     IEventSink
     IEventSource
     IEventStream]
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

(set! *unchecked-math* true)

(utils/when-core-async
  (require 'manifold.stream.async))

;;;

(let [f (utils/fast-satisfies #'core/Sinkable)]
  (defn sinkable? [x]
    (or
      (instance? IEventSink x)
      (f x))))

(let [f (utils/fast-satisfies #'core/Sourceable)]
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
     (sinkable? x) (core/to-sink x)
     :else default-val)))

(defn ->source
  "Converts, if possible, the object to a Manifold source, or `default-val` if it cannot.  If no
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
     (sourceable? x) (core/to-source x)
     :else default-val)))

(deftype+ SinkProxy [^IEventSink sink]
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

(deftype+ SourceProxy [^IEventSource source]
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

(definline onto
  "Returns an identical stream whose deferred callbacks will be executed
   on `executor`."
  [executor s]
  `(manifold.stream.default/onto ~executor ~s))

;;;

(definline stream?
  "Returns true if the object is a Manifold stream."
  [x]
  `(instance? IEventStream ~x))

(definline source?
  "Returns true if the object is a Manifold source."
  [x]
  `(instance? IEventSource ~x))

(definline sink?
  "Returns true if the object is a Manifold sink."
  [x]
  `(instance? IEventSink ~x))

(definline description
  "Returns a description of the stream."
  [x]
  `(.description ~(with-meta x {:tag "manifold.stream.core.IEventStream"})))

(definline downstream
  "Returns all sinks downstream of the given source as a sequence of 2-tuples, with the
   first element containing the connection's description, and the second element containing
   the sink."
  [x]
  `(.downstream ~(with-meta x {:tag "manifold.stream.core.IEventStream"})))

(definline weak-handle
  "Returns a weak reference that can be used to construct topologies of streams."
  [x]
  `(.weakHandle ~(with-meta x {:tag "manifold.stream.core.IEventStream"}) nil))

(definline synchronous?
  "Returns true if the underlying abstraction behaves synchronously, using thread blocking
   to provide backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "manifold.stream.core.IEventStream"})))

(definline close!
  "Closes a sink, so you can't `put!` any more messages onto it.

   Note that if it's also a source, `take!`s will still work until the source is drained."

  [sink]
  `(.close ~(with-meta sink {:tag "manifold.stream.core.IEventStream"})))

(definline closed?
  "Returns true if the event sink is closed."
  [sink]
  `(.isClosed ~(with-meta sink {:tag "manifold.stream.core.IEventSink"})))

(definline on-closed
  "Registers a no-arg callback which is invoked when the sink is closed."
  [sink callback]
  `(.onClosed ~(with-meta sink {:tag "manifold.stream.core.IEventSink"}) ~callback))

(definline drained?
  "Returns true if the event source is drained."
  [source]
  `(.isDrained ~(with-meta source {:tag "manifold.stream.core.IEventSource"})))

(definline on-drained
  "Registers a no-arg callback which is invoked when the source is drained."
  [source callback]
  `(.onDrained ~(with-meta source {:tag "manifold.stream.core.IEventSource"}) ~callback))

(defn put!
  "Puts a value into a sink, returning a deferred that yields `true` if it succeeds,
   and `false` if it fails.  Guaranteed to be non-blocking."
  {:inline (fn [sink x]
             `(.put ~(with-meta sink {:tag "manifold.stream.core.IEventSink"}) ~x false))}
  ([^IEventSink sink x]
   (.put sink x false)))

(defn put-all!
  "Puts all values into the sink, returning a deferred that yields `true` if all puts
   are successful, or `false` otherwise.  If the sink provides backpressure, will
   pause. Guaranteed to be non-blocking."
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
   milliseconds.  Returns a promise that yields `true` if it succeeds, and `false`
   if it fails or times out.  Guaranteed to be non-blocking.

   A special `timeout-val` may be specified, if it is important to differentiate
   between failure due to timeout and other failures."
  {:inline (fn
             ([sink x timeout]
              `(.put ~(with-meta sink {:tag "manifold.stream.core.IEventSink"}) ~x false ~timeout false))
             ([sink x timeout timeout-val]
              `(.put ~(with-meta sink {:tag "manifold.stream.core.IEventSink"}) ~x false ~timeout ~timeout-val)))}
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
              `(.take ~(with-meta source {:tag "manifold.stream.core.IEventSource"}) nil false))
             ([source default-val]
              `(.take ~(with-meta source {:tag "manifold.stream.core.IEventSource"}) ~default-val false)))}
  ([^IEventSource source]
   (.take source nil false))
  ([^IEventSource source default-val]
   (.take source default-val false)))

(defn try-take!
  "Takes a value from a stream, returning a deferred that yields the value if it is
   available within `timeout` milliseconds, or `nil` if it fails or times out.
   Guaranteed to be non-blocking.

   Special `timeout-val` and `default-val` values may be specified, if it is
   important to differentiate between actual `nil` values and timeouts/failures."
  {:inline (fn
             ([source timeout]
              `(.take ~(with-meta source {:tag "manifold.stream.core.IEventSource"}) nil false ~timeout nil))
             ([source default-val timeout timeout-val]
              `(.take ~(with-meta source {:tag "manifold.stream.core.IEventSource"}) ~default-val false ~timeout ~timeout-val)))}
  ([^IEventSource source ^double timeout]
   (.take source nil false timeout nil))
  ([^IEventSource source default-val ^double timeout timeout-val]
   (.take source default-val false timeout timeout-val)))

;;;

(require '[manifold.stream.graph])

(defn connect
  "Connects a source to a sink, propagating all messages from the former into the latter.

   Optionally takes a map of parameters:

   |:---|:---
   | `upstream?` | Whether closing the sink should always close the source, even if there are other sinks downstream of the source.  Defaults to `false`.  Note that if the sink is the only thing downstream of the source, the source will eventually be closed, unless it is permanent.
   | `downstream?` | Whether closing the source will close the sink.  Defaults to `true`.
   | `timeout` | If defined, the maximum time, in milliseconds, that will be spent trying to put a message into the sink before closing it.  Useful when there are multiple sinks downstream of a source, and you want to avoid a single backed-up sink from blocking all the others.
   | `description` | Describes the connection, useful for traversing the stream topology via `downstream`."
  {:arglists
   '[[source sink]
     [source
      sink
      {:keys [upstream?
              downstream?
              timeout
              description]
       :or   {upstream?   false
              downstream? true}}]]}
  ([source sink]
   (connect source sink nil))
  ([^IEventSource source
    ^IEventSink sink
    options]
   (let [source    (->source source)
         sink      (->sink sink)
         connector (.connector ^IEventSource source sink)]
     (if connector
       (connector source sink options)
       (manifold.stream.graph/connect source sink options))
     nil)))

;;;

(defn stream
  "Returns a Manifold stream with a configurable `buffer-size`.  If a capacity is specified,
   `put!` will yield `true` when the message is in the buffer.  Otherwise, it will only yield
   `true` once it has been consumed.

   `xform` is an optional transducer, which will transform all messages that are enqueued
   via `put!` before they are dequeued via `take!`.

   `executor`, if defined, specifies which java.util.concurrent.Executor will be used to
   handle the deferreds returned by `put!` and `take!`."
  ([]
   (default/stream))
  ([buffer-size]
   (default/stream buffer-size))
  ([buffer-size xform]
   (default/stream buffer-size xform))
  ([buffer-size xform executor]
   (default/stream buffer-size xform executor)))

(defn stream*
  "An alternate way to build a stream, via a map of parameters.

   |:---|:---
   | `permanent?` | if `true`, the channel cannot be closed
   | `buffer-size` | the number of messages that can accumulate in the channel before backpressure is applied
   | `description` | the description of the channel, which is a single arg function that takes the base properties and returns an enriched map.
   | `executor` | the `java.util.concurrent.Executor` that will execute all callbacks registered on the deferreds returns by `put!` and `take!`
   | `xform` | a transducer which will transform all messages that are enqueued via `put!` before they are dequeued via `take!`."
  {:arglists '[[{:keys [permanent? buffer-size description executor xform]}]]}
  [options]
  (default/stream* options))

;;;

(deftype+ SplicedStream
  [^IEventSink sink
   ^IEventSource source
   ^:volatile-mutable mta
   lock]

  clojure.lang.IReference
  (meta [_] mta)
  (resetMeta [_ m]
    (utils/with-lock* lock
      (set! mta m)))
  (alterMeta [_ f args]
    (utils/with-lock* lock
      (set! mta (apply f mta args))))


  IEventStream
  (isSynchronous [_]
    (or (synchronous? sink)
        (synchronous? source)))
  (description [_]
    {:type   "splice"
     :sink   (.description ^IEventStream sink)
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
  "Splices together two halves of a stream, such that all messages enqueued via `put!` go
   into `sink`, and all messages dequeued via `take!` come from `source`."
  [sink source]
  (SplicedStream. (->sink sink) (->source source) nil (utils/mutex)))

;;;

(deftype+ Callback
  [f
   close-callback
   ^IEventSink downstream
   constant-response]
  IEventStream
  (isSynchronous [_]
    false)
  (close [_]
    (when close-callback
      (close-callback)))
  (weakHandle [_ ref-queue]
    (if downstream
      (.weakHandle ^IEventStream downstream ref-queue)
      (throw (IllegalArgumentException.))))
  (description [_]
    {:type "callback"})
  (downstream [_]
    (when downstream
      [[(description downstream) downstream]]))
  IEventSink
  (put [this x _]
    (try
      (let [rsp (f x)]
        (if (nil? constant-response)
          rsp
          constant-response))
      (catch Throwable e
        (log/error e "error in stream handler")
        (.close this)
        (d/success-deferred false))))
  (put [this x default-val _ _]
    (.put this x default-val))
  (isClosed [_]
    (if downstream
      (.isClosed downstream)
      false))
  (onClosed [_ callback]
    (when downstream
      (.onClosed downstream callback))))

(let [result (d/success-deferred true)]
  (defn consume
    "Feeds all messages from `source` into `callback`.

     Messages will be processed as quickly as the callback can be executed. Returns
     a deferred which yields `true` when `source` is exhausted."
    [callback source]
    (let [complete (d/deferred)]
      (connect source (Callback. callback #(d/success! complete true) nil result) nil)
      complete)))

(defn consume-async
  "Feeds all messages from `source` into `callback`, which must return a deferred yielding
   `true` or `false`.  If the returned value yields `false`, the consumption will be cancelled.

   Messages will be processed only as quickly as the deferred values are realized. Returns a
   deferred which yields `true` when `source` is exhausted or `callback` yields `false`."
  [callback source]
  (let [complete (d/deferred)
        callback #(d/chain %
                           callback
                           (fn [result]
                             (when (p/false? result)
                               (d/success! complete true))
                             result))]
    (connect source (Callback. callback #(d/success! complete true) nil nil) nil)
    complete))

(defn connect-via
  "Feeds all messages from `src` into `callback`, with the understanding that they will
   eventually be propagated into `dst` in some form.  The return value of `callback`
   should be a deferred yielding either `true` or `false`. When `false`,  the downstream
   sink is assumed to be closed, and the connection is severed.

   Returns a deferred which yields `true` when `src` is exhausted or `callback` yields `false`."
  ([src callback dst]
   (connect-via src callback dst nil))
  ([src callback dst options]
   (let [dst            (->sink dst)
         complete       (d/deferred)
         close-callback #(do
                           (close! dst)
                           (d/success! complete true))]
     (connect
       src
       (Callback. callback close-callback dst nil)
       options)
     complete)))

(defn- connect-via-proxy
  ([src proxy dst]
   (connect-via-proxy src proxy dst nil))
  ([src proxy dst options]
   (let [dst            (->sink dst)
         proxy          (->sink proxy)
         complete       (d/deferred)
         close-callback #(do
                           (close! proxy)
                           (d/success! complete true))]
     (connect
       src
       (Callback. #(put! proxy %) close-callback dst nil)
       options)
     complete)))

(defn drain-into
  "Takes all messages from `src` and puts them into `dst`, and returns a deferred that
   yields `true` once `src` is drained or `dst` is closed.  If `src` is closed or drained,
   `dst` will not be closed."
  [src dst]
  (let [dst      (->sink dst)
        complete (d/deferred)]
    (connect
      src
      (Callback. #(put! dst %) #(d/success! complete true) dst nil)
      {:description "drain-into"})
    complete))

;;;

(defn stream->seq
  "Transforms a stream into a lazy sequence.  If a `timeout-interval` is defined, the sequence
   will terminate if `timeout-interval` milliseconds elapses without a new event."
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
  [stream ^long period initial-delay f]
  (let [cancel (promise)]
    (deliver cancel
             (time/every period
                         initial-delay
                         (fn []
                           (try
                             (let [d (if (closed? stream)
                                       (d/success-deferred false)
                                       (put! stream (f)))]
                               (if (d/realized? d)
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
                                                 (periodically- stream period (p/- period (rem (System/currentTimeMillis) period)) f)))))))
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
  ([^long period f]
   (periodically period (p/- period (rem (System/currentTimeMillis) period)) f)))

(declare zip)

(defn transform
  "Takes a transducer `xform` and returns a source which applies it to source `s`. A buffer-size
   may optionally be defined for the output source."
  ([xform s]
   (transform xform 0 s))
  ([xform buffer-size ^IEventSource s]
   (let [s' (stream buffer-size xform)]
     (connect s s' {:description {:op "transducer"}})
     (source-only s'))))

(defn map
  "Equivalent to Clojure's `map`, but for streams instead of sequences."
  ([f s]
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

(defn realize-each
  "Takes a stream of potentially deferred values, and returns a stream of realized values."
  [s]
  (let [s' (stream)]
    (connect-via s
                 (fn [msg]
                   (-> msg
                       (d/chain' #(put! s' %))
                       (d/catch' (fn [e]
                                   (log/error e "deferred realized as error, closing stream")
                                   (close! s')
                                   false))))
                 s'
                 {:description {:op "realize-each"}})
    (source-only s')))

(let [some-drained? (partial some #{::drained})]
  (defn zip
    "Takes n-many streams, and returns a single stream which will emit n-tuples representing
     a message from each stream."
    ([a]
     (map vector a))
    ([a & rest]
     (let [srcs          (list* a rest)
           intermediates (clj/repeatedly (count srcs) stream)
           dst           (stream)]

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

(defn filter
  "Equivalent to Clojure's `filter`, but for streams instead of sequences."
  [pred s]
  (let [s' (stream)]
    (connect-via s
                 (fn [msg]
                   (if (pred msg)
                     (put! s' msg)
                     (d/success-deferred true)))
                 s'
                 {:description {:op "filter"}})
    (source-only s')))

(defn reductions
  "Equivalent to Clojure's `reductions`, but for streams instead of sequences."
  ([f s]
   (reductions f ::none s))
  ([f initial-value s]
   (let [s'  (stream)
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
                                      (d/catch' (fn [e]
                                                  (log/error e "error in reductions")
                                                  (close! s)
                                                  false)))))
                              s')))

     (source-only s'))))

(defn reduce
  "Equivalent to Clojure's `reduce`, but returns a deferred representing the return value.

  The deferred will be realized once the stream is closed or if the accumulator
  functions returns a `reduced` value."
  ([f s]
   (reduce f ::none s))
  ([f initial-value s]
   (-> (if (identical? ::none initial-value)
         (take! s ::none)
         initial-value)
       (d/chain'
         (fn [initial-value]
           (if (identical? ::none initial-value)
             (f)
             (d/loop [val initial-value]
               (-> (take! s ::none)
                   (d/chain' (fn [x]
                               (if (identical? ::none x)
                                 val
                                 (let [r (f val x)]
                                   (if (reduced? r)
                                     (deref r)
                                     (d/recur r))))))))))))))

(defn mapcat
  "Equivalent to Clojure's `mapcat`, but for streams instead of sequences.

  Note that just like `clojure.core/mapcat`, the provided function `f`
  must return a collection and not a stream."
  ([f s]
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
  ([f s & rest]
   (->> (apply zip s rest)
        (mapcat #(apply f %)))))

(defn lazily-partition-by
  "Equivalent to Clojure's `partition-by`, but returns a stream of streams.  This means that
   if a sub-stream is not completely consumed, the next sub-stream will never be emitted.

   Use with caution.  If you're not totally sure you want a stream of streams, use
   `(transform (partition-by f))` instead."
  [f s]
  (let [in  (stream)
        out (stream)]

    (connect-via-proxy s in out {:description {:op "lazily-partition-by"}})

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
                                   (log/error e "error in lazily-partition-by")
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

    (source-only out)))

(defn concat
  "Takes a stream of streams, and flattens it into a single stream."
  [s]
  (let [in  (stream)
        out (stream)]

    (connect-via-proxy s in out {:description {:op "concat"}})

    (d/loop []
      (d/chain' (take! in ::none)
                (fn [s']
                  (cond
                    (closed? out)
                    (close! s')

                    (identical? ::none s')
                    (do
                      (close! out)
                      s')

                    :else
                    (d/loop []
                      (d/chain' (take! s' ::none)
                                (fn [msg]
                                  (if (identical? ::none msg)
                                    msg
                                    (put! out msg)))
                                (fn [result]
                                  (case result
                                    false (do (close! s') (close! in))
                                    ::none nil
                                    (d/recur)))))))
                (fn [result]
                  (when-not (identical? ::none result)
                    (d/recur)))))

    (source-only out)))

;;;

(deftype+ BufferedStream
  [buf
   limit
   metric
   description
   ^AtomicLong buffer-size
   ^AtomicReference last-put
   buf+
   handle
   mta]

  clojure.lang.IReference
  (meta [_] @mta)
  (resetMeta [_ m]
    (reset! mta m))
  (alterMeta [_ f args]
    (apply swap! mta f args))

  IEventStream
  (isSynchronous [_]
    false)
  (downstream [this]
    (manifold.stream.graph/downstream this))
  (close [_]
    (.close ^IEventStream buf))
  (description [_]
    (description
      (merge
        (manifold.stream/description buf)
        {:buffer-size     (.get buffer-size)
         :buffer-capacity limit})))
  (weakHandle [this ref-queue]
    (or @handle
        (do
          (compare-and-set! handle nil (WeakReference. this ref-queue))
          @handle)))

  IEventSink
  (put [_ x blocking?]
    (let [size (metric x)]
      (let [val (d/chain' (.put ^IEventSink buf [size x] blocking?)
                          (fn [result]
                            (if result
                              (do
                                (buf+ size)
                                (.get last-put))
                              false)))]
        (if blocking?
          @val
          val))))
  (put [_ x blocking? timeout timeout-val]
    ;; TODO: this doesn't really time out, because that would
    ;; require consume-side filtering of messages
    (let [size (metric x)]
      (let [val (d/chain' (.put ^IEventSink buf [size x] blocking? timeout ::timeout)
                          (fn [result]
                            (cond

                              (identical? result ::timeout)
                              timeout-val

                              (p/false? result)
                              false

                              :else
                              (do
                                (buf+ size)
                                (.get last-put)))))]
        (if blocking?
          @val
          val))))
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
                            (let [[size msg] x]
                              (buf+ (p/- ^long size))
                              msg))))]
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
                            (let [[size msg] x]
                              (buf+ (p/- ^long size))
                              msg))))]
      (if blocking?
        @val
        val)))
  (isDrained [_]
    (.isDrained ^IEventSource buf))
  (onDrained [_ callback]
    (.onDrained ^IEventSource buf callback))
  (connector [_ sink]
    (.connector ^IEventSource buf sink)))

(defn buffered-stream
  "A stream which will buffer at most `limit` data, where the size of each message
   is defined by `(metric message)`.

   `description` is a fn that takes the existing description map and returns a new one."
  ([buffer-size]
   (buffered-stream (constantly 1) buffer-size))
  ([metric limit]
   (buffered-stream metric limit identity))
  ([metric ^long limit description]
   (let [buf         (stream Integer/MAX_VALUE)
         buffer-size (AtomicLong. 0)
         last-put    (AtomicReference. (d/success-deferred true))
         buf+        (fn [^long n]
                       (locking last-put
                         (let [buf' (.addAndGet buffer-size n)
                               buf  (unchecked-subtract buf' n)]
                           (cond
                             (and (p/<= buf' limit) (p/< limit buf))
                             (-> last-put .get (d/success! true))

                             (and (p/<= buf limit) (p/< limit buf'))
                             (-> last-put (.getAndSet (d/deferred)) (d/success! true))))))]

     (BufferedStream.
       buf
       limit
       metric
       description
       buffer-size
       last-put
       buf+
       (atom nil)
       (atom nil)))))


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
   latency, in milliseconds.  By default, each message is of size `1`, but a custom `metric` function that
   returns the size of each message may be defined."
  ([batch-size s]
   (batch (constantly 1) batch-size nil s))
  ([max-size max-latency s]
   (batch (constantly 1) max-size max-latency s))
  ([metric ^long max-size ^long max-latency s]
   (assert (pos? max-size))

   (let [buf (stream)
         s'  (stream)]

     (connect-via-proxy s buf s' {:description {:op "batch"}})
     (on-closed s' #(close! buf))

     (d/loop [msgs [], ^long size 0, ^long earliest-message -1, ^long last-message -1]
       (cond
         (or
           (p/== size max-size)
           (and (p/< max-size size) (p/== (count msgs) 1)))
         (d/chain' (put! s' msgs)
                   (fn [_]
                     (d/recur [] 0 -1 -1)))

         (p/> size max-size)
         (let [msg (peek msgs)]
           (d/chain' (put! s' (pop msgs))
                     (fn [_]
                       (d/recur [msg] (metric msg) last-message last-message))))

         :else
         (d/chain' (if (or
                         (nil? max-latency)
                         (neg? earliest-message)
                         (empty? msgs))
                     (take! buf ::empty)
                     (try-take! buf
                                ::empty
                                (p/- max-latency (p/- (System/currentTimeMillis) earliest-message))
                                ::timeout))
                   (fn [msg]
                     (condp identical? msg
                       ::empty
                       (do
                         (when-not (empty? msgs)
                           (put! s' msgs))
                         (close! s'))

                       ::timeout
                       (d/chain' (when-not (empty? msgs)
                                   (put! s' msgs))
                                 (fn [_]
                                   (d/recur [] 0 -1 -1)))

                       (let [time (System/currentTimeMillis)]
                         (d/recur
                           (conj msgs msg)
                           (p/+ size ^long (metric msg))
                           (if (p/< earliest-message 0)
                             time
                             earliest-message)
                           time)))))))

     (source-only s'))))

(defn throttle
  "Limits the `max-rate` that messages are emitted, per second.

   The `max-backlog` dictates how much \"memory\" the throttling mechanism has, or how many
   messages it will emit immediately after a long interval without any messages.  By default,
   this is set to one second's worth."
  ([max-rate s]
   (throttle max-rate max-rate s))
  ([^long max-rate ^double max-backlog s]
   (let [buf    (stream)
         s'     (stream)
         period (p/double (p// 1000 max-rate))]

     (connect-via-proxy s buf s' {:description {:op "throttle"}})
     (on-closed s' #(close! buf))

     (d/loop [^double backlog 0.0, ^long read-start (System/currentTimeMillis)]
       (d/chain (take! buf ::none)

                (fn [msg]
                  (if (identical? ::none msg)
                    (do
                      (close! s')
                      false)
                    (put! s' msg)))

                (fn [result]
                  (when result
                    (let [elapsed  (p/double (p/- (System/currentTimeMillis) read-start))
                          backlog' (p/min (p/+ backlog (p/- (p// elapsed period) 1.0)) max-backlog)]
                      (if (p/<= 1.0 backlog')
                        (p/- backlog' 1.0)
                        (d/timeout! (d/deferred) (p/- period elapsed) 0.0)))))

                (fn [backlog]
                  (if backlog
                    (d/recur backlog (System/currentTimeMillis))
                    (close! s)))))

     (source-only s'))))

(defn dropping-stream
  "Creates a new stream with a buffer of size `n`, which will drop
   incoming items when full.

   If `source` is supplied, inserts a dropping stream after `source`
   with the provided capacity."
  {:author "Ryan Schmukler"
   :added "0.3.0"}
  ([n]
   (let [in  (stream)
         out (dropping-stream n in)]
     (splice in out)))
  ([n source]
   (let [sink (stream n)]
     (connect-via
       source
       (fn [val]
         (d/let-flow [put-result (try-put! sink val 0 ::timeout)]
           (case put-result
             true     true
             false    false
             ::timeout true)))
       sink
       {:upstream?   true
        :downstream? true})
     sink)))

(defn sliding-stream
  "Creates a new stream with a buffer of size `n`, which will drop
   the oldest items when full to make room for new items.

   If `source` is supplied, inserts a sliding stream after `source`
   with the provided capacity."
  {:author "Ryan Schmukler"
   :added "0.3.0"}
  ([n]
   (let [in  (stream)
         out (sliding-stream n in)]
     (splice in out)))
  ([n source]
   (let [sink (stream n)]
     (connect-via
       source
       (fn [val]
         (d/loop []
           (d/chain
             (try-put! sink val 0 ::timeout)
             (fn [put-result]
               (case put-result
                 true     true
                 false    false
                 ::timeout (d/chain (take! sink)
                                   (fn [_] (d/recur))))))))
       sink
       {:upstream?   true
        :downstream? true})
     sink)))

;;;

(alter-meta! #'->Callback assoc :private true)
(alter-meta! #'->SinkProxy assoc :private true)
(alter-meta! #'->SourceProxy assoc :private true)
(alter-meta! #'->SplicedStream assoc :private true)
