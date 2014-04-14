(ns manifold.stream
  (:require
    [manifold.promise :as p]
    [manifold.utils :as utils]
    [manifold.time :as time])
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
     LinkedList]))

;;;

(defprotocol Streamable
  (to-stream [_] "Provides a conversion mechanism to manifold streams."))

(definterface IStream
  (isSynchronous []))

(definterface IEventSink
  (put [x blocking?])
  (put [x blocking? timeout timeout-val])
  (close [])
  (isClosed [])
  (onClosed [callback]))

(definterface IEventSource
  (take [default-val blocking?])
  (take [default-val blocking? timeout timeout-val])
  (isDrained [])
  (onDrained [callback])
  (setBackpressure [enabled?])
  (connect [sink options]))

;;;

(definline stream?
  "Returns true if the object is a Manifold stream."
  [x]
  `(instance? IStream ~x))

(definline ^:private synchronous?
  "Returns true if the underlying abstraction behaves synchronously, using thread blocking
   to provide backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "manifold.stream.IStream"})))

(definline backpressure!
  "Sets whether backpressure is enabled on the event source."
  [source enabled?]
  `(.setBackpressure ~(with-meta source {:tag "manifold.stream.IEventSource"}) ~enabled?))

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

(defn put!
  "Puts a value into a stream, returning a promise that yields `true` if it succeeds,
   and `false` if it fails.  Guaranteed to be non-blocking."
  {:inline (fn [sink x]
             `(.put ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~x false))}
  ([^IEventSink sink x]
     (.put sink x false)))

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
  "Takes a value from a stream, returning a promise that yields the value when it
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
  "Takes a value from a stream, returning a promise that yields the value if it is
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

#_(defn- sync-connect
  [^IEventSource src
   ^CopyOnWriteArrayList dsts]
  (utils/defer
    (loop []
      (let [msg (.take src true ::closed)]
        (if (identical? ::closed msg)
          (if (if mapcat
                (loop [s (mapcat msg)]
                  (if (empty? s)
                    true
                    (if (.put dst msg true)
                      (recur (rest s))
                      false)))
                (.put dst msg true))
            (recur)
            (when upstream?
              (close! src))))))))

#_(defn connect
  ([^IEventSource src
    ^IEventSink dst
    {:keys [upstream?
            downstream?
            mapcat]
     :as options}]
     (if (or (synchronous? src) (synchronous? dst))
       (sync-connect src dst options))))

;;;

(deftype Production [promise token])
(deftype Consumption [message promise token])
(deftype Producer [message promise])
(deftype Consumer [promise default-val])

(defn- invoke-callbacks [^BlockingQueue callbacks]
  (loop []
    (when-let [c (.poll callbacks)]
      (try
        (c)
        (catch Throwable e
          ;; todo: log something
          )))))

(deftype Stream
  [
   lock

   ^BlockingQueue producers
   ^BlockingQueue consumers
   ^BlockingQueue messages

   ^BlockingQueue closed-callbacks
   ^BlockingQueue drained-callbacks
   ^:volatile-mutable closed?
   ]

  IStream

  (isSynchronous [_] false)

  IEventSink

  (close [this]
    (utils/with-lock lock
      (when-not closed?
        (set! closed? true)
        (let [l (java.util.ArrayList.)]
          (.drainTo consumers l)
          (doseq [^Consumer c l]
            (try
              (p/success! (.promise c) (.default-val c))
              (catch Throwable e
                ;; todo: log something
                ))))
        (invoke-callbacks closed-callbacks)
        (when (drained? this)
          (invoke-callbacks drained-callbacks)))))

  (onClosed [_ callback]
    (utils/with-lock lock
      (if closed?
        (callback)
        (.add closed-callbacks callback))))

  (onDrained [this callback]
    (utils/with-lock lock
      (if (drained? this)
        (callback)
        (.add drained-callbacks callback))))

  (isClosed [_]
    closed?)

  (isDrained [_]
    (and closed?
      (or (nil? messages) (nil? (.peek messages)))
      (nil? (.peek producers))))

  (put [_ msg blocking? timeout timeout-val]
    (let [result
          (utils/with-lock lock
            (or

              ;; closed, return << false >>
              (and closed?
                (p/success-promise false))

              ;; see if there are any unclaimed consumers left
              (loop [^Consumer c (.poll consumers 0 TimeUnit/NANOSECONDS)]
                (when c
                  (if-let [token (p/claim! (.promise c))]
                    (Production. (.promise c) token)
                    (recur (.poll consumers 0 TimeUnit/NANOSECONDS)))))

              ;; see if we can enqueue into the buffer
              (and
                messages
                (.offer messages msg 0 TimeUnit/NANOSECONDS)
                (p/success-promise true))

              ;; add to the producers queue
              (if (and timeout (<= timeout 0))
                (p/success-promise timeout-val)
                (let [p (p/promise)]
                  (when timeout
                    (time/in timeout #(p/success! p timeout-val)))
                  (let [pr (Producer. msg p)]
                    (if (.offer producers pr 0 TimeUnit/NANOSECONDS)
                      p
                      pr))))))]
      (cond
        (instance? Producer result)
        (do
          (.put producers result)
          (let [p (.promise ^Producer result)]
            (if blocking?
              @p
              p)))

        (instance? Production result)
        (let [^Production result result]
          (try
            (p/success! (.promise result) msg (.token result))
            (catch Throwable e
              ;; todo: log something
              ))
          (if blocking?
            true
            (p/success-promise true)))

        :else
        (if blocking?
          @result
          result))))

  (put [this msg blocking?]
    (.put ^IEventSink this  msg blocking? nil nil))

  IEventSource

  (setBackpressure [_ enabled?]
    ;; todo: if there's other means of backpressure, does this require an impl?
    )

  (take [this blocking? default-val timeout timeout-val]
    (let [result
          (utils/with-lock lock
            (or

              ;; see if we can dequeue from the buffer
              (when-let [msg (and messages (.poll messages 0 TimeUnit/NANOSECONDS))]

                ;; check if we're drained
                (when (and closed? (drained? this))
                  (invoke-callbacks drained-callbacks))

                (p/success-promise msg))

              ;; see if there are any unclaimed producers left
              (loop [^Producer p (.poll producers 0 TimeUnit/NANOSECONDS)]
                (when p
                  (if-let [token (p/claim! (.promise p))]
                    (let [c (Consumption. (.message p) (.promise p) token)]

                      ;; check if we're drained
                      (when (and closed? (drained? this))
                        (invoke-callbacks drained-callbacks))

                      c)
                    (recur (.poll producers 0 TimeUnit/NANOSECONDS)))))

              ;; closed, return << default-val >>
              (and closed?
                (p/success-promise default-val))

              ;; add to the consumers queue
              (if (and timeout (<= timeout 0))
                (p/success-promise timeout-val)
                (let [p (p/promise)]
                  (when timeout
                    (time/in timeout #(p/success! p timeout-val)))
                  (let [c (Consumer. p default-val)]
                    (if (.offer consumers c 0 TimeUnit/NANOSECONDS)
                      p
                      c))))))]

      (cond

        (instance? Consumer result)
        (do
          (.put consumers result)
          (let [p (.promise ^Consumer result)]
            (if blocking?
              @p
              p)))

        (instance? Consumption result)
        (let [^Consumption result result]
          (try
            (p/success! (.promise result) true (.token result))
            (catch Throwable e
              ;; todo: log something
              ))
          (let [msg (.message result)]
            (if blocking?
              msg
              (p/success-promise msg))))

        :else
        (if blocking?
          @result
          result))))

  (take [this blocking? default-val]
    (.take ^IEventSource this blocking? default-val nil nil))

  )

(defn stream
  "Returns a Manifold stream with a configurable `buffer-size`.  If a capacity is specified, `put!` will yield
   `true` when the message is in the buffer.  Otherwise it will only yield `true` once it has been consumed."
  ([]
     (Stream.
       (utils/mutex)
       (LinkedBlockingQueue. 1024)
       (LinkedBlockingQueue. 1024)
       nil
       (LinkedList.)
       (LinkedList.)
       false))
  ([buffer-size]
     (Stream.
       (utils/mutex)
       (LinkedBlockingQueue. 1024)
       (LinkedBlockingQueue. 1024)
       (ArrayBlockingQueue. buffer-size)
       (LinkedList.)
       (LinkedList.)
       false)))

;;;

(defn ->stream
  [x]
  (cond
    (instance? Stream x) x
    (streamable? x) (to-stream x)
    :else nil))

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

(defn- periodically-
  [stream period initial-delay f]
  (let [cancel (promise)]
    (deliver cancel
      (time/every period initial-delay
        (fn []
          (try
            (let [p (put! stream (f))]
              (if (realized? p)
                (when-not @p
                  (do
                    (@cancel)
                    (close! stream)))
                (do
                  (@cancel)
                  (p/chain p
                    (fn [x]
                      (if-not x
                        (close! stream)
                        (periodically- stream period (rem (System/currentTimeMillis) period) f)))))))
            (catch Throwable e
              (@cancel)
              (close! stream)
              ;; todo: add logging
              )))))))

(defn periodically
  "Creates a stream which emits the result of invoking `(f)` every `period` milliseconds."
  ([period initial-delay f]
     (let [s (stream 1)]
       (periodically- s period initial-delay f)
       s))
  ([period f]
     (periodically period (rem (System/currentTimeMillis) period) f)))

;;;

(utils/when-core-async
  (require 'manifold.stream.async))

(require 'manifold.stream.queue)
