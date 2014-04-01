(ns manifold.stream
  (:require
    [manifold.promise :as p]
    [manifold.utils :as utils]
    [manifold.time :as time])
  (:import
    [java.util.concurrent
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
  (isSynchronous [])
  (onClosed [callback])
  (isClosed []))

(definterface IEventSink
  (put [x])
  (put [x timeout timeout-val]))

(definterface IEventSource
  (take [default-val])
  (take [default-val timeout timeout-val])
  (setReadable [readable?])
  (connect [sink options]))

;;;

(definline stream?
  "Returns true if the object is an manifold stream."
  [x]
  `(instance? IStream ~x))

(definline synchronous?
  "Returns true if the stream behaves synchronously, using thread blocking to provide
   backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "manifold.stream.IStream"})))

(definline readable!
  "Sets whether the source is readable. A value of `false` implies that a backpressure
   mechanism should be engaged."
  [source readable?]
  `(.setReadable ~(with-meta source {:tag "manifold.stream.IEventSource"}) ~readable?))

(definline close!
  "Closes an event sink, so that it can't accept any more messages."
  [sink]
  `(.close ~(with-meta sink {:tag "java.io.Closeable"})))

(definline closed?
  "Returns true if the event sink is closed."
  [sink]
  `(.isClosed ~(with-meta sink {:tag "manifold.stream.IStream"})))

(let [^ConcurrentHashMap classes (ConcurrentHashMap.)]
  (add-watch #'Streamable ::memoization (fn [& _] (.clear classes)))
  (defn streamable?
    "Returns true if the object can be turned into an manifold stream."
    [x]
    (let [cls (class x)
          val (.get classes cls)]
      (if (nil? val)
        (let [val (satisfies? Streamable x)]
          (.put classes cls val)
          val)
        val))))

(defn put!
  "Puts a value into a stream, returning a promise that yields `true` if it succeeds,
   and `false` if it fails.  Guaranteed to be non-blocking."
  {:inline (fn [sink x]
             `(.put ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~x))}
  ([^IEventSink sink x]
     (.put sink x)))

(defn try-put!
  "Puts a value into a stream if the put can successfully be completed in `timeout`
   milliseconds.  Returns a promiise that yields `true` if it succeeds, and `false`
   if it fails or times out.  Guaranteed to be non-blocking.

   A special `timeout-val` may be specified, if it is important to differentiate
   between failure due to timeout and other failures."
  {:inline (fn
             ([sink x timeout]
                `(.put ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~x ~timeout false))
             ([sink x timeout timeout-val]
                `(.put ~(with-meta sink {:tag "manifold.stream.IEventSink"}) ~x ~timeout ~timeout-val)))}
  ([^IEventSink sink x ^double timeout]
     (.put sink x timeout false))
  ([^IEventSink sink x ^double timeout timeout-val]
     (.put sink x timeout timeout-val)))

(defn take!
  "Takes a value from a stream, returning a promise that yields the value when it
   is available, or `nil` if the take fails.  Guaranteed to be non-blocking.

   A special `default-val` may be specified, if it is important to differentiate
   between actual `nil` values and failures."
  {:inline (fn
             ([source]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) nil))
             ([source default-val]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) ~default-val)))}
  ([^IEventSource source]
     (.take source nil))
  ([^IEventSource source default-val]
     (.take source default-val)))

(defn try-take!
  "Takes a value from a stream, returning a promise that yields the value if it is
   available within `timeout` milliseconds, or `nil` if it fails or times out.
   Guaranteed to be non-blocking.

   Special `timeout-val` and `default-val` values may be specified, if it is
   important to differentiate between actual `nil` values and failures."
  {:inline (fn
             ([source timeout]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) nil ~timeout nil))
             ([source default-val timeout timeout-val]
                `(.take ~(with-meta source {:tag "manifold.stream.IEventSource"}) ~default-val ~timeout ~timeout-val)))}
  ([^IEventSource source ^double timeout]
     (.take source nil timeout nil))
  ([^IEventSource source default-val ^double timeout timeout-val]
     (.take source default-val timeout timeout-val)))

;;;

(deftype Production [promise token])
(deftype Consumption [message promise token])
(deftype Producer [message promise])
(deftype Consumer [promise default-val])

(deftype Stream
  [
   lock

   ^BlockingQueue producers
   ^BlockingQueue consumers
   ^BlockingQueue messages

   ^BlockingQueue closed-callbacks
   ^:volatile-mutable closed?
   ]

  java.io.Closeable
  (close [_]
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
        (doseq [c closed-callbacks]
          (try
            (c)
            (catch Throwable e
              ;; todo: log something
              ))))))

  IStream

  (isSynchronous [_] false)

  (onClosed [_ callback]
    (utils/with-lock lock
      (if closed?
        (callback)
        (.add closed-callbacks callback))))

  (isClosed [_]
    closed?)

  IEventSink

  (put [_ msg timeout timeout-val]
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
          (.promise ^Producer result))

        (instance? Production result)
        (let [^Production result result]
          (try
            (p/success! (.promise result) msg (.token result))
            (catch Throwable e
              ;; todo: log something
              ))
          (p/success-promise true))

        :else
        result)))

  (put [this msg]
    (.put ^IEventSink this msg nil nil))

  IEventSource

  (setReadable [_ readable?]
    ;; todo: if there's other means of backpressure, does this require an impl?
    )

  (take [_ default-val timeout timeout-val]
    (let [result
          (utils/with-lock lock
            (or

              ;; see if there are any unclaimed producers left
              (loop [^Producer p (.poll producers 0 TimeUnit/NANOSECONDS)]
                (when p
                  (if-let [token (p/claim! (.promise p))]
                    (Consumption. (.message p) (.promise p) token)
                    (recur (.poll producers 0 TimeUnit/NANOSECONDS)))))

              ;; see if we can dequeue from the buffer
              (when-let [msg (and messages (.poll messages 0 TimeUnit/NANOSECONDS))]
                (p/success-promise msg))

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
          (.promise ^Consumer result))

        (instance? Consumption result)
        (let [^Consumption result result]
          (try
            (p/success! (.promise result) true (.token result))
            (catch Throwable e
              ;; todo: log something
              ))
          (p/success-promise (.message result)))

        :else
        result)))

  (take [this default-val]
    (.take ^IEventSource this default-val nil nil))

  )

(defn stream
  "Returns an manifold stream with a configurable `buffer-size`.  If a capacity is specified, `put!` will yield
   `true` when the message is in the buffer.  Otherwise it will only yield `true` once it has been consumed."
  ([]
     (Stream.
       (utils/mutex)
       (LinkedBlockingQueue. 1024)
       (LinkedBlockingQueue. 1024)
       nil
       (LinkedList.)
       false))
  ([buffer-size]
     (Stream.
       (utils/mutex)
       (LinkedBlockingQueue. 1024)
       (LinkedBlockingQueue. 1024)
       (ArrayBlockingQueue. buffer-size)
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

(defn stream->seq
  "Returns a realized sequence of all messages that are immediately available from the stream."
  [s]
  (doall (stream->lazy-seq s 0)))

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

  (deftype CoreAsyncStream
   [ch
    ^:volatile-mutable closed?
    ^BlockingQueue close-callbacks]

   IStream
   (isSynchronous [_] false)
   (onClosed [this f]
     (locking this
       (if @closed?
         (f)
         (.add close-callbacks f))))
   (isClosed [_]
     @closed?)

   java.io.Closeable
   (close [this]
     (locking this
       (a/close! ch)
       (reset! closed? true)
       (let [l (java.util.ArrayList.)]
         (.drainTo close-callbacks l)
         (doseq [c l]
           (try
             (c)
             (catch Throwable e
               ;; todo: log something
               ))))))

   IEventSink
   (put [this x]
     (assert (not (nil? x)) "core.async channel cannot take `nil` as a message")
     (let [p (p/promise)]
       (a/go
         (>! ch x)
         (p/success! p true))
       p))
   (put [this x timeout timeout-val]
     (if (nil? timeout)
       (.put this x)
       (assert (not (nil? x)) "core.async channel cannot take `nil` as a message"))
     (let [p (p/promise)]
       (a/go
         (p/success! p
           (a/alt!
             [ch x] true
             (a/timeout timeout) timeout-val
             :priority true)))
       p))

   IEventSource
   (take [this default-val]
     (let [p (p/promise)]
       (a/go
         (p/success! p
           (let [x (<! ch)]
             (if (nil? x)
               default-val
               x))))
       p))
   (take [this default-val timeout timeout-val]
     (let [p (p/promise)]
       (a/go
         (p/success! p
           (a/alt!
             ch ([x] (if (nil? x) default-val x))
             (a/timeout timeout) timeout-val
             :priority true)))
       p))
   (setReadable [this readable?])
   (connect [this sink options]))

  (extend-protocol Streamable

    clojure.core.async.impl.channels.ManyToManyChannel
    (to-stream [ch]
      (CoreAsyncStream. ch false (LinkedBlockingQueue.)))))
