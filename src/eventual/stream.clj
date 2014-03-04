(ns eventual.stream
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

;;;

(defprotocol Streamable
  (to-stream [_] "Provides a conversion mechanism to Eventual streams."))

(definterface IStream
  (isSynchronous [])
  (onClosed [callback]))

(definterface IEventSink
  (put [x])
  (put [x ^double timeout timeout-val]))

(definterface IEventSource
  (take [closed-val])
  (take [closed-val ^double timeout timeout-val])
  (setReadable [readable?])
  (connect [sink options]))

;;;

(definline stream?
  "Returns true if the object is an Eventual stream."
  [x]
  `(instance? IStream ~x))

(definline synchronous?
  "Returns true if the stream behaves synchronously, using thread blocking to provide
   backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "IStream"})))

(definline readable!
  "Sets whether the source is readable. A value of `false` implies that a backpressure
   mechanism should be engaged."
  [source readable?]
  `(.setReadable ~(with-meta source {:tag "IEventSource"}) ~readable?))

(let [^ConcurrentHashMap classes (ConcurrentHashMap.)]
  (add-watch #'Streamable ::memoization (fn [& _] (.clear classes)))
  (defn streamable?
    "Returns true if the object can be turned into an Eventual stream."
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
             `(.put ~(with-meta sink {:tag "IEventSink"}) ~x))}
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
                `(.put ~(with-meta sink {:tag "IEventSink"}) ~x ~timeout false))
             ([sink x timeout timeout-val]
                `(.put ~(with-meta sink {:tag "IEventSink"}) ~x ~timeout ~timeout-val)))}
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
                `(.take ~(with-meta source {:tag "IEventSource"}) nil))
             ([source default-val]
                `(.take ~(with-meta source {:tag "IEventSource"}) ~default-val)))}
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
                `(.take ~(with-meta source {:tag "IEventSource"}) nil ~timeout nil))
             ([source default-val timeout timeout-val]
                `(.take ~(with-meta source {:tag "IEventSource"}) ~default-val ~timeout ~timeout-val)))}
  ([^IEventSource source ^double timeout]
     (.take source nil timeout nil))
  ([^IEventSource source default-val ^double timeout timeout-val]
     (.take source default-val timeout timeout-val)))

;;;
