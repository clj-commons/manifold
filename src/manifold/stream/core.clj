(ns manifold.stream.core)

(defprotocol Sinkable
  (to-sink [_] "Provides a conversion mechanism to Manifold sinks."))

(defprotocol Sourceable
  (to-source [_] "Provides a conversion mechanism to Manifold source."))

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

(definline close!
  "Closes an event sink, so that it can't accept any more messages."
  [sink]
  `(let [^manifold.stream.core.IEventStream x# ~sink]
     (.close x#)))

(definline closed?
  "Returns true if the event sink is closed."
  [sink]
  `(.isClosed ~(with-meta sink {:tag "manifold.stream.core.IEventSink"})))

(definline drained?
  "Returns true if the event source is drained."
  [source]
  `(.isDrained ~(with-meta source {:tag "manifold.stream.core.IEventSource"})))

(definline weak-handle
  "Returns a weak reference that can be used to construct topologies of streams."
  [x]
  `(.weakHandle ~(with-meta x {:tag "manifold.stream.core.IEventStream"}) nil))

(definline synchronous?
  "Returns true if the underlying abstraction behaves synchronously, using thread blocking
   to provide backpressure."
  [x]
  `(.isSynchronous ~(with-meta x {:tag "manifold.stream.core.IEventStream"})))

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

;;;

(def ^:private default-stream-impls
  `((meta [_#] ~'__mta)
    (resetMeta [_ m#]
      (manifold.utils/with-lock* ~'lock
        (set! ~'__mta m#)))
    (alterMeta [_ f# args#]
      (manifold.utils/with-lock* ~'lock
        (set! ~'__mta (apply f# ~'__mta args#))))
    (~'downstream [this#] (manifold.stream.graph/downstream this#))
     (~'weakHandle [this# ref-queue#]
       (manifold.utils/with-lock ~'lock
         (or ~'__weakHandle
           (set! ~'__weakHandle (java.lang.ref.WeakReference. this# ref-queue#)))))
     (~'close [this#])))

(def ^:private sink-params
  '[lock
    ^:volatile-mutable __mta
    ^:volatile-mutable __isClosed
    ^java.util.LinkedList __closedCallbacks
    ^:volatile-mutable __weakHandle
    ^:volatile-mutable __mta])

(def ^:private default-sink-impls
  `[(~'close [this#] (.markClosed this#))
    (~'isClosed [this#] ~'__isClosed)
    (~'onClosed [this# callback#]
      (manifold.utils/with-lock ~'lock
        (if ~'__isClosed
          (callback#)
          (.add ~'__closedCallbacks callback#))))
    (~'markClosed [this#]
      (manifold.utils/with-lock ~'lock
        (set! ~'__isClosed true)
        (manifold.utils/invoke-callbacks ~'__closedCallbacks)))])

(def ^:private source-params
  '[lock
    ^:volatile-mutable __mta
    ^:volatile-mutable __isDrained
    ^java.util.LinkedList __drainedCallbacks
    ^:volatile-mutable __weakHandle])

(def ^:private default-source-impls
  `[(~'isDrained [this#] ~'__isDrained)
    (~'onDrained [this# callback#]
      (manifold.utils/with-lock ~'lock
        (if ~'__isDrained
          (callback#)
          (.add ~'__drainedCallbacks callback#))))
    (~'markDrained [this#]
      (manifold.utils/with-lock ~'lock
        (set! ~'__isDrained true)
        (manifold.utils/invoke-callbacks ~'__drainedCallbacks)))
    (~'connector [this# _#] nil)])

(defn- merged-body [& bodies]
  (let [bs (apply concat bodies)]
    (->> bs
      (map #(vector [(first %) (count (second %))] %))
      (into {})
      vals)))

(defmacro def-source [name params & body]
  `(do
     (deftype ~name
       ~(vec (distinct (concat params source-params)))
       manifold.stream.core.IEventStream
       manifold.stream.core.IEventSource
       clojure.lang.IReference
       ~@(merged-body default-stream-impls default-source-impls body))

     (defn ~(with-meta (symbol (str "->" name)) {:private true})
       [~@(map #(with-meta % nil) params)]
       (new ~name ~@params (manifold.utils/mutex) nil false (java.util.LinkedList.) nil))))

(defmacro def-sink [name params & body]
  `(do
     (deftype ~name
       ~(vec (distinct (concat params sink-params)))
       manifold.stream.core.IEventStream
       manifold.stream.core.IEventSink
       clojure.lang.IReference
       ~@(merged-body default-stream-impls default-sink-impls body))

     (defn ~(with-meta (symbol (str "->" name)) {:private true})
       [~@(map #(with-meta % nil) params)]
       (new ~name ~@params (manifold.utils/mutex) nil false (java.util.LinkedList.) nil))))

(defmacro def-sink+source [name params & body]
  `(do
     (deftype ~name
       ~(vec (distinct (concat params source-params sink-params)))
       manifold.stream.core.IEventStream
       manifold.stream.core.IEventSink
       manifold.stream.core.IEventSource
       clojure.lang.IReference
       ~@(merged-body default-stream-impls default-sink-impls default-source-impls body))

     (defn ~(with-meta (symbol (str "->" name)) {:private true})
       [~@(map #(with-meta % nil) params)]
       (new ~name ~@params (manifold.utils/mutex) nil false (java.util.LinkedList.) nil false (java.util.LinkedList.)))))
