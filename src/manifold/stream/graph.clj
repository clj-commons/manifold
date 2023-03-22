(ns manifold.stream.graph
  {:no-doc true}
  (:require
    [manifold.deferred :as d]
    [manifold.utils :as utils :refer [deftype+]]
    [manifold.stream.core :as s]
    [manifold.executor :as ex]
    [clojure.tools.logging :as log])
  (:import
    [java.util
     LinkedList]
    [java.lang.ref
     WeakReference
     ReferenceQueue]
    [java.util.concurrent
     ConcurrentHashMap
     CopyOnWriteArrayList]
    [manifold.stream.core
     IEventStream
     IEventSink
     IEventSource]))

(def ^ReferenceQueue ^:private ref-queue (ReferenceQueue.))

;; a map of source handles onto a CopyOnWriteArrayList of sinks
(def ^ConcurrentHashMap handle->downstreams (ConcurrentHashMap.))

;; a map of stream handles onto other handles which have coupled life-cycles
(def ^ConcurrentHashMap handle->connected-handles (ConcurrentHashMap.))

(defn conj-to-list! [^ConcurrentHashMap m k x]
  (if-let [^CopyOnWriteArrayList l (.get m k)]
    (doto l (.add x))
    (let [l (CopyOnWriteArrayList.)
          l (or (.putIfAbsent m k l) l)]
      (doto ^CopyOnWriteArrayList l
        (.add x)))))

(deftype+ Downstream
  [^long timeout
   ^boolean upstream?
   ^boolean downstream?
   ^IEventSink sink
   ^String description])

(deftype+ AsyncPut
  [deferred
   ^CopyOnWriteArrayList dsts
   dst
   ^boolean upstream?])

;;;

(defn downstream [stream]
  (when-let [handle (s/weak-handle stream)]
    (when-let [^CopyOnWriteArrayList l (.get handle->downstreams handle)]
      (->> l
           .iterator
           iterator-seq
           (map
             (fn [^Downstream dwn]
               [(.description dwn) (.sink dwn)]))))))

(defn pop-connected! [stream]
  (when-let [handle (s/weak-handle stream)]
    (when-let [^CopyOnWriteArrayList l (.remove handle->connected-handles handle)]
      (->> l
           .iterator
           iterator-seq
           (map (fn [^WeakReference r] (.get r)))
           (remove nil?)))))

(defn add-connection! [a b]
  (conj-to-list! handle->connected-handles (s/weak-handle a) (s/weak-handle b)))

;;;

(defn- async-send
  [^Downstream dwn msg dsts]
  (let [^IEventSink sink (.sink dwn)]
    (let [x (if (== (.timeout dwn) -1)
              (.put sink msg false)
              (.put sink msg false (.timeout dwn) (if (.downstream? dwn) sink false)))]
      (AsyncPut. x dsts dwn (.upstream? dwn)))))

(defn- sync-send
  [^Downstream dwn msg ^CopyOnWriteArrayList dsts ^IEventSink upstream]
  (let [^IEventSink sink (.sink dwn)
        x                (try
                           (if (== (.timeout dwn) -1)
                             (.put sink msg true)
                             (.put sink msg true (.timeout dwn) ::timeout))
                           (catch Throwable e
                             (log/error e "error in message propagation")
                             (s/close! sink)
                             false))]
    (when (false? x)
      (.remove dsts dwn)
      (when upstream
        (s/close! upstream)))
    (when (and (identical? ::timeout x) (.downstream? dwn))
      (s/close! sink))))

(defn- handle-async-put [^AsyncPut x val source]
  (let [d   (.deferred x)
        val (if (instance? IEventSink val)
              (do
                (s/close! val)
                false)
              val)]
    (when (false? val)
      (let [^CopyOnWriteArrayList l (.dsts x)]
        (.remove l (.dst x))
        (when (or (.upstream? x) (== 0 (.size l)))
          (s/close! source)
          (.remove handle->downstreams (s/weak-handle source)))))))

(defn- handle-async-error [^AsyncPut x err source]
  (some-> ^Downstream (.dst x) .sink s/close!)
  (log/error err "error in message propagation")
  (let [^CopyOnWriteArrayList l (.dsts x)]
    (.remove l (.dst x))
    (when (or (.upstream? x) (== 0 (.size l)))
      (s/close! source)
      (.remove handle->downstreams (s/weak-handle source)))))

(defn- async-connect
  [^IEventSource source
   ^CopyOnWriteArrayList dsts]
  (let [sync-sinks      (LinkedList.)
        deferreds       (LinkedList.)

        sync-propagate  (fn this [recur-point msg]
                          (loop []
                            (let [^Downstream dwn (.poll sync-sinks)]
                              (if (nil? dwn)
                                recur-point
                                (let [^AsyncPut x (async-send dwn msg dsts)
                                      d           (.deferred x)
                                      val         (d/success-value d ::none)]
                                  (if (identical? val ::none)
                                    (d/on-realized d
                                                   (fn [v]
                                                     (handle-async-put x v source)
                                                     (trampoline #(this recur-point msg)))
                                                   (fn [e]
                                                     (handle-async-error x e source)
                                                     (trampoline #(this recur-point msg))))
                                    (do
                                      (handle-async-put x val source)
                                      (recur))))))))

        async-propagate (fn this [recur-point msg]
                          (loop []
                            (let [^AsyncPut x (.poll deferreds)]
                              (if (nil? x)

                                ;; iterator over sync-sinks
                                (if (.isEmpty sync-sinks)
                                  recur-point
                                  #(sync-propagate recur-point msg))

                                ;; iterate over async-sinks
                                (let [d   (.deferred x)
                                      val (d/success-value d ::none)]
                                  (if (identical? val ::none)
                                    (d/on-realized d
                                                   (fn [val]
                                                     (handle-async-put x val source)
                                                     (trampoline #(this recur-point msg)))
                                                   (fn [e]
                                                     (handle-async-error x e source)
                                                     (trampoline #(this recur-point msg))))
                                    (do
                                      (handle-async-put x val source)
                                      (recur))))))))

        err-callback    (fn [err]
                          (log/error err "error in source of 'connect'")
                          (.remove handle->downstreams (s/weak-handle source)))]

    (trampoline
      (fn this
        ([]
         (let [d (.take source ::drained false)]
           (if (d/realized? d)
             (this @d)
             (d/on-realized d
                            (fn [msg] (trampoline #(this msg)))
                            err-callback))))
        ([msg]
         (cond

           (identical? ::drained msg)
           (do
             (.remove handle->downstreams (s/weak-handle source))
             (let [i (.iterator dsts)]
               (loop []
                 (when (.hasNext i)
                   (let [^Downstream dwn (.next i)]
                     (when (.downstream? dwn)
                       (s/close! (.sink dwn)))
                     (recur))))))

           (== 1 (.size dsts))
           (try
             (let [dst         (.get dsts 0)
                   ^AsyncPut x (async-send dst msg dsts)
                   d           (.deferred x)
                   val         (d/success-value d ::none)]

               (if (identical? ::none val)
                 (d/on-realized d
                                (fn [val]
                                  (handle-async-put x val source)
                                  (trampoline this))
                                (fn [e]
                                  (handle-async-error x e source)
                                  (trampoline this)))
                 (do
                   (handle-async-put x val source)
                   this)))
             (catch IndexOutOfBoundsException e
               (this msg)))

           :else
           (let [i (.iterator dsts)]
             (if (not (.hasNext i))

               (do
                 (s/close! source)
                 (.remove handle->downstreams (s/weak-handle source)))

               (do
                 (loop []
                   (when (.hasNext i)
                     (let [^Downstream dwn (.next i)]
                       (if (s/synchronous? (.sink dwn))
                         (.add sync-sinks dwn)
                         (.add deferreds (async-send dwn msg dsts)))
                       (recur))))

                 (async-propagate this msg))))))))))

(defn- sync-connect
  [^IEventSource source
   ^CopyOnWriteArrayList dsts]
  (utils/future-with (ex/wait-pool)
    (let [sync-sinks (LinkedList.)
          deferreds  (LinkedList.)]
      (loop []
        (let [i (.iterator dsts)]
          (if (.hasNext i)

            (let [msg (.take source ::drained true)]
              (if (identical? ::drained msg)

                (do
                  (.remove handle->downstreams (s/weak-handle source))
                  (loop []
                    (when (.hasNext i)
                      (let [^Downstream dwn (.next i)]
                        (when (.downstream? dwn)
                          (s/close! (.sink dwn)))))))

                (do
                  (loop []
                    (when (.hasNext i)
                      (let [^Downstream dwn (.next i)]
                        (if (s/synchronous? (.sink dwn))
                          (.add sync-sinks dwn)
                          (.add deferreds (async-send dwn msg dsts)))
                        (recur))))

                  (loop []
                    (let [^AsyncPut x (.poll deferreds)]
                      (if (nil? x)
                        nil
                        (do
                          (try
                            (handle-async-put x @(.deferred x) source)
                            (catch Throwable e
                              (handle-async-error x e source)))
                          (recur)))))

                  (loop []
                    (let [^Downstream dwn (.poll sync-sinks)]
                      (if (nil? dwn)
                        nil
                        (do
                          (sync-send dwn msg dsts (when (.upstream? dwn) source))
                          (recur)))))

                  (recur))))

            (do
              (s/close! source)
              (.remove handle->downstreams (s/weak-handle source)))))))))

(defn connect
  ([^IEventSource src
    ^IEventSink dst
    {:keys [upstream?
            downstream?
            timeout
            description]
     :or   {timeout     -1
            upstream?   false
            downstream? true}
     :as   opts}]
   (locking src
     (let [dwn (Downstream.
                 timeout
                 (boolean (and upstream? (instance? IEventSink src)))
                 downstream?
                 dst
                 description)
           k (.weakHandle ^IEventStream src ref-queue)]
       (if-let [dsts (.get handle->downstreams k)]
         (.add ^CopyOnWriteArrayList dsts dwn)
         (let [dsts (CopyOnWriteArrayList.)]
           (if-let [dsts' (.putIfAbsent handle->downstreams k dsts)]
             (.add ^CopyOnWriteArrayList dsts' dwn)
             (do
               (.add ^CopyOnWriteArrayList dsts dwn)
               (if (s/synchronous? src)
                 (sync-connect src dsts)
                 (async-connect src dsts))))))))))
