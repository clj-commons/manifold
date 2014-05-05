(ns manifold.stream.graph
  (:require
    [manifold.deferred :as d]
    [manifold.utils :as utils]
    [manifold.stream :as s]
    [clojure.tools.logging :as log])
  (:import
    [java.util
     LinkedList]
    [java.lang.ref
     ReferenceQueue
     WeakReference]
    [java.util.concurrent
     ConcurrentHashMap
     CopyOnWriteArrayList]
    [manifold.stream
     IEventSink
     IEventSource]))

(def ^ReferenceQueue ^:private ref-queue (ReferenceQueue.))
(def ^ConcurrentHashMap graph (ConcurrentHashMap.))

(deftype Downstream
  [^long timeout
   ^boolean upstream?
   ^boolean downstream?
   ^IEventSink sink
   ^IEventSink sink'
   ^String description])

(deftype AsyncPut
  [deferred
   ^CopyOnWriteArrayList dsts
   dst
   ^boolean upstream?])

(defn downstream [source]
  (when-let [^CopyOnWriteArrayList dsts (.get graph (WeakReference. source))]
    (->> dsts
      .iterator
      iterator-seq
      (map (fn [^Downstream d]
             [(.description d) (or (.sink' d) (.sink d))])))))

(defn- async-send
  [^Downstream d msg dsts]
  (let [^IEventSink sink (.sink d)]
    (let [x (if (== (.timeout d) -1)
              (.put sink msg false)
              (.put sink msg false (.timeout d) (when (.downstream? d) d)))]
      (AsyncPut. x dsts d (.upstream? d)))))

(defn- sync-send
  [^Downstream d msg ^CopyOnWriteArrayList dsts ^IEventSink upstream]
  (let [^IEventSink sink (.sink d)
        x (if (== (.timeout d) -1)
            (.put sink msg true)
            (.put sink msg true (.timeout d) ::timeout))]
    (when (false? x)
      (.remove dsts d)
      (when upstream
        (s/close! upstream)))
    (when (identical? ::timeout x)
      (s/close! sink))))

(defn- handle-async-put [^AsyncPut x source]
  (let [d (.deferred x)]
    (try
      (let [x' @d]
        (cond
          (true? x')
          nil

          (false? x')
          (do
            (.remove ^CopyOnWriteArrayList (.dsts x) (.dst x))
            (when (.upstream? x)
              (s/close! source)))

          (instance? IEventSink x')
          (s/close! x')))
      (catch Throwable e
        (.remove ^CopyOnWriteArrayList (.dsts x) (.dst x))
        (when (.upstream? x)
          (s/close! source))))))

(defn- async-connect
  [^IEventSource source
   ^CopyOnWriteArrayList dsts]
  (let [sync-sinks (LinkedList.)
        deferreds  (LinkedList.)

        sync-propagate
        (fn this [recur-point msg]
          (loop []
            (let [^Downstream d (.poll sync-sinks)]
              (if (nil? d)
                recur-point
                (let [^AsyncPut x (async-send d msg dsts)
                      d (.deferred x)]
                  (if (d/realized? d)
                    (do
                      (handle-async-put x source)
                      (recur))
                    (d/on-realized d
                      (fn [_]
                        (handle-async-put x source)
                        (trampoline #(this recur-point msg)))
                      nil)))))))

        async-propagate
        (fn this [recur-point msg]
          (loop []
            (let [^AsyncPut x (.poll deferreds)]
              (if (nil? x)

                ;; iterator over sync-sinks
                (if (.isEmpty sync-sinks)
                  recur-point
                  #(sync-propagate recur-point msg))

                ;; iterate over async-sinks
                (let [d (.deferred x)]
                  (if (d/realized? d)
                    (do
                      (handle-async-put x source)
                      (recur))
                    (d/on-realized d
                      (fn [_]
                        (handle-async-put x source)
                        (trampoline #(this recur-point msg)))
                      nil)))))))

        weak-ref
        (WeakReference. source)

        err-callback
        (fn [err]
          (log/error err "error in source of 'connect'")
          (.remove graph weak-ref))]

    (trampoline
      (fn this
        ([]
           (let [d (.take source false ::drained)]
             (if (d/realized? d)
               (this @d)
               (d/on-realized d
                 (fn [msg] (trampoline #(this msg)))
                 err-callback))))
        ([msg]
           (cond

             (identical? ::drained msg)
             (do
               (.remove graph weak-ref)
               (let [i (.iterator dsts)]
                (loop []
                  (when (.hasNext i)
                    (let [^Downstream d (.next i)]
                      (s/close! (.sink d))
                      (recur))))))

             (== 1 (.size dsts))
             (try
               (let [dst (.get dsts 0)
                     ^AsyncPut x (async-send dst msg dsts)
                     d (.deferred x)]
                 (if (d/realized? d)
                   (do
                     (handle-async-put x source)
                     this)
                   (let [f (fn [_]
                             (handle-async-put x source)
                             (trampoline this))]
                     (d/on-realized d f f))))
               (catch IndexOutOfBoundsException e
                 (this msg)))

            :else
            (let [i (.iterator dsts)]
              (if (not (.hasNext i))
                (.remove graph weak-ref)

                (do
                  (utils/without-overflow
                    (loop []
                      (when (.hasNext i)
                        (let [^Downstream d (.next i)]
                          (if (s/synchronous? (.sink d))
                            (.add sync-sinks d)
                            (.add deferreds (async-send d msg dsts)))
                          (recur)))))

                  (async-propagate this msg))))))))))

(defn- sync-connect
  [^IEventSource source
   ^CopyOnWriteArrayList dsts]
  (utils/future
    (let [sync-sinks (LinkedList.)
          deferreds  (LinkedList.)]
      (loop []
        (let [i (.iterator dsts)]
          (if (.hasNext i)

            (let [msg (.take source true ::drained)]
              (if (identical? ::drained msg)

                (do
                  (.remove graph (WeakReference. source))
                  (doseq [^Downstream d (iterator-seq i)]
                    (when (.downstream? d)
                      (s/close! (.sink d)))))

                (do
                  (loop []
                    (when (.hasNext i)
                      (let [^Downstream d (.next i)]
                        (if (s/synchronous? (.sink d))
                          (.add sync-sinks d)
                          (.add deferreds (async-send d msg dsts)))
                        (recur))))

                  (loop []
                    (let [^AsyncPut x (.poll deferreds)]
                      (if (nil? x)
                        nil
                        (do
                          (handle-async-put x source)
                          (recur)))))

                  (loop []
                    (let [^Downstream d (.poll sync-sinks)]
                      (if (nil? d)
                        nil
                        (do
                          (sync-send d msg dsts (when (.upstream? d) source))
                          (recur)))))

                  (recur))))

            (do
              (s/close! source)
              (.remove graph (WeakReference. source)))))))))

(defn connect
  ([^IEventSource src
    ^IEventSink dst
    {:keys [upstream?
            downstream?
            dst'
            timeout
            description]
     :or {timeout -1
          upstream? false
          downstream? true}}]
     (locking src
       (let [d (Downstream.
                 timeout
                 upstream?
                 downstream?
                 dst
                 dst'
                 (or description ""))
             k (WeakReference. src ref-queue)]
         (if-let [dsts (.get graph k)]
           (.add ^CopyOnWriteArrayList dsts d)
           (let [dsts (CopyOnWriteArrayList.)]
             (if-let [dsts' (.putIfAbsent graph k dsts)]
               (.add ^CopyOnWriteArrayList dsts' d)
               (do
                 (.add ^CopyOnWriteArrayList dsts d)
                 (if (s/synchronous? src)
                   (sync-connect src dsts)
                   (async-connect src dsts))))))
        ))))
