(ns manifold.stream.core
  (:require
    [clojure.tools.logging :as log]
    [manifold.deferred :as d]
    [manifold.utils :as utils]
    [manifold.stream :as s]
    [manifold.stream.graph :as g]
    [manifold.time :as time])
  (:import
    [java.util
     LinkedList]
    [java.lang.ref
     WeakReference]
    [java.util.concurrent
     BlockingQueue
     ArrayBlockingQueue
     LinkedBlockingQueue]
    [manifold.stream
     IEventSink
     IEventSource
     IStream]))

(deftype Production [deferred token])
(deftype Consumption [message deferred token])
(deftype Producer [message deferred])
(deftype Consumer [deferred default-val])

(defn- invoke-callbacks [^BlockingQueue callbacks]
  (loop []
    (when-let [c (.poll callbacks)]
      (try
        (c)
        (catch Throwable e
          (log/error e "error in callback"))))))

(deftype Stream
  [
   lock

   ^:volatile-mutable weak-handle

   ^boolean permanent?
   description

   ^BlockingQueue producers
   ^BlockingQueue consumers

   ^long capacity
   ^BlockingQueue messages

   ^BlockingQueue closed-callbacks
   ^BlockingQueue drained-callbacks
   ^:volatile-mutable closed?
   ]

  IStream

  (isSynchronous [_] false)

  (description [this]
    (let [m {:type "manifold"
             :pending-puts (.size producers)
             :buffer-capacity capacity
             :buffer-size (if messages (.size messages) 0)
             :pending-takes (.size consumers)
             :permanent? permanent?
             :closed? closed?
             :drained? (s/drained? this)}]
      (if description
        (description m)
        m)))

  IEventSink

  (downstream [this]
    (g/downstream this))

  (close [this]
    (when-not permanent?
      (utils/with-lock lock
        (when-not closed?
          (set! closed? true)
          (let [l (java.util.ArrayList.)]
            (.drainTo consumers l)
            (doseq [^Consumer c l]
              (try
                (d/success! (.deferred c) (.default-val c))
                (catch Throwable e
                  (log/error e "error in callback")))))
          (invoke-callbacks closed-callbacks)
          (when (s/drained? this)
            (invoke-callbacks drained-callbacks))))))

  (onClosed [_ callback]
    (utils/with-lock lock
      (if closed?
        (callback)
        (.add closed-callbacks callback))))

  (onDrained [this callback]
    (utils/with-lock lock
      (if (s/drained? this)
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
                (d/success-deferred false))

              ;; see if there are any unclaimed consumers left
              (loop [^Consumer c (.poll consumers)]
                (when c
                  (if-let [token (d/claim! (.deferred c))]
                    (Production. (.deferred c) token)
                    (recur (.poll consumers)))))

              ;; see if we can enqueue into the buffer
              (and
                messages
                (.offer messages msg)
                (d/success-deferred true))

              ;; add to the producers queue
              (if (and timeout (<= timeout 0))
                (d/success-deferred timeout-val)
                (let [d (d/deferred)]
                  (when timeout
                    (time/in timeout #(d/success! d timeout-val)))
                  (let [pr (Producer. msg d)]
                    (if (.offer producers pr)
                      d
                      pr))))))]
      (cond
        (instance? Producer result)
        (do
          (.put producers result)
          (let [d (.deferred ^Producer result)]
            (if blocking?
              @d
              d)))

        (instance? Production result)
        (let [^Production result result]
          (try
            (d/success! (.deferred result) msg (.token result))
            (catch Throwable e
              (log/error e "error in callback")))
          (if blocking?
            true
            (d/success-deferred true)))

        :else
        (if blocking?
          @result
          result))))

  (put [this msg blocking?]
    (.put ^IEventSink this msg blocking? nil nil))

  IEventSource

  (take [this blocking? default-val timeout timeout-val]
    (let [result
          (utils/with-lock lock
            (or

              ;; see if we can dequeue from the buffer
              (when-let [msg (and messages (.poll messages))]

                ;; check if we're drained
                (when (and closed? (s/drained? this))
                  (invoke-callbacks drained-callbacks))

                (d/success-deferred msg))

              ;; see if there are any unclaimed producers left
              (loop [^Producer p (.poll producers)]
                (when p
                  (if-let [token (d/claim! (.deferred p))]
                    (let [c (Consumption. (.message p) (.deferred p) token)]

                      ;; check if we're drained
                      (when (and closed? (s/drained? this))
                        (invoke-callbacks drained-callbacks))

                      c)
                    (recur (.poll producers)))))

              ;; closed, return << default-val >>
              (and closed?
                (d/success-deferred default-val))

              ;; add to the consumers queue
              (if (and timeout (<= timeout 0))
                (d/success-deferred timeout-val)
                (let [d (d/deferred)]
                  (when timeout
                    (time/in timeout #(d/success! d timeout-val)))
                  (let [c (Consumer. d default-val)]
                    (if (.offer consumers c)
                      d
                      c))))))]

      (cond

        (instance? Consumer result)
        (do
          (.put consumers result)
          (let [d (.deferred ^Consumer result)]
            (if blocking?
              @d
              d)))

        (instance? Consumption result)
        (let [^Consumption result result]
          (try
            (d/success! (.deferred result) true (.token result))
            (catch Throwable e
              (log/error e "error in callback")))
          (let [msg (.message result)]
            (if blocking?
              msg
              (d/success-deferred msg))))

        :else
        (if blocking?
          @result
          result))))

  (take [this blocking? default-val]
    (.take ^IEventSource this blocking? default-val nil nil))

  (connector [_ _]
    nil)

  (weakHandle [this reference-queue]
    (utils/with-lock lock
      (or weak-handle
        (do
          (set! weak-handle (WeakReference. this reference-queue))
          weak-handle)))))

(defn stream
  ([]
     (Stream.
       (utils/mutex)
       nil
       false
       nil
       (LinkedBlockingQueue. 65536)
       (LinkedBlockingQueue. 65536)
       0
       nil
       (LinkedBlockingQueue.)
       (LinkedBlockingQueue.)
       false))
  ([buffer-size]
     (Stream.
       (utils/mutex)
       nil
       false
       nil
       (LinkedBlockingQueue. 65536)
       (LinkedBlockingQueue. 65536)
       (long buffer-size)
       (LinkedBlockingQueue. (long buffer-size))
       (LinkedBlockingQueue.)
       (LinkedBlockingQueue.)
       false)))

(defn stream*
  [{:keys [permanent?
           buffer-size
           description]
    :or {permanent? false}}]
  (Stream.
    (utils/mutex)
    nil
    permanent?
    description
    (LinkedBlockingQueue. 65536)
    (LinkedBlockingQueue. 65536)
    (if buffer-size (long buffer-size) 0)
    (when buffer-size (LinkedBlockingQueue. (long buffer-size)))
    (LinkedBlockingQueue.)
    (LinkedBlockingQueue.)
    false))
