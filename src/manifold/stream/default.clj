(ns manifold.stream.default
  (:require
    [clojure.tools.logging :as log]
    [manifold.deferred :as d]
    [manifold.utils :as utils]
    [manifold.stream
     [graph :as g]
     [core :as s]]
    [manifold.time :as time])
  (:import
    [java.util
     LinkedList
     ArrayDeque
     Queue]
    [java.util.concurrent
     BlockingQueue
     ArrayBlockingQueue
     LinkedBlockingQueue]))

(set! *unchecked-math* true)

;;;

(deftype Production [deferred message token])
(deftype Consumption [message deferred token])
(deftype Producer [message deferred])
(deftype Consumer [deferred default-val])

(s/def-sink+source Stream
  [^boolean permanent?
   description
   ^LinkedList producers
   ^LinkedList consumers
   ^long capacity
   ^Queue messages
   executor
   add!]

  (isSynchronous [_] false)

  (description [this]
    (let [m {:type "manifold"
             :sink? true
             :source? true
             :pending-puts (.size producers)
             :buffer-capacity capacity
             :buffer-size (if messages (.size messages) 0)
             :pending-takes (.size consumers)
             :permanent? permanent?
             :closed? (s/closed? this)
             :drained? (s/drained? this)}]
      (if description
        (description m)
        m)))

  (close [this]
    (when-not permanent?
      (utils/with-lock lock
        (when-not (s/closed? this)

          (try
            (add!)
            (catch Throwable e
              (log/error e "error in stream transformer")))

          (loop []
            (when-let [^Consumer c (.poll consumers)]
              (try
                (d/success! (.deferred c) (.default-val c))
                (catch Throwable e
                  (log/error e "error in callback")))
              (recur)))

          (.markClosed this)

          (when (s/drained? this)
            (.markDrained this))))))

  (isDrained [this]
    (utils/with-lock lock
      (and (s/closed? this)
        (nil? (.peek producers))
        (or (nil? messages)
          (nil? (.peek messages))))))

  (put [this msg blocking? timeout timeout-val]
    (let [result
          (utils/with-lock lock
            (try
              (if (.isClosed this)
                (d/success-deferred false executor)
                (add! this msg))
              (catch Throwable e
                (.close this)
                (log/error e "error in stream transformer")
                (d/success-deferred false))))

          close?
          (reduced? result)

          result
          (if close?
            @result
            result)

          val
          (cond

            (instance? Producer result)
            (do
              (log/warn "excessive pending puts (> 16384), closing stream")
              (s/close! this)
              (d/success-deferred false executor))

            (instance? Production result)
            (let [^Production p result]
              (d/success! (.deferred p) (.message p) (.token p))
              (if blocking?
                true
                (d/success-deferred true executor)))

            (identical? this result)
            (d/success-deferred true executor)

            :else
            (do
              (d/timeout! result timeout timeout-val)
              (if blocking?
                @result
                result)))]

      (if close?
        (do
          (.close this)
          (d/success-deferred false executor))
        val)))

  (put [this msg blocking?]
    (.put this msg blocking? nil nil))

  (take [this default-val blocking? timeout timeout-val]
    (let [result
          (utils/with-lock lock
            (or

              ;; see if we can dequeue from the buffer
              (when-let [msg (and messages (.poll messages))]

                ;; check if we're drained
                (when (and (s/closed? this) (s/drained? this))
                  (.markDrained this))

                (if-let [^Producer p (.poll producers)]
                  (if-let [token (d/claim! (.deferred p))]
                    (do
                      (.offer messages (.message p))
                      (Consumption. msg (.deferred p) token))
                    (d/success-deferred msg executor))
                  (d/success-deferred msg executor)))

              ;; see if there are any unclaimed producers left
              (loop [^Producer p (.poll producers)]
                (when p
                  (if-let [token (d/claim! (.deferred p))]
                    (let [c (Consumption. (.message p) (.deferred p) token)]

                      ;; check if we're drained
                      (when (and (s/closed? this) (s/drained? this))
                        (.markDrained this))

                      c)
                    (recur (.poll producers)))))

              ;; closed, return << default-val >>
              (and (s/closed? this)
                (d/success-deferred default-val executor))

              ;; add to the consumers queue
              (if (and timeout (<= timeout 0))
                (d/success-deferred timeout-val executor)
                (let [d (d/deferred executor)]
                  (d/timeout! d timeout timeout-val)
                  (let [c (Consumer. d default-val)]
                    (if (and (< (.size consumers) 16384) (.offer consumers c))
                      d
                      c))))))]

      (cond

        (instance? Consumer result)
        (do
          (log/warn "excessive pending takes (> 16384), closing stream")
          (s/close! this)
          (d/success-deferred false executor))

        (instance? Consumption result)
        (let [^Consumption result result]
          (try
            (d/success! (.deferred result) true (.token result))
            (catch Throwable e
              (log/error e "error in callback")))
          (let [msg (.message result)]
            (if blocking?
              msg
              (d/success-deferred msg executor))))

        :else
        (if blocking?
          @result
          result))))

  (take [this default-val blocking?]
    (.take this default-val blocking? nil nil)))

(defn add!
  [^LinkedList producers
   ^LinkedList consumers
   ^Queue messages
   capacity
   executor]
  (let [capacity (long capacity)
        t-d (d/success-deferred true executor)]
    (fn
      ([]
         )
      ([_]
        (d/success-deferred false))
      ([_ msg]
        (or

           ;; see if there are any unclaimed consumers left
          (loop [^Consumer c (.poll consumers)]
            (when c
              (if-let [token (d/claim! (.deferred c))]
                (Production. (.deferred c) msg token)
                (recur (.poll consumers)))))

           ;; see if we can enqueue into the buffer
          (and
            messages
            (when (< (.size messages) capacity)
              (.offer messages msg))
            t-d)

           ;; add to the producers queue
          (let [d (d/deferred executor)]
            (let [pr (Producer. msg d)]
              (if (and (< (.size producers) 16384) (.offer producers pr))
                d
                pr))))))))

(defn stream
  ([]
    (stream 0 nil nil))
  ([buffer-size]
    (stream buffer-size nil nil))
  ([buffer-size xform]
    (stream buffer-size xform nil))
  ([buffer-size xform executor]
    (let [consumers    (LinkedList.)
          producers    (LinkedList.)
          buffer-size  (long (Math/max 0 (long buffer-size)))
          messages     (when (pos? buffer-size) (ArrayDeque.))
          add!         (add! producers consumers messages buffer-size executor)
          add!         (if xform (xform add!) add!)]
      (->Stream
        false
        nil
        producers
        consumers
        buffer-size
        messages
        executor
        add!))))

(defn stream*
  [{:keys [permanent?
           buffer-size
           description
           executor
           xform]
    :or {permanent? false}}]
  (let [consumers   (LinkedList.)
        producers   (LinkedList.)
        buffer-size (long (or buffer-size 0))
        messages    (when buffer-size (ArrayDeque.))
        buffer-size (if buffer-size (long (Math/max 0 buffer-size)) 0)
        add!        (add! producers consumers messages buffer-size executor)
        add!        (if xform (xform add!) add!)]
    (->Stream
      permanent?
      description
      producers
      consumers
      buffer-size
      messages
      executor
      add!)))
