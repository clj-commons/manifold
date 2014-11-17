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
     LinkedList
     ArrayDeque]
    [java.util.concurrent
     BlockingQueue
     ArrayBlockingQueue
     LinkedBlockingQueue]))

;;;

(deftype Production [deferred token])
(deftype Consumption [message deferred token])
(deftype Producer [message deferred])
(deftype Consumer [deferred default-val])

(s/def-sink+source Stream
  [
   ^boolean permanent?
   description
   ^LinkedList producers
   ^LinkedList consumers
   ^long capacity
   ^ArrayDeque messages
   add!
   ]

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
          (loop []
            (when-let [^Consumer c (.poll consumers)]
              (try
                (d/success! (.deferred c) (.default-val c))
                (catch Throwable e
                  (log/error e "error in callback")))))
          (.markClosed this)
          (when (s/drained? this)
            (.markDrained this))))))

  (isDrained [this]
    (utils/with-lock lock
      (and (s/closed? this)
        (or (nil? messages)
          (nil? (.peek messages))))))

  (put [this msg blocking? timeout timeout-val]
    (let [result (utils/with-lock lock
                   (try
                     (add! this msg)
                     (catch Throwable e
                       (d/error-deferred e)
                       (.close this))))]
      (cond
        (instance? Producer result)
        (do
          (.add producers result)
          (let [d (.deferred ^Producer result)]
            (when timeout
              (time/in timeout #(d/success! d timeout-val)))
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

        (identical? this result)
        (d/success-deferred true)

        (reduced? result)
        (do
          (.close this)
          (d/success-deferred false))

        :else
        (do
          (when timeout
            (time/in timeout #(d/success! result timeout-val)))
          (if blocking?
            @result
            result)))))

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
                    (d/success-deferred msg))
                  (d/success-deferred msg)))

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
          (.add consumers result)
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

  (take [this default-val blocking?]
    (.take this default-val blocking? nil nil)))

(defn add!
  [^LinkedList producers
   ^LinkedList consumers
   ^ArrayDeque messages
   capacity
   this]
  (let [capacity (long capacity)]
    (fn
      ([_]
         (d/success-deferred false))
      ([_ msg]
         (or

           ;; closed, return << false >>
           (and (s/closed? @this)
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
             (when (< (.size messages) capacity)
               (.offer messages msg))
             (d/success-deferred true))

           ;; add to the producers queue
           (let [d (d/deferred)]
             (let [pr (Producer. msg d)]
               (if (.offer producers pr)
                 d
                 pr))))))))

(defn stream
  ([]
     (stream 0))
  ([buffer-size]
     (stream buffer-size nil))
  ([buffer-size xform]
     (let [consumers    (LinkedList.)
           producers    (LinkedList.)
           buffer-size  (long (max 0 buffer-size))
           messages     (when (pos? buffer-size) (ArrayDeque.))
           this         (promise)
           add!         (add! producers consumers messages buffer-size this)
           add!         (if xform (xform add!) add!)]
       @(deliver this
          (->Stream
            false
            nil
            producers
            consumers
            buffer-size
            messages
            add!)))))

(defn stream*
  [{:keys [permanent?
           buffer-size
           description
           xform]
    :or {permanent? false}}]
  (let [consumers   (LinkedList.)
        producers   (LinkedList.)
        messages    (when buffer-size (ArrayDeque.))
        buffer-size (if buffer-size (long (max 0 buffer-size)) 0)
        this        (promise)
        add!        (add! producers consumers messages buffer-size this)
        add!        (if xform (xform add!) add!)]
    @(deliver this
       (->Stream
         permanent?
         description
         producers
         consumers
         buffer-size
         messages
         add!))))
