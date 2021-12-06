(ns manifold.stream.default
  {:no-doc true}
  (:require
    [clojure.tools.logging :as log]
    [manifold
     [deferred :as d]
     [utils :as utils :refer [deftype+ defrecord+]]
     [executor :as ex]]
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
     LinkedBlockingQueue]
    [java.util.concurrent.atomic
     AtomicLong]))

(set! *unchecked-math* true)

;;;

(def max-dirty-takes "Every X takes, scan for timed-out deferreds and remove" 64)
(def max-dirty-puts "Every X puts, scan for timed-out deferreds and remove" 64)
(def max-consumers "Maximum number of pending consumers" 16384)
(def max-producers "Maximum number of pending producers" 16384)

(deftype+ Production [deferred message token])
(deftype+ Consumption [message deferred token])
(defrecord+ Producer [message deferred])
(defrecord+ Consumer [deferred default-val])

(defn de-nil [x]
  (if (nil? x)
    ::nil
    x))

(defn re-nil [x]
  (if (identical? ::nil x)
    nil
    x))

(defn- cleanup-expired-deferreds
  "Removes all realized deferreds (presumably from timing out)."
  [^LinkedList l]
  (locking l
    (when-not (.isEmpty l)
      (let [iter (.iterator l)]
        (loop [c (.next iter)]
          (when (-> c :deferred d/realized?)
            (.remove iter))
          (when (.hasNext iter)
            (recur (.next iter))))))))

(s/def-sink+source Stream
  [^boolean permanent?
   description
   ^LinkedList producers
   ^LinkedList consumers
   ^long capacity
   ^Queue messages
   executor
   add!
   ^AtomicLong dirty-takes]

  (isSynchronous [_] false)

  (description [this]
    (let [m {:type            "manifold"
             :sink?           true
             :source?         true
             :pending-puts    (.size producers)
             :buffer-capacity capacity
             :buffer-size     (if messages (.size messages) 0)
             :pending-takes   (.size consumers)
             :permanent?      permanent?
             :closed?         (s/closed? this)
             :drained?        (s/drained? this)}]
      (if description
        (description m)
        m)))

  (close [this]
    (when-not permanent?
      (utils/with-lock lock
        (when-not (s/closed? this)

          (try
            (let [acc    (LinkedList.)
                  result (try
                           (unreduced (add! acc))
                           (catch Throwable e
                             (log/error e "error in stream transformer")
                             false))]

              (loop []
                (if-not (.isEmpty acc)
                  (let [x (.removeFirst acc)]
                    (cond

                      (instance? Producer x)
                      (log/warn (IllegalStateException.) (format "excessive pending puts (> %d) while closing stream" max-producers))

                      (instance? Production x)
                      (let [^Production p x]
                        (d/success! (.deferred p) (.message p) (.token p))
                        (recur))

                      :else
                      (recur))))))

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
    (let [acc    (LinkedList.)

          result (utils/with-lock lock
                   (try
                     (if (.isClosed this)
                       false
                       (add! acc msg))
                     (catch Throwable e
                       (log/error e "error in stream transformer")
                       false)))

          close? (reduced? result)

          result (if close?
                   @result
                   result)

          val    (loop [val true]
                   (if (.isEmpty acc)
                     val
                     (let [x (.removeFirst acc)]
                       (cond

                         (instance? Producer x)
                         (do
                           (log/warn (IllegalStateException.) (format "excessive pending puts (> %d), closing stream" max-producers))
                           (s/close! this)
                           false)

                         (instance? Production x)
                         (let [^Production p x]
                           (d/success! (.deferred p) (.message p) (.token p))
                           (recur true))

                         :else
                         (do
                           (d/timeout! x timeout timeout-val)
                           (recur x))))))]

      (cond

        (or close? (false? result))
        (do
          (.close this)
          (d/success-deferred false executor))

        (d/deferred? val)
        val

        :else
        (d/success-deferred val executor))))

  (put [this msg blocking?]
    (.put this msg blocking? nil nil))

  (take [this default-val blocking? timeout timeout-val]
    (let [result
          (utils/with-lock lock
            (or

              ;; see if we can dequeue from the buffer
              (when-let [msg (and messages (.poll messages))]
                (let [msg (re-nil msg)]

                  ;; check if we're drained
                  (when (and (s/closed? this) (s/drained? this))
                    (.markDrained this))

                  (if-let [^Producer p (.poll producers)]
                    (if-let [token (d/claim! (.deferred p))]
                      (do
                        (.offer messages (de-nil (.message p)))
                        (Consumption. msg (.deferred p) token))
                      (d/success-deferred msg executor))
                    (d/success-deferred msg executor))))

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
                (do
                  (when (> (.getAndIncrement dirty-takes) max-dirty-takes)
                    (cleanup-expired-deferreds consumers)
                    (.set dirty-takes 0))
                  (let [d (d/deferred executor)]
                    (d/timeout! d timeout timeout-val)
                    (let [c (Consumer. d default-val)]
                      (if (and (< (.size consumers) max-consumers) (.offer consumers c))
                        d
                        c)))))))]

      (cond

        (instance? Consumer result)
        (do
          (log/warn (IllegalStateException.) (format "excessive pending takes (> %s), closing stream" max-consumers))
          (s/close! this)
          (d/success-deferred default-val executor))

        (instance? Consumption result)
        (let [^Consumption result result]
          (try
            (d/success! (.deferred result) true (.token result))
            (catch Throwable e
              (log/error e "error in callback")))
          (let [msg (re-nil (.message result))]
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
   executor
   ^AtomicLong dirty-puts]
  (let [capacity (long capacity)
        t-d      (d/success-deferred true executor)]
    (fn
      ([])
      ([acc] acc)
      ([^LinkedList acc msg]
       (doto acc
         (.add
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
                 (.offer messages (de-nil msg)))
               t-d)

             ;; add to the producers queue
             (do
               (when (> (.getAndIncrement dirty-puts) max-dirty-puts)
                 (cleanup-expired-deferreds producers)
                 (.set dirty-puts 0))
               (let [d (d/deferred executor)]
                 (let [pr (Producer. msg d)]
                   (if (and (< (.size producers) max-producers) (.offer producers pr))
                     d
                     pr)))))))))))

(defn stream
  ([]
   (stream 0 nil (ex/executor)))
  ([buffer-size]
   (stream buffer-size nil (ex/executor)))
  ([buffer-size xform]
   (stream buffer-size xform (ex/executor)))
  ([buffer-size xform executor]
   (let [consumers   (LinkedList.)
         producers   (LinkedList.)
         dirty-takes (AtomicLong.)
         dirty-puts  (AtomicLong.)
         buffer-size (long (Math/max 0 (long buffer-size)))
         messages    (when (pos? buffer-size) (ArrayDeque.))
         add!        (add! producers consumers messages buffer-size executor dirty-puts)
         add!        (if xform (xform add!) add!)]
     (->Stream
       false
       nil
       producers
       consumers
       buffer-size
       messages
       executor
       add!
       dirty-takes))))

(defn onto [ex s]
  (if (and (instance? Stream s) (identical? ex (.executor ^Stream s)))
    s
    (let [s' (stream 0 nil ex)]
      (g/connect s s' nil)
      s')))

(defn stream*
  [{:keys [permanent?
           buffer-size
           description
           executor
           xform]
    :or   {permanent? false
           executor   (ex/executor)}}]
  (let [consumers   (LinkedList.)
        producers   (LinkedList.)
        dirty-takes (AtomicLong.)
        dirty-puts  (AtomicLong.)
        buffer-size (long (or buffer-size 0))
        messages    (when buffer-size (ArrayDeque.))
        buffer-size (if buffer-size (long (Math/max 0 buffer-size)) 0)
        add!        (add! producers consumers messages buffer-size executor dirty-puts)
        add!        (if xform (xform add!) add!)]
    (->Stream
      permanent?
      description
      producers
      consumers
      buffer-size
      messages
      executor
      add!
      dirty-takes)))
