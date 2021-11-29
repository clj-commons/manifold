(ns ^{:author "Ryan Smith"
      :doc    "Provide a variant of `core.async/go` that works with manifold's deferreds and executors. Utilizes core.async's state-machine generator, so core.async must be provided as a dependency."}
  manifold.go-off
  (:require [manifold
             [executor :as ex]
             [deferred :as d]]
            [clojure.core.async.impl
             [ioc-macros :as ioc]]
            [manifold.stream :as s])
  (:import (manifold.stream.core IEventSource)))

(defn return-deferred [state value]
  (let [d (ioc/aget-object state ioc/USER-START-IDX)]
    (d/success! d value)
    d))

(defn <!
  "Takes value from a deferred/stream. Must be called inside a (go ...) block. Will
   return nil if a stream is closed. Will park if nothing is available. If an error
   is thrown inside the body, that error will be placed as the return value.

   N.B. To make `go-off` usage idiomatic with the rest of manifold, use `<!?`
   instead."
  [port]
  (assert nil "<! used not in (go-off ...) block"))

(defmacro <!?
  "Takes a val from a deferred/stream. Must be called inside a (go-off ...) block.
   Will park if nothing is available. If value that is returned is a Throwable,
   it will re-throw."
  [port]
  `(let [r# (<! ~port)]
     (if (instance? Throwable r#)
       ;; this is a re-throw of the original throwable. the expectation is that
       ;; it still will maintain the original stack trace
       (throw r#)
       r#)))

(defn run-state-machine-wrapped [state]
  (try (ioc/run-state-machine state)
       (catch Throwable ex
         (d/error! (ioc/aget-object state ioc/USER-START-IDX) ex)
         (throw ex))))

(defn take! [state blk d]
  (let [handler          (fn [x]
                           (ioc/aset-all! state ioc/VALUE-IDX x ioc/STATE-IDX blk)
                           (run-state-machine-wrapped state))
        ;; if `d` is a stream, use `take` to get a deferrable that we can wait on
        d                (if (instance? IEventSource d) (s/take! d) d)
        d-is-deferrable? (d/deferrable? d)]
    (if
      ;; if d is not deferrable immediately resume processing state machine
      (not d-is-deferrable?)
      (do (ioc/aset-all! state ioc/VALUE-IDX d ioc/STATE-IDX blk)
          :recur)
      (let [d (d/->deferred d)]
        (if
          ;; if already realized, deref value and immediately resume processing state machine
          (d/realized? d)
          (do (ioc/aset-all! state ioc/VALUE-IDX @d ioc/STATE-IDX blk)
              :recur)

          ;; resume processing state machine once d has been realized
          (do (-> d
                  (d/chain handler)
                  (d/catch handler))
              nil))))))

(def async-custom-terminators
  {'manifold.go-off/<! `manifold.go-off/take!
   :Return             `return-deferred})

(defmacro go-off-executor
  "Implementation of go-off that allows specifying executor. See docstring of go-off for usage."
  [executor & body]
  (let [executor     (vary-meta executor assoc :tag 'java.util.concurrent.Executor)
        crossing-env (zipmap (keys &env) (repeatedly gensym))]
    `(let [d#                 (d/deferred)
           captured-bindings# (clojure.lang.Var/getThreadBindingFrame)]
       (.execute ~executor ^Runnable
                 (^:once fn* []
                   (let [~@(mapcat (fn [[l sym]] [sym `(^:once fn* [] ~(vary-meta l dissoc :tag))]) crossing-env)
                         f# ~(ioc/state-machine `(do ~@body) 1 [crossing-env &env] async-custom-terminators)
                         state# (-> (f#)
                                    (ioc/aset-all! ioc/USER-START-IDX d#
                                                   ioc/BINDINGS-IDX captured-bindings#))]
                     (run-state-machine-wrapped state#))))
       ;; chain is8 being used to apply unwrap chain
       (d/chain d#)))
  )

(defmacro go-off
  "Asynchronously executes the body on manifold's default executor, returning
   immediately to the calling thread. Additionally, any visible calls to <!?
   and <! deferred operations within the body will block (if necessary)
   by 'parking' the calling thread rather than tying up an OS thread.
   Upon completion of the operation, the body will be resumed.

   Returns a deferred which will receive the result of the body when
   completed. If the body returns a deferred, the result will be unwrapped
   until a non-deferrable value is available to be placed onto the return deferred.

   This method is intended to be similar to `core.async/go`, and even utilizes the
   underlying state machine-related functions from `core.async`. It's been designed
   to address the following major points from core.async & vanilla manifold deferreds:

   - `core.async/go` assumes that all of your code is able to be purely async
   and will never block the handling threads. go-off removes the concept of handling
   threads, which means blocking is not an issue, but if you spawn too many of these you
   can create too many threads for the OS to handle.
   - `core.async/go` has no built-in way of handling exceptions and assumes all async
   code will be either written defensively, or have custom error propagation, which
   differs from how clojure code blocks typically work outside of the async world.
   - `deferred/let-flow` presumes that every deferrable needs to be resolved. This prevents
   more complex handling of parallelism or being able to pass deferreds into other functions
   from within the `let-flow` block.
   - `deferred/chain` only works with single deferreds, which means having to write code in
   unnatural ways to handle multiple deferreds."
  [& body]
  `(go-off-executor (ex/execute-pool) ~@body))


(comment
  (go-off "cat")

  @(go-off (+ (<!? (d/future 10))
              (<!? (d/future 20))))                         ;; ==> 30

  )
