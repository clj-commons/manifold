(ns ^{:author "Ryan Smith"
      :doc    "Provide a variant of `core.async/go` that works with manifold's deferreds and executors. Utilizes core.async's state-machine generator, so core.async must be available as a dependency."}
  manifold.tsasvla
  (:require [manifold
             [executor :as ex]
             [deferred :as d]]
            [clojure.core.async.impl
             [ioc-macros :as ioc]]
            [manifold.stream :as s])
  (:import (java.util.concurrent Executor)
           (manifold.stream.core IEventSource)))

(defn return-deferred [state value]
  (let [d (ioc/aget-object state ioc/USER-START-IDX)]
    (d/success! d value)
    d))

(defn <!-no-throw
  "takes value from deferred. Must be called inside a (go ...) block. Will
  return nil if closed. Will park if nothing is available. If an error
  is thrown inside the body, that error will be placed as the return value.

  N.B. To make `tsasvla` usage idiomatic with the rest of manifold, use `<!?`
  instead of this directly."
  [port]
  (assert nil "<! used not in (tsasvla ...) block"))

(defmacro <!?
  "takes a val from port. Must be called inside a (tsasvla ...) block.
  Will park if nothing is available. If value that is returned is
  a Throwable, will re-throw."
  [port]
  `(let [r# (<!-no-throw ~port)]
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
  {'manifold.tsasvla/<!-no-throw `manifold.tsasvla/take!
   :Return                      `return-deferred})

(defmacro tsasvla-exeuctor
  "Implementation of tsasvla that allows specifying executor. See docstring of tsasvla for usage."
  [executor & body]
  (let [executor     (vary-meta executor assoc :tag 'Executor)
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

(defmacro tsasvla
  "წასვლა - Georgian for \"to go\"
  Asynchronously executes the body on manifold's default executor, returning
  immediately to the calling thread. Additionally, any visible calls to <!?
  and <!-no-throw deferred operations within the body will block (if necessary)
  by 'parking' the calling thread rather than tying up an OS thread.
  Upon completion of the operation, the body will be resumed.

  Returns a deferred which will receive the result of the body when
  completed. If the body returns a deferred, the result will be unwrapped
  until a non-deferable value is available to be placed onto the return deferred.

  This method is intended to be similar to `core.async/go`, and even utilizes the
  underlying state machine related functions from `core.async`. It's been designed
  to address the following major points from core.async & vanilla manifold deferreds:

  - `core.async/go` assumes that all of your code is able to be purely async
  and will never block the handling threads. Tsasvla removes the concept of handling
  threads, which means blocking is not an issue, but if you spawn too many of these you
  can create too many threads for the OS to handle.
  - `core.async/go` has absolutely no way of bubbling up exceptions and assumes all
  code will be defensively written, which differs from how clojure code blocks work
  outside of the async world.
  - `deferred/let-flow` presumes that every deferrable needs to be resolved. This prevents
  more complex handling of parallelism or being able to pass deferreds into other functions
  from within the `let-flow` block
  - `deferred/chain` only works with single deferreds, which means having to write code in
  unnatural ways to handle multiple deferreds."
  [& body]
  `(tsasvla-exeuctor (ex/execute-pool) ~@body))

(tsasvla "cat")

@(tsasvla (+ (<!? (d/future 10))
             (<!? (d/future 20))))                          ;; ==> 30
