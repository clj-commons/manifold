(ns manifold.debug
  {:no-doc true}
  (:require [clojure.tools.logging :as log]))

(def ^:dynamic *dropped-error-logging-enabled?* true)

(defn enable-dropped-error-logging! []
  (.bindRoot #'*dropped-error-logging-enabled?* true))

(defn disable-dropped-error-logging! []
  (.bindRoot #'*dropped-error-logging-enabled?* false))

(def ^:dynamic *leak-aware-deferred-rate* 1024)

(defn set-leak-aware-deferred-rate! [n]
  (.bindRoot #'*leak-aware-deferred-rate* n))

(def dropped-errors nil)

(defn log-dropped-error! [error]
  (some-> dropped-errors (swap! inc))
  (log/warn error "unconsumed deferred in error state, make sure you're using `catch`."))

(defn with-dropped-error-detection
  "Calls f, then attempts to trigger dropped errors to be detected and finally calls
  handle-dropped-errors with the number of detected dropped errors. Details about these are logged
  as warnings."
  [f handle-dropped-errors]
  (assert (nil? dropped-errors) "with-dropped-error-detection may not be nested")
  ;; Flush out any pending dropped errors from before
  (System/gc)
  (System/runFinalization)
  (with-redefs [dropped-errors (atom 0)]
    (f)
    ;; Flush out any errors which were dropped during f
    (System/gc)
    (System/runFinalization)
    (handle-dropped-errors @dropped-errors)))
