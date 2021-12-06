(ns manifold.debug
  {:no-doc true})

(def ^:dynamic *dropped-error-logging-enabled?* true)

(defn enable-dropped-error-logging! []
  (.bindRoot #'*dropped-error-logging-enabled?* true))

(defn disable-dropped-error-logging! []
  (.bindRoot #'*dropped-error-logging-enabled?* false))
