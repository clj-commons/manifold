(ns manifold.debug)

(def ^:dynamic *enabled?* false)

(defn enable! []
  (.bindRoot #'*enabled?* true))

(defn disable! []
  (.bindRoot #'*enabled?* false))
