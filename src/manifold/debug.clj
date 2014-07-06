(ns manifold.debug)

(def ^:private ^:dynamic *enabled?* false)

(defn enable! []
  (.bindRoot #'*enabled?* true))

(defn disable! []
  (.bindRoot #'*enabled?* false))
