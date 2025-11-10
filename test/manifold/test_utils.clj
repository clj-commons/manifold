(ns manifold.test-utils
  (:require
    [clojure.test :as test]
    [criterium.core :as c]
    [manifold.debug :as debug]))

(defmacro long-bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (c/bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(defmacro bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (c/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))
