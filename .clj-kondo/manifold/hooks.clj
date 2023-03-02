(ns manifold.hooks
  (:require [clj-kondo.hooks-api :as api]))

(defn- cons-vector-node
  [node parent]
  (api/vector-node (cons node (:children parent))))

(defn def-sink-or-source [call]
  (let [[name bindings & body] (-> call :node :children rest)
        extended-bindings
        (cons-vector-node (api/token-node 'lock) bindings)]

    {:node
     (api/list-node
      (list
       (api/token-node 'do)

       (api/list-node
        (list*
         (api/token-node 'deftype)
         name
         extended-bindings
         body))

       (api/list-node
        (list
         (api/token-node 'defn)
         (api/token-node (symbol (str "->" (:string-value name))))
         bindings
         ))))}))
