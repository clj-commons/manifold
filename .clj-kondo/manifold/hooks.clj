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
         bindings))))}))

(defn- seq-node? [node]
  (or (api/vector-node? node)
      (api/list-node? node)))

(defn- nth-child [node n] (nth (:children node) n))

(defn both [call]
  (let [body (-> call :node :children second :children)
        expand-nth
        (fn [n item]
          (if (and (seq-node? item) (= 'either (:value (nth-child item 0))))
            (:children (nth-child item n))
            [item]))]

    {:node
     (api/list-node
      (list
       (api/token-node 'do)

       (api/list-node
        (->> body (mapcat (partial expand-nth 1))))

       (api/list-node
        (->> body (mapcat (partial expand-nth 2))))))}))


(def fallback-value
  "The fallback value used for declaration of local variables whose
  values are unknown at lint time."
  (api/list-node
   (list
    (api/token-node 'new)
    (api/token-node 'java.lang.Object))))

(defn success-error-unrealized [call]

  (let [[deferred
         success-value success-clause
         error-value error-clause
         unrealized-clause] (-> call :node :children rest)]

    (when-not (and deferred success-value success-clause error-value
                   error-clause unrealized-clause)
      (throw (ex-info "Missing success-error-unrealized arguments" {})))

    {:node
     (api/list-node
      (list
       (api/token-node 'do)

       (api/list-node
        (list
         (api/token-node 'let)
         (api/vector-node (vector success-value fallback-value))
         success-clause))

       (api/list-node
        (list
         (api/token-node 'let)
         (api/vector-node (vector error-value fallback-value))
         error-clause))

       unrealized-clause))}))
