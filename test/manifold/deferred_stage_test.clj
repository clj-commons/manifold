(ns manifold.deferred-stage-test
  (:require [manifold.deferred :as d]
            [clojure.test :refer [deftest is testing]])
  (:import [java.util.concurrent
            CompletionStage
            Executors]))

(defn fn->Function [function]
  (reify java.util.function.Function
    (apply [_ x] (function x))))

(defn fn->Consumer [function]
  (reify java.util.function.Consumer
    (accept [_ x] (function x))))

(defn fn->Runnable [function]
  (reify java.lang.Runnable
    (run [_] (function nil))))


;; On these tests:
;; CompletionStage has many methods that mimic the chain, zip and alt
;; functions in manifold. Unfortunately, each of these has 3 different versions,
;; one for each java functional interaface, and each version has 3
;; variants/modes, a raw/same thread variant, an async variant which runs in
;; a separate thread when possible, and an async variant that runs
;; in a given executor.

(def functor-method-info
  [{:methods {:raw (fn [d op _] (.thenApply ^CompletionStage d op))
              :async (fn [d op _] (.thenApplyAsync ^CompletionStage d op))
              :with-executor
              (fn [d op ex] (.thenApplyAsync ^CompletionStage d op ex))}

    :interface fn->Function
    :inner-assertion #(is (= "a test string" %))
    :post-assertion #(is (= true %))}

   {:methods {:raw (fn [d op _] (.thenAccept ^CompletionStage d op))
              :async (fn [d op _] (.thenAcceptAsync ^CompletionStage d op))
              :with-executor
              (fn [d op ex] (.thenAcceptAsync ^CompletionStage d op ex))}
    :interface fn->Consumer
    :inner-assertion #(is (= % "a test string"))
    :post-assertion #(is (= % nil))}

   {:methods {:raw (fn [d op _] (.thenRun ^CompletionStage d op))
              :async (fn [d op _] (.thenRunAsync ^CompletionStage d op))
              :with-executor
              (fn [d op ex] (.thenRunAsync ^CompletionStage d op ex))}
    :interface fn->Runnable
    :inner-assertion #(is (= % nil))
    :post-assertion #(is (= % nil))}])

(defn test-functor-success [method-info mode executor]

  (let [was-called (atom false)

        method (get-in method-info [:methods mode])
        {:keys [inner-assertion post-assertion]
         to-java-interface :interface} method-info

        d1 (d/success-deferred "a test string")
        d2 (method
            d1
            (to-java-interface
             (fn [x]
               (inner-assertion x)
               (reset! was-called true)
               (= x "a test string")))
            executor)]

    (is (= "a test string" @d1))
    (post-assertion @d2)
    (is (= true @was-called))))

(defn test-functor-error [method-info mode executor]

  (let [was-called (atom false)
        method (get-in method-info [:methods mode])
        {to-java-interface :interface} method-info

        d1 (d/error-deferred (RuntimeException.))
        d2 (method
            d1
            (to-java-interface
             (fn [_]
               (reset! was-called true)))
            executor)]

    (is (thrown? RuntimeException @d1))
    (is (thrown? RuntimeException @d2))
    (is (= false @was-called))))

(deftest test-functor-methods

  (let [executor (Executors/newSingleThreadExecutor)]
    (testing "functor success"
      (dorun (for [method-info functor-method-info
                   mode [:raw :async :with-executor]]
               (test-functor-success method-info mode executor))))

    (testing "functor error"
      (dorun (for [method-info functor-method-info
                   mode [:raw :async :with-executor]]
               (test-functor-error method-info mode executor))))))
