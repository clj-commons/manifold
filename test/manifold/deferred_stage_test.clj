(ns manifold.deferred-stage-test
  (:require [manifold.deferred :as d]
            [clojure.test :refer [deftest is testing]])
  (:import [java.util.concurrent
            CompletionStage
            CompletableFuture
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

(defn fn->BiFunction [function]
  (reify java.util.function.BiFunction
    (apply [_ x y] (function x y))))

(defn fn->BiConsumer [function]
  (reify java.util.function.BiConsumer
    (accept [_ x y] (function x y))))

(defn fn->Runnable' [function]
  (reify java.lang.Runnable
    (run [_] (function nil nil))))

;; On these tests:
;; CompletionStage has many methods that mimic the chain, zip and alt
;; functions in manifold. Unfortunately, each of these has 3 different versions,
;; one for each java functional interaface, and each version has 3
;; variants/modes, a raw/same thread variant, an async variant which runs in
;; a separate thread when possible, and an async variant that runs
;; in a given executor.

(def functor-method-info
  [{:methods {:raw
              (fn [^CompletionStage this operator _]
                (.thenApply this operator))
              :async
              (fn [^CompletionStage this operator _]
                (.thenApplyAsync this operator))
              :with-executor
              (fn [^CompletionStage this operator executor]
                (.thenApplyAsync this operator executor))}

    :interface fn->Function
    :inner-assertion #(is (= "a test string" %))
    :post-assertion #(is (= true %))}

   {:methods {:raw (fn [^CompletionStage this operator _]
                     (.thenAccept this operator))
              :async (fn [^CompletionStage this operator _]
                       (.thenAcceptAsync this operator))
              :with-executor
              (fn [^CompletionStage this operator executor]
                (.thenAcceptAsync this operator executor))}
    :interface fn->Consumer
    :inner-assertion #(is (= % "a test string"))
    :post-assertion #(is (= % nil))}

   {:methods {:raw (fn [^CompletionStage this operator _]
                     (.thenRun this operator))
              :async (fn [^CompletionStage this operator _]
                       (.thenRunAsync this operator))
              :with-executor
              (fn [^CompletionStage this operator executor]
                (.thenRunAsync this operator executor))}
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

(def zip-method-info
  [{:methods {:raw
              (fn [^CompletionStage this other operator _]
                (.thenCombine this other operator))
              :async
              (fn [^CompletionStage this other operator _]
                (.thenCombineAsync this other operator))
              :with-executor
              (fn [^CompletionStage this other operator executor]
                (.thenCombineAsync this other operator executor))}
    :interface fn->BiFunction
    :inner-assertion (fn [_ _])
    :post-assertion #(is (= 2 %))}
   {:methods {:raw
              (fn [^CompletionStage this other operator _]
                (.thenAcceptBoth this other operator))
              :async
              (fn [^CompletionStage this other operator _]
                (.thenAcceptBothAsync this other operator))
              :with-executor
              (fn [^CompletionStage this other operator executor]
                (.thenAcceptBothAsync this other operator executor))}
    :interface fn->BiConsumer
    :inner-assertion (fn [x y] (is (= 1 x)) (is (= 1 y)))
    :post-assertion (fn [_])}
   {:methods {:raw
              (fn [^CompletionStage this other operator _]
                (.runAfterBoth this other operator))
              :async
              (fn [^CompletionStage this other operator _]
                (.runAfterBothAsync this other operator))
              :with-executor
              (fn [^CompletionStage this other operator executor]
                (.runAfterBothAsync this other operator executor))}
    :interface fn->Runnable'
    :inner-assertion (fn [_ _])
    :post-assertion (fn [_])}])

(defn- test-zip-success [method-info mode executor]

  (let [was-called (atom false)

        method (get-in method-info [:methods mode])
        {:keys [inner-assertion post-assertion]
         to-java-interface :interface} method-info

        d1 (d/success-deferred 1)
        d2 (d/success-deferred 1)
        d3 (method
            d1
            d2
            (to-java-interface
             (fn [x y]
               (inner-assertion x y)
               (reset! was-called true)
               (when (and x y) (+ x y))))
            executor)]

    (is (= @d1 1))
    (is (= @d2 1))
    (post-assertion @d3)
    (is (= true @was-called))))

(defn test-zip-error [method-info mode executor]

  (let [was-called (atom false)
        method (get-in method-info [:methods mode])
        {to-java-interface :interface} method-info

        d1 (d/error-deferred (RuntimeException.))
        d2 (d/error-deferred (RuntimeException.))
        d3 (method
            d1
            d2
            (to-java-interface
             (fn [_ _]
               (reset! was-called true)))
            executor)]

    (is (thrown? RuntimeException @d1))
    (is (thrown? RuntimeException @d2))
    (is (thrown? RuntimeException @d3))
    (is (= false @was-called))))


(deftest test-zip-methods

  (let [executor (Executors/newSingleThreadExecutor)]
    (testing "zip success"
      (dorun (for [method-info zip-method-info
                   mode [:raw :async :with-executor]]
               (test-zip-success method-info mode executor))))

    (testing "zip error"
      (dorun (for [method-info zip-method-info
                   mode [:raw :async :with-executor]]
               (test-zip-error method-info mode executor))))))

(def alt-method-info
  [{:methods {:raw
              (fn [^CompletionStage this other operator _]
                (.applyToEither this other operator))
              :async
              (fn [^CompletionStage this other operator _]
                (.applyToEitherAsync this other operator))
              :with-executor
              (fn [^CompletionStage this other operator executor]
                (.applyToEitherAsync this other operator executor))}
    :interface fn->Function
    :inner-assertion #(is (or (= % 1) (= % 2)))
    :post-assertion #(is (#{1 2} %))}

   {:methods {:raw
              (fn [^CompletionStage this other operator _]
                (.acceptEither this other operator))
              :async
              (fn [^CompletionStage this other operator _]
                (.acceptEitherAsync this other operator))
              :with-executor
              (fn [^CompletionStage this other operator executor]
                (.acceptEitherAsync this other operator executor))}
    :interface fn->Consumer
    :inner-assertion #(is (or (= % 1) (= % 2)))
    :post-assertion (fn [_])}

   {:methods {:raw
              (fn [^CompletionStage this other operator _]
                (.runAfterEither this other operator))
              :async
              (fn [^CompletionStage this other operator _]
                (.runAfterEitherAsync this other operator))
              :with-executor
              (fn [^CompletionStage this other operator executor]
                (.runAfterEitherAsync this other operator executor))}
    :interface fn->Runnable
    :inner-assertion (fn [_])
    :post-assertion (fn [_])}])

(defn- test-alt-success [method-info mode executor]

  (let [was-called (atom false)

        method (get-in method-info [:methods mode])
        {:keys [inner-assertion post-assertion]
         to-java-interface :interface} method-info

        d1 (d/success-deferred 1)
        d2 (d/success-deferred 2)
        d3 (method
            d1
            d2
            (to-java-interface
             (fn [x]
               (inner-assertion x)
               (reset! was-called true)
               x))
            executor)]

    (is (= @d1 1))
    (is (= @d2 2))
    (post-assertion @d3)
    (is (= true @was-called))))

(defn test-alt-error [method-info mode executor]

  (let [was-called (atom false)
        method (get-in method-info [:methods mode])
        {to-java-interface :interface} method-info

        d1 (d/error-deferred (RuntimeException.))
        d2 (d/error-deferred (RuntimeException.))
        d3 (method
            d1
            d2
            (to-java-interface
             (fn [_]
               (reset! was-called true)))
            executor)]

    (is (thrown? RuntimeException @d1))
    (is (thrown? RuntimeException @d2))
    (is (thrown? RuntimeException @d3))
    (is (= false @was-called))))

(deftest test-alt-methods

  (let [executor (Executors/newSingleThreadExecutor)]
    (testing "alt success"
      (dorun (for [method-info alt-method-info
                   mode [:raw :async :with-executor]]
               (test-alt-success method-info mode executor))))

    (testing "alt error"
      (dorun (for [method-info alt-method-info
                   mode [:raw :async :with-executor]]
               (test-alt-error method-info mode executor))))))



(def compose-method-info
  {:methods {:raw
              (fn [^CompletionStage this operator _]
                (.thenCompose this operator))
              :async
              (fn [^CompletionStage this operator _]
                (.thenComposeAsync this operator))
              :with-executor
              (fn [^CompletionStage this operator executor]
                (.thenComposeAsync this operator executor))}
    :interface fn->Function
    :inner-assertion #(is (= 1 %))
    :post-assertion #(is (= 2 %))})

(defn- test-compose-success [method-info mode executor]

  (let [was-called (atom false)

        method (get-in method-info [:methods mode])
        {:keys [inner-assertion post-assertion]
         to-java-interface :interface} method-info

        d1 (d/success-deferred 1)
        d2 (method
            d1
            (to-java-interface
             (fn [x]
               (inner-assertion x)
               (reset! was-called true)
               (d/success-deferred 2)))
            executor)]

    (is (= @d1 1))
    (post-assertion @d2)
    (is (= true @was-called))))


(deftest test-compose

  (let [executor (Executors/newSingleThreadExecutor)]
    (testing "compose success"
      (dorun (for [method-info [compose-method-info]
                   mode [:raw :async :with-executor]]
               (test-compose-success method-info mode executor))))

    (testing "compose error"
      (dorun (for [method-info [compose-method-info]
                   mode [:raw :async :with-executor]]
               (test-functor-error method-info mode executor))))))

(deftest test-compose-into-completable-future

  (testing "deferred can compose into CompletableFuture"
    (let [d1 ^CompletionStage (d/success-deferred 10)
          d2 (.thenCompose
              d1
              (fn->Function
               (fn [x] (CompletableFuture/completedFuture (inc x)))
               ))]
      (is (= @d2 11)))))
