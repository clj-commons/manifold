(defproject manifold "0.1.0-beta7"
  :description "a compatibility layer for event-driven abstractions"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :url "https://github.com/ztellman/manifold"
  :dependencies [[org.clojure/tools.logging "0.3.1"]
                 [riddley "0.1.7"]]
  :profiles {:dev {:dependencies [[codox-md "0.2.0" :exclusions [org.clojure/clojure]]
                                  [org.clojure/clojure "1.7.0-alpha3"]
                                  [criterium "0.4.3"]
                                  [org.clojure/core.async "0.1.319.0-6b1aca-alpha"]]}}
  :test-selectors {:default #(not (some #{:benchmark :stress}
                                        (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)}
  :plugins [[codox "0.6.4"]]
  :codox {:writer codox-md.writer/write-docs
          :include [manifold.deferred manifold.stream manifold.time manifold.bus]}
  :global-vars {*warn-on-reflection* true}
  :jvm-opts ["-server"
             "-XX:+UseConcMarkSweepGC"
             "-Xmx2g"
             "-XX:NewSize=1g"
             "-XX:MaxPermSize=256m"])
