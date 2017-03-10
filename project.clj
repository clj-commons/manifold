(defproject manifold "0.1.6"
  :description "a compatibility layer for event-driven abstractions"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :url "https://github.com/ztellman/manifold"
  :dependencies [[org.clojure/tools.logging "0.3.1" :exclusions [org.clojure/clojure]]
                 [io.aleph/dirigiste "0.1.5"]
                 [riddley "0.1.14"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]
                                  [criterium "0.4.4"]
                                  [org.clojure/core.async "0.2.385"]]}}
  :test-selectors {:default #(not
                               (some #{:benchmark :stress}
                                 (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)}
  :plugins [[lein-codox "0.9.4"]
            [lein-jammin "0.1.1"]
            [ztellman/lein-cljfmt "0.1.10"]]
  :cljfmt {:indents {#".*" [[:inner 0]]}}
  :codox {:source-uri "https://github.com/ztellman/manifold/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}
          :namespaces [manifold.deferred manifold.stream manifold.time manifold.bus manifold.executor]}
  :global-vars {*warn-on-reflection* true}
  :jvm-opts ^:replace ["-server"
                       "-XX:-OmitStackTraceInFastThrow"
                       "-XX:+UseConcMarkSweepGC"
                       "-Xmx2g"
                       "-XX:NewSize=1g"])
