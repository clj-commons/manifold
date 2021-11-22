(defproject manifold "0.2.0-SNAPSHOT"
  :description "A compatibility layer for event-driven abstractions"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :url "https://github.com/clj-commons/manifold"
  :scm {:name "git" :url "https://github.com/KingMob/manifold"}
  :dependencies [[org.clojure/clojure "1.10.3" :scope "provided"]
                 [org.clojure/tools.logging "1.1.0" :exclusions [org.clojure/clojure]]
                 [io.aleph/dirigiste "1.0.0"]
                 [riddley "0.1.15"]
                 [org.clojure/core.async "1.4.627" :scope "provided"]]
  :profiles {:dev {:dependencies [[criterium "0.4.6"]]}}
  :test-selectors {:default #(not
                               (some #{:benchmark :stress}
                                 (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)}
  :plugins [[lein-codox "0.10.7"]]
  :codox {:source-uri "https://github.com/clj-commons/manifold/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}
          :namespaces [manifold.deferred manifold.stream manifold.time manifold.bus manifold.executor]}
  :global-vars {*warn-on-reflection* true}
  :jvm-opts ^:replace ["-server"
                       "-XX:-OmitStackTraceInFastThrow"
                       "-Xmx2g"
                       "-XX:NewSize=1g"])
