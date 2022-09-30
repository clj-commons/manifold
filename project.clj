(defproject manifold "0.2.5-SNAPSHOT"
  :description "A compatibility layer for event-driven abstractions"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :url "https://github.com/clj-commons/manifold"
  :scm {:name "git" :url "https://github.com/clj-commons/manifold"}
  :dependencies [[org.clojure/clojure "1.11.0" :scope "provided"]
                 [org.clojure/tools.logging "1.1.0" :exclusions [org.clojure/clojure]]
                 [org.clj-commons/dirigiste "1.0.1"]
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
          :namespaces [manifold.deferred manifold.stream manifold.time manifold.bus manifold.executor manifold.go-off]}
  :global-vars {*warn-on-reflection* true}
  :jvm-opts ^:replace ["-server"
                       "-XX:-OmitStackTraceInFastThrow"
                       "-Xmx2g"
                       "-XX:NewSize=1g"]
  :javac-options ["-target" "1.8" "-source" "1.8"]

  :pom-addition ([:organization
                  [:name "CLJ Commons"]
                  [:url "http://clj-commons.org/"]]
                 [:developers [:developer
                               [:id "kingmob"]
                               [:name "Matthew Davidson"]
                               [:url "http://modulolotus.net"]
                               [:email "matthew@modulolotus.net"]]]))
