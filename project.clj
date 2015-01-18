(defproject tabby "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]
  :global-vars {*print-length* 4096
                *print-level* 10}
  :main ^:skip-aot tabby.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
