(defproject tabby "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [compojure "1.2.1"]
                 [ring/ring-json "0.3.1"]
                 [clj-http "1.0.1"]
                 [ring/ring-defaults "0.1.2"]
                 [ring-server "0.3.1"]]
  :main ^:skip-aot tabby.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
