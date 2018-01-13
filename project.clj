(defproject tabby "0.1.0-SNAPSHOT"
  :description "A raft backed key value store"
  :url "https://github.com/ibawt/tabby"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [compojure "1.6.0"]
                 [ring/ring-json "0.4.0"]
                 [clj-http "3.7.0"]
                 [cheshire "5.8.0"]
                 [aleph "0.4.4"]
                 [gloss "0.2.6"]
                 [com.taoensso/nippy "2.14.0"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :main tabby.core
  :repl-options {:init-ns user}
  :aliases {"jepsen" ["run" "-m" "jepsen.tabby"]}
  :profiles {:uberjar {:aot :all}
             :dev {:plugins [[lein-cloverage "1.0.10"]]
                   :dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [jepsen "0.1.7-SNAPSHOT"]]
                   :source-paths ["dev" "jepsen"]}
             :test {:dependencies [[lein-cloverage "1.0.10"]
                                   [jepsen "0.1.7-SNAPSHOT"]]
                    :source-paths ["jepsen"]}})
