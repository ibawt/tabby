(defproject tabby "0.1.0-SNAPSHOT"
  :description "A raft implementation"
  :url "https://github.com/ibawt/tabby"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                [org.clojure/tools.cli "0.3.5"]
                [aleph "0.4.3"]
                [gloss "0.2.6"]
                [com.taoensso/nippy "2.13.0"]
                [log4j "1.2.17" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :main tabby.core
  :repl-options {:init-ns user}
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[jepsen "0.1.6-SNAPSHOT"]
                                [org.clojure/tools.namespace "0.2.11"]]
                   :source-paths ["dev"]}
             :test {:dependencies [[jepsen "0.1.6-SNAPSHOT"]]}})
