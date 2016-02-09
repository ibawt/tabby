(defproject tabby "0.1.0-SNAPSHOT"
  :description "A raft implementation"
  :url "https://github.com/ibawt/tabby"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [aleph "0.4.1-beta2"]
                 [gloss "0.2.5"]
                 [com.taoensso/nippy "2.10.0"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :main tabby.core
  :repl-options {:init-ns user}
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]]
                   :source-paths ["dev"]}})
