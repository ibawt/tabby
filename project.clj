(defproject tabby "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [aleph "0.4.0-beta3"]
                 [gloss "0.2.4"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :global-vars {*print-length* 4096
                *print-level* 20}
  :main ^:skip-aot tabby.core
  :target-path "target/%s"

  :profiles {:uberjar {:aot :all}
             :dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/tools.namespace "0.2.9"]]}})
