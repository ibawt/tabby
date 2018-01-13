(ns jepsen.tabby
  (:gen-class)
  (:require  [clojure.test :as t]
             [tabby.client :as client]
             [clojure.tools.logging :refer [warn info]]
             [knossos.model :as model]
             [jepsen [checker :as checker]
              [cli :as cli]
              [client :as jclient]
              [control :as c]
              [db :as db]
              [generator :as gen]
              [independent :as independent]
              [nemesis :as nemesis]
              [tests :as tests]
              [util :as util :refer [timeout]]]
             [jepsen.checker.timeline :as timeline]
             [jepsen.control.util :as cu]
             [jepsen.os.debian :as debian]
             [clojure.string :as str]))

(def dir "/opt/tabby")
(def data-dir "/opt/tabby/data")
(def binary "/usr/bin/java")
(def logfile (str dir "/tabby.log"))
(def pidfile (str dir "/tabby.pid"))

(defn node-url
  [node port]
  (str (name node) ":" port))

(defn peer-url [node]
  (node-url node 7659))

(defn client-url [nodes]
  (map (fn [node]
         {:host node :port 7659}) nodes))

(defn initial-cluster
  [test node]
  (->> (:nodes test)
       (filter (fn [n] (not= n node)))
       (map (fn [node] (str (peer-url node) "=" (name node))))
       (str/join ",")))

(defn db
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "installing tabby" version)
       (c/exec :/usr/sbin/update-java-alternatives :-s :java-1.8.0-openjdk-amd64)
       (c/upload "/tabby/tabby.tar.gz" "/tabby.tar.gz")
       (cu/install-archive! "file:///tabby.tar.gz" dir)
       (c/exec :mkdir :-p data-dir)
       (cu/start-daemon!
        {:logfile logfile
         :pidfile pidfile
         :chdir   dir}
        binary
        :-jar
        "/opt/tabby/tabby-0.1.0-SNAPSHOT-standalone.jar"
        :--id (name node)
        :--peers (initial-cluster test node)
        :--data-dir data-dir)))

    (teardown! [_ test node]
      (info node "tearing down tabby")
      (cu/stop-daemon! binary pidfile)
      (c/su
       (c/exec :rm :-rf data-dir)))

    db/LogFiles
    (log-files [_ test node]
      ["/opt/tabby/tabby.log" (str "/opt/tabby/" node "-log.bin")])))

(defn tclient
  [conn]
  (reify jclient/Client
    (setup! [_ test node]
      (tclient (client/make-network-client (client-url (:nodes test)))))

    (invoke! [this test op]
      (let [[k v] (:value op)
            crash (if (= :read (:f op)) :fail :info)]
        (try
          (case (:f op)
            :read (let [value (client/get-value! conn k)]
                    (warn "read" value)
                    (if (:value value)
                      (assoc op :type :ok :value (independent/tuple k (:value value)))
                      (assoc op :type :fail :error :not-found)))
            :write (let [value (client/set-value! conn k v)]
                     (warn "write" value)
                     (if (= :ok (:value value))
                       (assoc op :type :ok)
                       (assoc op :type :fail :errror :timeout)))
            :cas (let [[value value'] v
                       value'' (client/compare-and-swap! conn k value' value)]
                   (warn "cas" value'')
                   (if (= :ok (:value value''))
                     (assoc op :type :ok)
                     (assoc op :type :fail :error :not-found))))
          (catch java.lang.Exception e
            (assoc op :type crash :error :timeout)))))
    (teardown! [_ test]
      (client/close! conn))))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn tabby-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name "tabby"
          :os debian/os
          :db (db "0.1.0-SNAPSHOT")
          :client (tclient nil)
          :nemesis (nemesis/partition-random-halves)
          :model  (model/cas-register)
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :indep (independent/checker
                               (checker/compose
                                 {:timeline (timeline/html)
                                  :linear   (checker/linearizable)}))})
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w cas])
                                   (gen/stagger 1/30)
                                   (gen/limit 300))))
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
         opts))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn tabby-test})
                   (cli/serve-cmd))
            args))
