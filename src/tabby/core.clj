(ns tabby.core
  (:require [tabby.cluster :as cluster]
            [tabby.local-net :as local-net]
            [clojure.tools.logging :refer [warn info]]
            [clojure.tools.cli :as cli]
            [tabby.net :as net]
            [tabby.server :as server]
            [tabby.utils :as u])
  (:gen-class))

(defn- parse-peers [s]
  (into {}
        (map
         (fn [x]
           (let [[_ host port id] (re-matches #"(.*):(\d+)=(\d+)" x)]
             [id {:hostname host :port (Integer/parseInt port)}]))
         (.split s ","))))

(def ^:private cli-options
  [["-p" "--port PORT" "Port number"
   :default 8080
    :parse-fn #(Integer/parseInt %)
    :validate-fn [#(and (> % 1024) (< % 65535))]]
   ["-i" "--id ID" "Unique ID"]
   ["-P" "--peers PEERLIST"
    :parse-fn parse-peers]
   ["-t" "--timeout" "event loop timeout"
    :default 50
    :parse-fn #(Integer/parseInt %)
    :validate-fn [pos?]]])

(defn- parse-args [args]
  (let [opts (cli/parse-opts args cli-options)]
    (:options opts)))

(defn -main [& args]
  (try
    (let [options (parse-args args)]
      (info "options: " options)
      (let [server (-> (server/create-server (:id options))
                      (server/set-peers (:peers options))
                      (net/create-server (:port options)))]

        (swap! server assoc :event-loop (net/event-loop server (:timeout options)))
        @(:event-loop @server)))
    (catch Exception e
      (warn e "main caught exception exiting...: " (.getMessage e)))))
