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
  (map (fn [x]
         (let [[_ host port id] (re-matches #"(.*):(\d+)=(\d+)" x)]
           {:host host :port (Integer/parseInt port) :id id}))
       (.split s ",")))

(def cli-options
  [["-p" "--port PORT" "Port number"
   :default 8080
    :parse-fn #(Integer/parseInt %)
    :validate-fn [#(and (> % 1024) (<  % 65535 ))]]
   ["-i" "--id ID" "Unique ID"]
   ["-P" "--peers PEERLIST"
    :parse-fn parse-peers]])

(defn- parse-args [args]
  (let [opts (cli/parse-opts args cli-options)]
    (:options opts)))

(defn -main [& args]
  (try
    (let [options (parse-args args)]
     (-> (server/create-server (:id options))
         (net/start-server! (:port options))
         (server/set-peers (:peers options))))
    (catch Exception e
      (warn e "main caught exception exiting..."))))
