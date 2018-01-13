(ns tabby.core
  (:require [tabby.cluster :as cluster]
            [tabby.local-net :as local-net]
            [clojure.tools.logging :refer [warn info]]
            [clojure.tools.cli :as cli]
            [tabby.state-store :as store]
            [tabby.net :as net]
            [tabby.server :as server]
            [tabby.http-server :as http]
            [tabby.utils :as u])
  (:import [java.io File])
  (:gen-class))

(defn- parse-peers
  "parses a list of form x.y.z:323=1,y.a.b:32=2
   into a map like {1 {:hostname x.y.z :port 323}}"
  [^String s]
  (into {}
        (map
         (fn [x]
           (let [[_ host port id] (re-matches #"(.*):(\d+)=(.*)" x)]
             [id {:hostname host :port (Integer/parseInt port)}]))
         (.split s ","))))

(defn- dir-exists? [^String dir]
  (.isDirectory (File. dir)))

(def ^:private cli-options
  [["-p" "--port PORT" "Port number"
    :default 7659
    :parse-fn #(Integer/parseInt %)
    :validate-fn [#(and (> % 1024) (< % 65535))]]
   ["-i" "--id ID" "Unique ID"]
   ["-P" "--peers PEERLIST" "list of peers in form x.y.z:<port>=ID,..."
    :parse-fn parse-peers]
   ["-t" "--timeout" "event loop timeout"
    :default 10
    :parse-fn #(Integer/parseInt %)
    :validate-fn [pos?]]
   ["-d" "--data-dir DIR" "data directory"
    :validate-fn dir-exists?]
   ["-h" "--help" "show summary"]])

(defn filter-options
  "remove myself from the peer list"
  [options]
  (update options :peers
          (fn [x]
            (into {}
                  (filter (fn [[ k  v]]
                            (not= k (:id options)))
                          x)))))

(defn- parse-args [args]
  (let [{:keys [options errors summary] } (cli/parse-opts args cli-options)]
    (cond
      (:help options) (do (println summary) (System/exit 0))
      errors (do (println errors) (System/exit 1))
      :else options)))

(defn -main [& args]
  (try
    (let [options (filter-options (parse-args args))]
      (info "options: " options)
      (let [server (-> (server/create-server options)
                       (#(if (:data-dir %)
                           (store/restore %)
                           %))
                       (net/create-server (:port options))
                       (http/start!))]

        (swap! server assoc :event-loop (net/event-loop server (:timeout options)))
        @(:event-loop @server)))
    (catch Exception e
      (warn e "main caught exception exiting...: " (.getMessage e)))))
