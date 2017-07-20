(ns tabby.local-net
  (:require [clojure.tools.logging :refer [warn info]]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [tabby.http-server :as http]
            [tabby.state-store :as store]
            [tabby.cluster :as cluster]
            [tabby.net :as net]
            [tabby.server :as server]
            [tabby.utils :as utils]
            [tabby.state-store :as store]))

(defn- connect-to-peers [server]
  (let [peers (->> (map #(net/connect-to-peer @server %) (:peers @server))
                   (apply d/zip) ; multiple deferred's into one
                   (deref)
                   (into {}))]
    (swap! server assoc :peers peers)))

(defn- start-server [server & rest]
  (net/create-server server (:port (if (instance? clojure.lang.Atom server)
                                     @server server))))

(defn- connect [server timeout]
  (swap! server assoc :event-loop
         (net/event-loop server timeout))
  server)

(defn- start
  [state]
  (-> (cluster/foreach-server state start-server)
      (cluster/foreach-server http/start!)
      (cluster/foreach-server (fn [s]
                                (connect s (:timeout state))))))



(defn- stop [state]
  (cluster/foreach-server state (fn [server]
                                  (if (instance? clojure.lang.Atom server)
                                    (do
                                      (http/stop! server)
                                      (swap! server net/stop-server))
                                    server))))

(defn- step [state dt]
  (cluster/foreach-server state swap! server/update-state dt))

(defn- assign-ports [servers base-port]
  (into {} (map-indexed (fn [i [key server]]
                          [key (-> server
                                   (assoc :port (+ i base-port)
                                          :http-port (+ i 9090))
                                   (assoc :hostname (str i ":" (+ i base-port))))]) servers)))

(defn- get-data-dir []
  (str (System/getenv "HOME") "/devel/tabby/tmp"))

(defn- add-data-dir [servers]
  (utils/mapf servers assoc :data-dir (get-data-dir)))

(defrecord LocalNetworkCluster
    [servers time base-port timeout]
  cluster/Cluster

  (init-cluster [this num]
    (-> (merge this (cluster/create base-port num))
        (update :servers assign-ports base-port)
        (update :servers add-data-dir)))

  (start-cluster [this]
    (start this))

  (kill-server [this id]
    (warn "Killing server: " id)
    (update-in this [:servers id] (fn [x]
                                    (swap! x net/stop-server)
                                    x)))
  (rez-server [this id]
    (warn "Rezing server: " id)
    (update-in this [:servers id] (fn [x]
                                    (swap! x store/restore)
                                    (connect (start-server x) (:timeout this))
                                    x)))

  (stop-cluster [this]
    (stop this))

  (step-cluster [this dt]
    (step this dt)))

(defn create-network-cluster [timeout base-port]
  (map->LocalNetworkCluster {:timeout timeout :base-port base-port}))
