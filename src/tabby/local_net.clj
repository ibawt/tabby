(ns tabby.local-net
  (:require [tabby.net :as net]
            [tabby.utils :as utils]
            [tabby.server :as server]
            [clojure.core.async :refer [close!]]
            [manifold.stream :as s]
            [tabby.cluster :as cluster]
            [clojure.tools.logging :refer :all]))

(defn- connect-to-peers [server]
  (doseq [peer (:peers @server)]
    (net/connect-to-peer server peer))
  server)

(defn- start-server [server & rest]
  (net/create-server server (:port server)))

(defn- connect [server]
  (connect-to-peers server)
  (swap! server assoc :event-loop (net/event-loop server (select-keys @server [:timeout])))
  server)

(defn- start
  [state]
  (-> (cluster/foreach-server state start-server)
      (cluster/foreach-server connect)))

(defn- stop-server [server]
  (doall
   (utils/mapf (:peer-sockets server) s/close!))
  (when-let [s (:server-socket server)]
    (info "stopping server socket: " (:id server))
    (.close s))
  (when-let [e (:event-loop server)]
    (info "stopping event loop: " (:id server))
    (close! e))
  (merge server {:event-loop nil :server-socket nil}))

(defn- stop [state]
  (cluster/foreach-server state (fn [server]
                                  (if (instance? clojure.lang.Atom server)
                                    (swap! server stop-server)
                                    server))))

(defn- step [state dt]
  (cluster/foreach-server state swap! (partial server/update dt)))

(defn- assign-ports [servers base-port]
  (into {} (map-indexed (fn [i [key server]]
                          [key (assoc server :port (+ i base-port))]) servers)))

(defrecord LocalNetworkCluster
    [servers ^Long time ^Integer base-port ^Integer timeout]
  cluster/Cluster
  (init-cluster [this num]
    (-> (merge this (cluster/create num))
        (update-in [:servers] assign-ports base-port)))

  (start-cluster [this]
    (start this))

  (stop-cluster [this]
    (stop this))

  (step-cluster [this dt]
    (step this dt)))

(defn create-network-cluster [timeout base-port]
  (map->LocalNetworkCluster {:timeout timeout :base-port base-port}))
