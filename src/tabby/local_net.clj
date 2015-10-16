(ns tabby.local-net
  (:require [clojure.core.async :refer [close!]]
            [clojure.tools.logging :refer :all]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [tabby.cluster :as cluster]
            [tabby.net :as net]
            [tabby.server :as server]
            [tabby.utils :as utils]))

(defn- connect-to-peers [server]
  (let [peers (->> (map #(net/connect-to-peer3 @server %) (:peers @server))
                   (apply d/zip) ; multiple deferred's into one
                   (deref)
                   (into {}))]
    (swap! server assoc :peers peers)))

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
  (when-let [^java.io.Closeable s (:server-socket server)]
    (info "stopping server socket: " (:id server))
    (.close s))
  (when-let [e (:event-loop server)]
    (info "stopping event loop: " (:id server))
    (close! e))
  (dissoc server :event-loop :server-socket))

(defn- stop [state]
  (cluster/foreach-server state (fn [server]
                                  (if (instance? clojure.lang.Atom server)
                                    (swap! server stop-server)
                                    server))))

(defn- step [state dt]
  (cluster/foreach-server state swap! (partial server/update-state dt)))

(defn- assign-ports [servers base-port]
  (into {} (map-indexed (fn [i [key server]]
                          [key (-> server
                                   (assoc :port (+ i base-port))
                                   (assoc :hostname (str i ":" (+ i base-port))))]) servers)))

(defrecord LocalNetworkCluster
    [servers ^Long time ^Integer base-port ^Integer timeout]
  cluster/Cluster
  (init-cluster [this num]
    (-> (merge this (cluster/create base-port num))
        (update :servers assign-ports base-port)))

  (start-cluster [this]
    (start this))

  (kill-server [this id]
    (update-in this [:servers id] (fn [x]
                                    (if (instance? clojure.lang.Atom x)
                                      (swap! x stop-server)
                                      x))))
  (rez-server [this id]
    (update-in this [:servers id] (fn [x]
                                    (if (instance? clojure.lang.Atom x)
                                      x
                                      (connect  (start-server x))))))

  (stop-cluster [this]
    (stop this))

  (step-cluster [this dt]
    (step this dt)))

(defn create-network-cluster [timeout base-port]
  (map->LocalNetworkCluster {:timeout timeout :base-port base-port}))
