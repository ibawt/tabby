(ns user
  (:require [tabby.server :as server]
            [tabby.client :as client]
            [tabby.utils :as utils]
            [tabby.cluster :as cluster]
            [clojure.tools.logging :refer :all]
            [tabby.local-net :as local-net]
            [clojure.tools.namespace.repl :refer [refresh]]))

(def cluster-maker
  (partial local-net/create-network-cluster 10 8090))

(def cluster (cluster-maker))

(def klient (client/make-local-client [{:host "127.0.0.1" :port 8090}
                                       {:host "127.0.0.1" :port 8091}
                                       {:host "127.0.0.1" :port 8092}]))
(defn unatom [x]
  (if (instance? clojure.lang.Atom x)
    @x
    x))

(defmacro setc [& body]
  `(alter-var-root #'cluster
                   (fn ~@body)))

(defn init []
  (setc [c] (cluster/init-cluster c 3)))

(defn step [dt]
  (setc [c] (cluster/step-cluster c dt)))

(defn servers []
  (:servers cluster))

(defn start []
  (info "----------------------------------------------------")
  (setc [c] (cluster/start-cluster c)))

(defn stop []
  (setc [c] (cluster/stop-cluster c)))

(defn go
  []
  (init)
  (start)
  :ready)

(defn reset []
  (stop)
  (alter-var-root #'cluster (fn [s] (cluster-maker)))
  (refresh :after 'user/go))

(defn set-value [key value]
  (let [[kk value] @(utils/dbg client/set-or-create klient key value)]
    (alter-var-root #'klient (constantly kk))
    value))

(defn get-value [key]
  (let [[kk value] @(client/get-value klient key)]
    (alter-var-root #'klient (constantly kk))
    value))

(defn server-at [key]
  (unatom (get (:servers cluster) key)))

(defn find-leader []
  (reduce-kv (fn [_ k v]
               (if (= :leader (:type (if (instance? clojure.lang.Atom v) @v v)))
                 (reduced [k v]) _)) nil (:servers cluster)))
