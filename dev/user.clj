(ns user
  (:require [tabby.server :as server]
            [tabby.cluster :as cluster]
            [tabby.local-net :as local-net]
            [clojure.tools.namespace.repl :refer [refresh]]))

(def cluster-maker
  (partial local-net/create-network-cluster 10 8090))

(def cluster (cluster-maker))

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
  (refresh :after 'user/go))
