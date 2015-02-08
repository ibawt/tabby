(ns user
  (:require [tabby.server :as server]
            [tabby.cluster :as cl]
            [clojure.tools.namespace.repl :refer [refresh]]))

(def cluster nil)

(defn update-in-srv [id field f & args]
  (cl/update-in-srv cluster id field f args))

(defn init
  []
  (alter-var-root #'cluster (constantly
                             (cl/update-in-srv (cl/create 3)
                                               0 :election-timeout (constantly 0)))))

(defn step [dt]
  (alter-var-root #'cluster #(cl/step dt %)))

(defn srv [id]
  (get (:servers cluster) id))

(defn ps []
  (cl/ps cluster))

(defn servers []
  (:servers cluster))

(defn until-empty []
  (cl/until-empty cluster))

(defn start [])

(defn stop [])

(defn go
  []
  (init)
  (start)
  :ready)

(defn reset []
  (stop)
  (refresh :after 'user/go))
