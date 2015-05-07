(ns tabby.core
  (:require [tabby.server :as server]
            [tabby.utils :as u]
            [tabby.cluster :as cluster]
            [tabby.local-net :as local-net])
  (:gen-class))

(defn -main [& args]
  (->
   (local-net/create-network-cluster 10 8090)
   (cluster/init-cluster 3)
   (cluster/start-cluster)))
