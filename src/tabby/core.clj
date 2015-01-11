(ns tabby.core
  (:require [tabby.server :as server])
  (:gen-class))

(def ^:dynamic *cluster* nil)
(def base-port 3000)

(defn create-cluster [num]
  (alter-var-root #'*cluster*
                  (constantly (for [n (range num)]
                                (server/create-server n (+ base-port n))))))
(defn stop-cluster []
  (doseq [server *cluster*]
    (server/close-server server)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
