(ns tabby.core
  (:require [tabby.server :as server])
  (:gen-class))

(def cluster-states (atom nil))

(defmacro dbg [& body]
  `(let [x# ~body]
     (println (quote body) "=" x#) x#))

(defn create-system [num]
  (let [servers (into [] (for [n (range num)] (atom (server/create-server n))))]
    (doall (map (fn [p] (server/set-peers p (filterv #(not= p %1) servers))) servers))
    {:servers servers :time 0}))

(defn update-system [system dt]
  (doseq [s (:servers system)] (server/update s dt))
  (update-in system [:time] + dt))

(defn- servers []
  (:servers @cluster-states))

(defn- print-fields [& rest]
  (map #(select-keys @% (reverse rest)) (servers)))

(defn ps []
  (print-fields :id :type :election-timeout :current-term :commit-index))

(defn init []
  (reset! cluster-states (create-system 3))
  (ps))

(defn step [dt]
  (swap! cluster-states (fn [s] (update-system s dt)))
  (ps))
