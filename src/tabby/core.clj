(ns tabby.core
  (:require [tabby.server :as server])
  (:gen-class))

(def cluster-states (atom nil))

(defmacro dbg [& body]
  `(let [x# ~body]
     (println (quote ~body) "=" x#) x#))

(defn create-system [num]
  (let [servers (vec (for [n (range num)] (server/create-server n)))]
    {:servers (mapv (fn [p] (server/set-peers p (mapv :id (filterv #(not= p %1) servers)))) servers)
     :time 0}))

(defn server-write [s kv]
  (loop [servers s
         out '()]
    (if (empty? servers)
      out
      (if (= :leader (:type (first servers)))
        (apply conj out (server/write (first servers) kv) (rest servers))
        (recur (rest servers) (conj out (first servers)))))))

(defn system-write [kv]
  (swap! cluster-states (fn [cs]
                          (update-in cs [:servers] server-write kv))))

(defn collect-packets [system id]
  (flatten (map (fn [s] (filter #(= (:dst %1) id) (:tx-queue s)))
                (:servers system))))

(defn collect-rx-packets [system]
  (assoc system :servers (map (fn [server]
                                (assoc server :rx-queue (concat (:rx-queue server) (collect-packets system (:id server)))))
                              (:servers system))))

(defn clear-tx-packets [system]
  (assoc system :servers (map (fn [server]
                                (assoc server :tx-queue '())) (:servers system))))

(defn pump-transmit-queues [system]
  (-> system
      (collect-rx-packets)
      (clear-tx-packets)))

(defn update-system [system dt]
  (-> system
      (pump-transmit-queues)
      (update-in [:servers]
                 (fn [servers] (mapv #(server/update % dt) servers)))
      (update-in [:time] + dt)))

(defn srv [id]
  (get (:servers @cluster-states) id))

(defn update-in-srv [id field f & args]
  (swap! cluster-states (fn [c]
                          (assoc c :servers (assoc (:servers c) id (update-in (srv id) [field] #(apply f % args)))))))

(defn update-srv [src id]
  (swap! cluster-states (fn [cs]
                          (assoc cs :servers (assoc (:servers cs) id src)))))

(defn servers []
  (:servers @cluster-states))

(defn queue-for [id]
  (select-keys (get (servers) id) [:tx-queue :rx-queue]))

(defn- print-fields [& rest]
  (map #(select-keys % (reverse rest)) (servers)))

(defn ps []
  (print-fields :id :type :election-timeout :current-term :commit-index))

(defn init []
  (reset! cluster-states (create-system 3))
  (ps))

(defn step [dt]
  (swap! cluster-states (fn [s] (update-system s dt)))
  (ps))
