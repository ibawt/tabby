(ns tabby.core
  (:require [tabby.server :as server]
            [tabby.utils :as u]))

;; TODO: remove these things into some testing and dev deps
(def cluster-states (atom nil))

(def packet-loss (atom {}))

(defn create-system [num]
  (let [servers (vec (for [n (range num)] (server/create-server n)))]
    {:servers (mapv (fn [p] (server/set-peers
                             p (mapv :id (filterv #(not= p %1) servers)))) servers)
     :time 0}))

(defn server-write [s kv]
  (loop [servers s
         out '[]]
    (if (empty? servers)
      out
      (if (= :leader (:type (first servers)))
        (apply conj out (server/handle-write (first servers) kv) (rest servers))
        (recur (rest servers) (conj out (first servers)))))))

(defn add-packet-loss [from-id to-id]
  (swap! packet-loss update-in [from-id to-id] (constantly true)))

(defn clear-packet-loss []
  (reset! packet-loss {}))

(defn servers []
  (:servers @cluster-states))

(defn system-write [kv]
  (swap! cluster-states (fn [cs]
                          (update-in cs [:servers] server-write kv))))

(defn- valid-packet-for[id p]
  (and (= (:dst p) id) ; if it's for me
       (not (get (get @packet-loss id) (:src p)))))

(defn collect-packets [system id]
  (flatten (map (fn [s] (filter (partial valid-packet-for id) (:tx-queue s)))
                (:servers system))))

(defn collect-rx-packets [system]
  (assoc system :servers (map (fn [server]
                                (assoc server :rx-queue
                                       (concat (:rx-queue server)
                                               (collect-packets system (:id server)))))
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
  (first (filter #(= (:id %) id) (servers))))

(defn update-in-srv [id field f & args]
  (swap! cluster-states (fn [c]
                          (assoc c :servers
                                 (assoc (:servers c) id
                                        (update-in (srv id) [field]
                                                   #(apply f % args)))))))

(defn update-srv [src id]
  (swap! cluster-states (fn [cs]
                          (assoc cs :servers (assoc (:servers cs) id src)))))

(defn queue-for [id]
  (select-keys (get (servers) id) [:tx-queue :rx-queue]))

(defn- print-fields [& rest]
  (map #(select-keys % (reverse rest)) (servers)))

(defn ps []
  (print-fields :id :type :election-timeout :current-term :commit-index))

(defn init []
  (reset! packet-loss {})
  (reset! cluster-states (create-system 3))
  (update-in-srv 0 :election-timeout (constantly 0))
  (ps))

(defn step
  [dt]
  (swap! cluster-states (fn [s] (update-system s dt)))
  (ps))

(defn until-empty []
  (loop []
    (step 0)
    (when (> (reduce + (map server/packet-count (servers))) 0)
      (recur))))

(defn init-to-stable []
  (init)
  (until-empty))

(defn t []
  (init-to-stable)
  (add-packet-loss 1 0)
  (system-write {:a "a"})
  (until-empty)
  (update-in-srv 0 :election-timeout (constantly 300))
  (step 10))
