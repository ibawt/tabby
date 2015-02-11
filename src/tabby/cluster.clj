(ns tabby.cluster
  (:require [tabby.server :as server]
            [tabby.utils :refer :all]))

;;; Testing and Development things for cluster testing
(defn- find-peers [id servers]
  (vec (keys (filterv (fn [[k v]] (not= k id)) servers))))

(defn- set-peers [servers]
  (mapf servers (fn [v]
                  (server/set-peers v (find-peers (:id v) servers)))))

(defn create [num]
  (let [servers (reduce #(merge %1 {%2 (server/create-server %2)}) {} (range num))]
    {:time 0
     :servers (set-peers servers)}))

(defn write [k cluster]
  (let [[id leader] (first (filter (fn [[k v]] (= :leader (:type v))) (:servers cluster)))]
    (update-in cluster [:servers id] (fn [s] (server/handle-write s k)))))

(defn add-packet-loss [from to cluster]
  (update-in cluster [:pkt-loss from to] (constantly true)))

(defn clear-packet-loss [cluster]
  (assoc cluster :pkt-loss {}))

(defn valid-packet-for [cluster id p]
  (and (= id (:dst p))
       (not (get-in cluster [:pkt-loss (:src p) (:dst p)]))))

(defn collect-packets [cluster id]
  (flatten (map (fn [[k s]]
                  (filter (partial valid-packet-for cluster id) (:tx-queue s))) (:servers cluster))))

(defn collect-rx-packets [system]
  (update-in system [:servers] mapf
             (fn [v]
               (update-in v [:rx-queue] concat (collect-packets system (:id v))))))

(defn clear-tx-packets [system]
  (update-in system [:servers] mapf assoc :tx-queue '()))

(defn pump-transmit-queues [system]
  (-> system
      (collect-rx-packets)
      (clear-tx-packets)))

(defn step [dt system]
  (-> system
      (pump-transmit-queues)
      (update-in [:servers] mapf (partial server/update dt))
      (update-in [:time] + dt)))

(defn step-times [dt times system]
  (loop [s system
         c (range times)]
    (if (empty? c)
      s
      (recur (step dt s)
             (rest c)))))

(defn srv [cluster id]
  (get (:servers cluster) id))

(defn update-in-srv [cluster id field f & args]
  (update-in cluster [:servers id field] f args))

(defn update-srv [cluster src id]
  (update-in cluster [:servers] assoc id src))

(defn queue-for [cluster id]
  (select-keys (get (:servers cluster) id) [:tx-queue :rx-queue]))

(defn print-fields [cluster & rest]
  (mapf (:servers cluster) #(select-keys % (reverse rest))))

(defn ps [cluster]
  (print-fields cluster :id :type :election-timeout :current-term :commit-index))

(defn until-empty [cluster]
  (loop [c (step 0 cluster)]
    (if (zero? (reduce + (for [[k v] (:servers c)] (server/packet-count v))))
      c
      (recur (step 0 c)))))
