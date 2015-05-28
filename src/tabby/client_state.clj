(ns tabby.client-state
  (:require [tabby.utils :refer :all]
            [tabby.log :as log]
            [clojure.tools.logging :refer :all]))

(defn create-client
  "puts a new client into the supplied clients map"
  [clients client-id socket]
  (assoc clients client-id
            {:pending-reads {}
             :pending-writes {}
             :history {}
             :socket socket}))

(defn needs-broadcast?
  "returns true if we are waiting for a broadcast"
  [clients]
  (reduce-kv (fn [_ k v]
               (if-not (empty? (:pending-reads v))
                 (reduced true)
                 false))
             false clients))

(defmacro update-map [m ks & args]
  `(mapf ~m update-in [~(first ks)] mapf (fn [r#] (update-in r# [~(second ks)] ~@args))))

(defn inc-heartbeats [clients src]
  (update-map clients [:pending-reads :hb-count] conj src))

(defn- fetch-reads [ready state]
  (into {} (for [[id r] ready]
             [id (assoc r :value (log/read-value state (:key r)))])))

(defn- transmit-reads [client-id state reads]
  (loop [s state
         r (seq reads)]
    (let [[id value] (first r)]
      (if (empty? r)
        s
        (recur (transmit s {:client-dst client-id :uuid id :value (:value value)})
               (rest r))))))

(defn- group-by-ready
  "given a client map and the peer count
  return a vector of [notready ready] reads"
  [{reads :pending-reads} peer-count]
  (let [{notready false ready true}
        (group-by (fn [[_ r]]
                    (quorum? peer-count (inc (count (:hb-count r)))))
                  reads)]
    [notready ready]))

(defn- perform-read [[client-id client] state]
  (let [[notready ready] (group-by-ready client (count (:peers state)))
        fetched-reads (fetch-reads ready state)
        c (-> (assoc client :pending-reads (into {} notready))
              (update-in [:history] merge fetched-reads))]

    [[client-id c] (transmit-reads client-id state fetched-reads)]))

(defn check-clients [state]
  (loop [c (seq (:clients state))
         s state
         out []]
    (if (empty? c)
      (assoc s :clients (into {} out))
      (let [[cc ss] (perform-read (first c) s)]
        (recur (rest c)
               ss
               (conj out cc))))))

(defn add-read
  "returns the update clients hash and either the response or a request
   to broadcast"
  [state pkt]
  (if-let [old-response (get-in (:clients state) [(:client-id pkt)
                                         :history (:uuid pkt)])]
    [(transmit state old-response)]
    [(assoc-in state [:clients (:client-id pkt) :pending-reads (:uuid pkt)]
               {:key (:key pkt) :hb-count #{}}) :broadcast-heart-beat]))

