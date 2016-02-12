(ns tabby.client-state
  (:require [clojure.tools.logging :refer [warn info]]
            [manifold.stream :as s]
            [tabby.leader :as l]
            [tabby.log :as log]
            [tabby.utils :as utils]))

(defn create-client
  "puts a new client into the supplied clients map"
  [clients client-id socket]
  (assoc clients client-id
            {:pending-read {}
             :pending-write {}
             :history {}
             :socket socket}))

(defn needs-broadcast?
  "returns true if we are waiting for a broadcast"
  [clients]
  (reduce-kv (fn [_ k v]
               (if-not (empty? (:pending-read v))
                 (reduced true)
                 false))
             false clients))

(def has-reads? (comp not empty? :pending-read))

(defn inc-heartbeats [clients src]
  (utils/mapf clients (fn [client]
                        (if (has-reads? client)
                          (update-in client [:pending-read :hb-count] conj src)
                          client))))

(defn- handle-read-cas
  [state client-id]
  (let [client (get-in state [:clients client-id])
        r (:pending-read client)
        cas (:cas r)]
    (if (= (log/read-value state (:key cas)) (:old cas))
      (-> (assoc-in state [:clients client-id :pending-write]
                    {:uuid (:uuid r)
                     :target-commit-index (inc (:commit-index state))})
          (assoc-in [:clients client-id :pending-read] {})
          (l/write {:key (:key cas)
                    :op :set
                    :value (:new cas)}))
      (-> (assoc-in state [:clients client-id :pending-read] {})
          (utils/transmit {:client-dst client-id :uuid (:uuid r)
                           :type :cas-reply
                           :body {:value :invalid-value}})))))

(defn- check-writes [state client-id]
  (let [client (get-in state [:clients client-id])]
    (if (and (not (empty? (:pending-write client)))
             (>= (:commit-index state)
                 (:target-commit-index (:pending-write client))))
      (->
       (assoc-in state [:clients client-id :pending-write] {})
       (utils/transmit {:type :write-reply :client-dst client-id
                        :uuid (:uuid (:pending-write client))
                        :body {:value :ok}}))
      state)))

(defn check-clients [state]
  (reduce (fn [state client-id]
            (-> (check-reads state client-id)
                (check-writes client-id)))
          state (keys (:clients state))))

(defn add-write
  "adds a client write that is waiting for a response.
   It should only respond when a quorum of followers
   have acked it (ie when the commit-index is +1 of what it is now)"
  [state pkt]
  (assoc-in state [:clients (:client-id pkt) :pending-write]
            {:target-commit-index (inc (:commit-index state))
             :uuid (:uuid pkt)}))

(defn add-read
  "returns the update clients hash and either the response or a request
   to broadcast"
  [state pkt]
  (if-let [old-response (get-in (:clients state)
                                [(:client-id pkt) :history (:uuid pkt)])]
    [(utils/transmit state old-response)]
    [(assoc-in state [:clients (:client-id pkt) :pending-read]
               {:uuid (:uuid pkt) :key (:key pkt) :hb-count #{}})
     :broadcast-heart-beat]))

(defn add-cas
  [state pkt]
  (assoc-in state [:clients (:client-id pkt) :pending-read]
             {:key (:key (:body pkt)) :hb-count #{} :cas (:body pkt)
              :uuid (:uuid pkt)}))

(defn close-clients [state]
  (update state :clients
          (fn [clients]
            (doseq [[k v] clients]
              (when (:socket v)
                (s/close! (:socket v))))
            nil)))
