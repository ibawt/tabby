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

(defn- has-reads? [client]
  (not (empty? (:pending-read client))))

(defn inc-heartbeats [clients src]
  (utils/mapf clients (fn [client]
                        (if (has-reads? client)
                          (update-in client [:pending-read :hb-count] conj src)
                          client))))

(defn- handle-read-cas
  [client-id client state]
  (let [r (:pending-read client)
        cas (:cas r)]
    (if (= (log/read-value state (:key cas)) (:old cas))
      [[client-id (-> (assoc client :pending-write
                             {:uuid (:uuid r)
                              :target-commit-index (inc (:commit-index state))})
                     (assoc :pending-read {}))]
       (l/write state {:key (:key cas)
                       :op :set
                       :value (:new cas)})]
      [[client-id (assoc client :pending-read {})]
         (utils/transmit state {:client-dst client-id :uuid (:uuid r)
                                :type :cas-reply :body {:value :invalid-value}})])))

(defn- check-reads
  "takes a client-id client map pair and a state and returns the same
   but updated."
  [[client-id client] state]
  (if (and (has-reads? client)
           (utils/quorum? (count (:peers state))
                    (inc (count (:hb-count (:pending-read client)))))) ; count yourself
    (if (get-in client [:pending-read :cas])
      (handle-read-cas client-id client state)
      [[client-id (assoc client :pending-read {})]
       (utils/transmit state {:client-dst client-id :uuid (:uuid (:pending-read client))
                              :body {:value (log/read-value state (:key (:pending-read client)))}})])
    [[client-id client] state]))

(defn- check-writes [[client-id client] state]
  (if (and (not (empty? (:pending-write client)))
           (>= (:commit-index state)
               (:target-commit-index (:pending-write client))))
    [[client-id (assoc client :pending-write {})]
     (utils/transmit state {:type :write-reply :client-dst client-id
                      :uuid (:uuid (:pending-write client))
                      :body {:value :ok}})]
    [[client-id client] state]))

(defn check-clients
  "invoked when the leader to check the state of client
   reads."
  [state]
  (loop [c (seq (:clients state))
         s state
         out []]
    ;; TODO: this pretty kludgy
    (if (empty? c)
      (assoc s :clients (into {} out))
      (let [[cc ss] (check-reads (first c) s)
            [cc2 ss2] (check-writes cc ss)]
        (recur (rest c)
               ss2
               (conj out cc2))))))

(defn add-write
  "adds a client write that is waiting for a response.
   It should only respond when a quorum of followers
   have acked it (ie when the commit-index is +1 of what it is now)"
  [state pkt]
  (assoc-in state [:clients (:client-id pkt) :pending-write]
            {:target-commit-index (inc (:commit-index state)) :uuid (:uuid pkt)}))

(defn add-read
  "returns the update clients hash and either the response or a request
   to broadcast"
  [state pkt]
  (if-let [old-response (get-in (:clients state) [(:client-id pkt) :history (:uuid pkt)])]
    [(utils/transmit state old-response)]
    [(assoc-in state [:clients (:client-id pkt) :pending-read]
               {:uuid (:uuid pkt) :key (:key pkt) :hb-count #{}}) :broadcast-heart-beat]))

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
