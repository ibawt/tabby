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

(def has-reads?
  "does this client have any pending reads?"
  (comp not empty? :pending-read))

(defn inc-heartbeats
  "adds the src to the set of heartbeats
   for each pending read.  This establishes
   the quorum count for each read."
  [clients src]
  (utils/mapf clients
              (fn [client]
                (if (has-reads? client)
                  (update-in client [:pending-read :hb-count] conj src)
                  client))))

(defn- handle-read-cas
  "handles compare-and-swap operations,
   read to comparison to write"
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

(defn- check-writes
  "check if the required commit-index is set and
   transmits client packets"
  [state client-id]
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

(defn- check-reads
  "checks if a pending read is ready and
   transmits client packets."
  [state client-id]
  (let [client (get-in state [:clients client-id])]
    (if (and (has-reads? client)
             (utils/quorum? (count (:peers state))
                            (inc (count (:hb-count (:pending-read client)))))) ; count yourself
     (if (get-in client [:pending-read :cas])
       (handle-read-cas state client-id)
       (-> (assoc-in state  [:clients client-id :pending-read] {})
           (utils/transmit
            {:client-dst client-id :uuid (:uuid (:pending-read client))
             :body {:value (log/read-value state (:key (:pending-read client)))}})))
     state)))

(defn check-clients
  "check current clients for read/write/cas operations"
  [state]
  (reduce (fn [state [client-id _]]
            (-> (check-reads state client-id)
                (check-writes client-id)))
          state (:clients state)))

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
    (utils/transmit state old-response)
    (-> (assoc-in state [:clients (:client-id pkt) :pending-read]
                  {:uuid (:uuid pkt) :key (:key pkt) :hb-count #{}})
        (l/broadcast-heartbeat))))

(defn add-cas
  "adds a compare and swap operation to client state table."
  [state pkt]
  (assoc-in state [:clients (:client-id pkt) :pending-read]
             {:key (:key (:body pkt)) :hb-count #{} :cas (:body pkt)
              :uuid (:uuid pkt)}))

(defn close-clients [state]
  (update state :clients
          (fn [clients]
            (doseq [[k v] clients]
              (when-let [socket (:socket v)]
                (s/close! socket)))
            nil)))
