(ns tabby.follower
  (:require [clojure.tools.logging :refer :all]
            [tabby.candidate :refer :all]
            [tabby.client-state :as cs]
            [tabby.log :refer :all]
            [tabby.utils :refer :all]))

(defn- election-timeout? [state]
  (<= (:election-timeout state) 0))

(defn- request-vote [state params]
  (let [r {:term (:current-term state)
           :vote-granted?
           (and (valid-term? state params)
                (nil? (:voted-for state))
                (prev-log-term-equals? state params))}]
    {:response r
     :state (if (:vote-granted? r)
              (assoc state :voted-for (:candidate-id params))
              state)}))

;;; TODO: refactor this shit show
(defn- append-entries [state params]
  (let [r {:term (:current-term state)
           :count (count (:entries params))
           :success (and (valid-term? state params)
                         (prev-log-term-equals? state params))}]
    {:state (if (:success r) (append-log state params) state)
     :result r}))

(defn handle-request-vote
  "incoming request to for a vote packet"
  [state p]
  (let [{s :state r :response} (request-vote state (:body p))]
    (transmit s  {:dst (:src p)
                  :src (:id state)
                  :type :request-vote-reply
                  :body r})))

(defn check-election-timeout
  "checks if we have timed out on the election,
   and transitions into candidate state"
  [state]
  (if (election-timeout? state)
    (become-candidate state)
    state))

(defn handle-append-entries
  "append entries packet"
  [state p]
  (let [r (append-entries state (:body p))]
    (transmit (:state r) {:dst (:src p) :src (:id state)
                          :type :append-entries-response
                          :body (:result r)})))

(defn become-follower
  "called from the packet queue if current-term < supplied term"
  [state leader-id]
  (info (:id state) " becoming follower, leader-id: " leader-id)
  (-> state
      (cs/close-clients)
      (dissoc :next-timeout)
      (dissoc :match-index)
      (assoc :election-timeout (random-election-timeout))
      (assoc :leader-id leader-id)
      (assoc :type :follower)
      (assoc :voted-for nil)))
