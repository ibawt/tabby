(ns tabby.follower
  (:require [clojure.tools.logging :refer [warn info]]
            [tabby.candidate :as candidate]
            [tabby.client-state :as cs]
            [tabby.log :as log]
            [tabby.utils :as utils]))

(defn- election-timeout? [state]
  (<= (:election-timeout state) 0))

(defn- request-vote [state params]
  (let [r {:term (:current-term state)
           :vote-granted?
           (and
            (utils/valid-term? state params)
            (not (:voted-for state))
            (log/prev-log-term-equals? state params))}]
    {:response r
     :state (if (:vote-granted? r)
              (assoc state :voted-for (:candidate-id params))
              state)}))

;;; TODO: refactor this shit show
(defn- append-entries [state params]
  (if (> 0 (count (:entries params)))
      (warn (:id state) " append-entries: " params))
  (let [r {:term (:current-term state)
           :count (count (:entries params))
           :success (and (utils/valid-term? state params)
                         (log/prev-log-term-equals? state params))}]
    {:state (if (:success r) (log/append-log state params) state)
     :result r}))

(defn handle-request-vote
  "incoming request to for a vote packet"
  [state p]
  (let [{s :state r :response} (request-vote state (:body p))]
    (utils/transmit s {:dst (:src p)
                       :src (:id state)
                       :type :request-vote-reply
                       :body r})))

(defn check-election-timeout
  "checks if we have timed out on the election,
   and transitions into candidate state"
  [state]
  (if (election-timeout? state)
    (candidate/become-candidate state)
    state))

(defn handle-append-entries
  "append entries packet"
  [state p]
  ;; (warn "[" (:id state) "] append entries:" p)
  (let [r (append-entries (assoc state :election-timeout
                                 (utils/random-election-timeout))
                          (:body p))]
    (utils/transmit (:state r) {:dst (:src p) :src (:id state)
                                :type :append-entries-response
                                :body (:result r)})))

(defn become-follower
  "called from the packet queue if current-term < supplied term"
  [state leader-id]
  (info (:id state) " becoming follower, leader-id: " leader-id)
  (-> state
      (cs/close-clients)
      (dissoc :next-timeout)
      (dissoc :voted-for)
      (dissoc :match-index)
      (assoc :election-timeout (utils/random-election-timeout))
      (assoc :leader-id leader-id)
      (assoc :type :follower)))
