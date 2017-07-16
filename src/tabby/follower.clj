(ns tabby.follower
  (:require [clojure.tools.logging :refer [warn info]]
            [tabby.candidate :as candidate]
            [tabby.client-state :as cs]
            [tabby.log :as log]
            [tabby.utils :as utils]))

(defn- election-timeout? [state]
  (<= (:election-timeout state) 0))

;;; TODO: refactor this
(defn- append-entries [state params]
  (let [r {:term (:current-term state)
           :count (count (:entries params))
           :success (and
                     (= (:prev-log-index params) (count (:log state)))
                     (utils/valid-term? state params)
                     (log/prev-log-term-equals? state params))}]
    {:state (if (:success r)
              (log/append-log state params)
              (update state :log (fn [x]
                                   (if (and (not= (:prev-log-index params) (inc (count (:log state))))
                                            (not (empty? x)))
                                     (pop x)
                                     x))))
     :result r}))

(defn handle-request-vote
  "incoming request to for a vote packet"
  [state p]
  (let [params (:body p)
        vote-granted? (and
                       (utils/valid-term? state params)
                       (not (:voted-for state))
                       (log/prev-log-term-equals? state params))]

    (-> (if vote-granted?
          (assoc state :voted-for (:candidate-id params))
          state)
        (utils/transmit {:dst (:src p)
                         :src (:id state)
                         :type :request-vote-reply
                         :body {:term (:current-term state)
                                 :vote-granted? vote-granted?}}))))

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
  (let [r (append-entries (assoc state :election-timeout
                                 (utils/random-election-timeout state))
                          (:body p))]
    (utils/transmit (:state r) {:dst (:src p) :src (:id state)
                                :type :append-entries-response
                                :body (:result r)})))

(defn become-follower
  "called from the packet queue if current-term < supplied term"
  [state leader-id]
  (info (:id state) "becoming follower, leader-id" leader-id)
  (-> (cs/close-clients state)
      (dissoc :next-timeout :voted-for :match-index :clients)
      (assoc :election-timeout (utils/random-election-timeout state)
             :leader-id leader-id
             :type :follower)))
