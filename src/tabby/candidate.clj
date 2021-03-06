(ns tabby.candidate
  (:require [tabby.utils :as u]
            [tabby.leader :as l]
            [tabby.log :as log]
            [clojure.tools.logging :refer [warn info]]
            [tabby.client-state :as cs]))

(defn- make-request-vote-pkt [state peer]
  {:dst peer
   :src (:id state)
   :type :request-vote
   :body {:term (:current-term state)
          :candidate-id (:id state)
          :prev-log-index (count (:log state))
          :prev-log-term (log/get-log-term state (log/last-log-index state))}})

(defn- broadcast-request-vote [state]
  (u/foreach-peer state
                (fn [s [p _]]
                  (u/transmit s (make-request-vote-pkt s p)))))

(defn become-candidate
  "becomes a candidate"
  [state]
  (info (:id state) " becoming candidate")
  (-> (assoc state :type :candidate
             :election-timeout (u/random-election-timeout state)
             :votes {(:id state) true}
             :voted-for (:id state))
      (dissoc :leader-id)
      (update :current-term inc)
      (broadcast-request-vote)))

(defn handle-request-vote-response
  "response to a request to vote"
  [state p]
  (if (not= :candidate (:type state))
    state
    (let [s (assoc-in state [:votes (:src p)] (:vote-granted? (:body p)))
          c (count (filter identity (vals (:votes s))))]
      (if (u/quorum? (count (:peers state)) c)
        (l/become-leader s)
        s))))
