(ns tabby.candidate
  (:require [tabby.utils :refer :all]
            [tabby.leader :refer :all]
            [tabby.log :refer :all]))

(defn- make-request-vote-pkt [state peer]
  {:dst peer
   :src (:id state)
   :type :request-vote
   :body {:term (:current-term state)
          :candidate-id (:id state)
          :prev-log-index (count (:log state))
          :prev-log-term (get-log-term state (last-log-index state))}})

(defn- broadcast-request-vote [state]
  (foreach-peer state
                (fn [s p]
                  (transmit s (make-request-vote-pkt s p)))))

(defn become-candidate [state]
  (-> state
      (assoc :type :candidate)
      (assoc :voted-for (:id state))
      (update-in [:current-term] inc)
      (assoc :election-timeout (random-election-timeout))
      (broadcast-request-vote)
      (assoc :votes {(:id state) true})))

(defn handle-request-vote-response
  "response to a request to vote"
  [state p]
  (if (not= :candidate (:type state))
    state
    (let [s (update-in state [:votes]
                       (fn [votes]
                         (assoc votes (:src p)
                                          (:vote-granted? (:body p)))))
          c (count (filter identity (vals (:votes s))))]
      (if (quorum? s c)
        (become-leader s)
        s))))
