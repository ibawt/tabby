(ns tabby.server
  (:require [tabby.utils :refer :all]
            [tabby.log :refer :all]
            [tabby.leader :refer :all]
            [tabby.follower :refer :all]
            [tabby.candidate :refer :all]))

(defn leader? [state]
  (= :leader (:type state)))

(defn follower? [state]
  (= :follower (:type state)))

(defn candidate? [state]
  (= :candidate (:type state)))

;;; Utility Functions
(defn packet-count
  "returns the number of packets
   in the rx and tx queue"
  [state]
  (reduce + (map count (vals (select-keys state [:tx-queue :rx-queue])))))

(defn check-term
  "if supplied term < current term, set current term to term
  and convert to follower"
  [state params]
  (if (< (:current-term state) (:term params))
    (-> state
        (assoc :type :follower)
        (assoc :voted-for nil)
        (assoc :current-term (:term params)))
    state))

(defn invalid-term? [state params]
  (< (:term params) (:current-term state)))



(defn trace-s [state]
  (when (= (:id state) 0)
    (println (select-keys state [:id :type :election-timeout
                                 :current-term :commit-index])))
  state)

(defn set-peers [state peers]
  (assoc state :peers peers))

(defn apply-commit-index [state]
  (if (> (:commit-index state) (:last-applied state))
    (->
     state
     (update-in [:last-applied] inc)
     (update-in [:db] (partial apply-entry state)))
    state))

(defn handle-packet [state]
  (let [p (first (:rx-queue state))
        s (check-term state (:body p))]
    (condp = (:type p)
      :request-vote (handle-request-vote s p)
      :request-vote-reply (handle-request-vote-response s p)
      :append-entries (handle-append-entries s p)
      :append-entries-response (handle-append-entries-response s p))))

(defn process-rx-packets [state]
  (loop [s state]
    (if (empty? (:rx-queue s)) s
        (recur (-> s
                   (handle-packet)
                   (update-in [:rx-queue] rest))))))

(defn if-leader? [state f & args]
  (if (leader? state) (apply f state args) state))

(defn if-follower? [state f & args]
  (if (follower? state) (apply f state args) state))

(defn update [state dt]
  (-> state
      (update-in [:election-timeout] - dt)
      (apply-commit-index)
      (if-follower? check-election-timeout)
      (process-rx-packets)
      (if-leader? check-backlog dt)))

(defn redirect-to-leader [state]
  state)

(defn handle-write [state kv]
  (if (leader? state)
    (write state kv)
    (redirect-to-leader state)))

(defn create-server [id]
  {:current-term 0 ; persist
   :log []
   :id id          ; user assigned
   :tx-queue '()
   :rx-queue '()
   :commit-index 0
   :last-applied 0
   :type :follower
   :election-timeout (random-election-timeout)
   :peers []
   :db {}})
