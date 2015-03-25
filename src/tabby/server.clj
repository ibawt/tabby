(ns tabby.server
  (:require [tabby.utils :refer :all]
            [tabby.log :refer :all]
            [tabby.leader :refer :all]
            [tabby.follower :refer :all]
            [clojure.tools.logging :refer :all]
            [tabby.candidate :refer :all]))

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
     (assoc :current-term (:term params))
     (become-follower))
    state))

(defn- apply-commit-index [state]
  (if (> (:commit-index state) (:last-applied state))
    (->
     state
     (update-in [:last-applied] inc)
     (update-in [:db] (partial apply-entry state)))
    state))

(defn- redirect-to-leader [state p]
  (transmit state {:client-dst (:client-id p)
                   :leader-id (:leader-id state)}))

(defn- handle-get [state p]
  (if (leader? state)
    (write state (select-keys p [:key :value]))
    (redirect-to-leader state p)))

(defn- handle-packet [state]
  (let [p (first (:rx-queue state))
        s (check-term state (:body p))]
    (condp = (:type p)
      :get (handle-get s p)
      ;:set (handle-set s p)
      :request-vote (handle-request-vote s p)
      :request-vote-reply (handle-request-vote-response s p)
      :append-entries (handle-append-entries s p)
      :append-entries-response (handle-append-entries-response s p))))

(defn- process-rx-packets [state]
  (loop [s state]
    (if (empty? (:rx-queue s)) s
        (recur (-> s
                   (handle-packet)
                   (update-in [:rx-queue] rest))))))

(defn update [dt state]
  (-> state
      (update-in [:election-timeout] - dt)
      (apply-commit-index)
      (if-not-leader? check-election-timeout)
      (process-rx-packets)
      (if-leader? check-backlog dt)))

(defn set-peers [state peers]
  (assoc state :peers peers))

(defn handle-write [state kv]
  (if (leader? state)
    (write state kv)
    (redirect-to-leader state)))

(defn create-server [id]
  {:current-term 0
   :log []
   :id id
   :tx-queue '()
   :rx-queue '()
   :commit-index 0
   :last-applied 0
   :type :follower
   :election-timeout (random-election-timeout)
   :peers []
   :clients []
   :db {}})
