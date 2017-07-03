(ns tabby.server
  (:require [tabby.utils :as utils]
            [tabby.log :as log]
            [tabby.client-state :as cs]
            [tabby.leader :as l]
            [tabby.follower :as f]
            [clojure.tools.logging :refer [warn info]]
            [tabby.candidate :as c]))

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
  (let [body (:body params)]
    (if (or (and (:term body)
                 (< (:current-term state) (:term body)))
            (and (= :candidate (:type state))
                 (= :append-entries (:type params))
                 (<= (:current-term state) (:term body))))
      (f/become-follower (assoc state :current-term (:term body))
                         (or (:leader-id body) (:candidate-id body)))
      (if (:leader-id body)
        (assoc state :leader-id (:leader-id body))
        state))))

(defn- apply-commit-index [state]
  (if (> (:commit-index state) (:last-applied state))
    (-> (update state :last-applied inc)
        (update :db #(log/apply-entry state %)))
    state))

(defn- redirect-to-leader [state p]
  (info (:id state) ": redirecting client " (:src p)
        "to leader: " (:leader-id state))
  (utils/transmit state {:client-dst (:client-id p)
                         :type :redirect
                         :hostname (get-in state [:peers (:leader-id state) :hostname])
                         :port (get-in state [:peers (:leader-id state) :port])
                         :leader-id (:leader-id state)}))

(defn- handle-set [state p]
  (-> state
      (l/write {:key (:key p)
                :value (:value p)
                :op :set})
      (cs/add-write p)))

(defn handle-append-entries-response [state p]
  (if (pos? (get-in p [:body :count])) ; heart beat response
    (l/check-and-update-append-entries state p)
    (update state :clients cs/inc-heartbeats (:src p))))

(defn- handle-packet
  [state p]
  (if (:client-id p)
    (if (utils/leader? state)
      ((condp = (:type p)
        :get cs/add-read
        :set handle-set
        :cas cs/add-cas) state p)
      (redirect-to-leader state p))
    (-> (check-term state p)
        ((condp = (:type p)
           :request-vote f/handle-request-vote
           :request-vote-reply c/handle-request-vote-response
           :append-entries f/handle-append-entries
           :append-entries-response handle-append-entries-response
           (fn [s p]
             (warn (:id state) " invalid packet: " p)
             s))
         p))))

(defn- process-rx-packets [state]
  (reduce (fn [s p]
            (-> (handle-packet s p)
                (update :rx-queue rest))) state (:rx-queue state)))

(defn update-state [state dt]
  (-> (apply-commit-index state)
      (process-rx-packets)
      (update :election-timeout - dt)
      (utils/if-not-leader? f/check-election-timeout)
      (utils/if-leader? l/check-backlog dt)
      (utils/if-leader? cs/check-clients)))

(defn set-peers [state peers]
  (assoc state :peers peers))

(defn handle-write
  "skips the client state part of the state machine"
  [state kv]
  (assert (= :leader (:type state)))
  (l/write state {:key (first (keys kv))
                  :value (first (vals kv))
                  :op :set}))

(defn create-server [id]
  {:current-term 0
   :log [{:term 0 :cmd {:op :reset}}]
   :id id
   :tx-queue '()
   :rx-queue '()
   :commit-index 0
   :last-applied 0
   :type :follower
   :election-timeout (utils/random-election-timeout nil)
   :peers {}
   :clients {}
   :db {}})
