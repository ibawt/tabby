(ns tabby.server
  (:require [tabby.utils :as utils]
            [tabby.log :refer :all]
            [tabby.client-state :as cs]
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
  (let [body (:body params)]
    (if (or (and (:term body)
                 (< (:current-term state) (:term body)))
            (and (= :candidate (:type state))
                 (= :append-entries (:type params))
                 (<= (:current-term state) (:term body))))
      (become-follower (assoc state :current-term (:term body))
                       (:leader-id body))
      (if (:leader-id body)
        (assoc state :leader-id (:leader-id body))
        state))))

(defn- apply-commit-index [state]
  (if (> (:commit-index state) (:last-applied state))
    (->
     state
     (update-in [:last-applied] inc)
     (update-in [:db] (partial apply-entry state)))
    state))

(defn- redirect-to-leader [state p]
  (warn (:id state) "redirecting to leader")
  (utils/transmit state {:client-dst (:client-id p)
                         :type :redirect
                         :hostname (get-in state [:peers (:leader-id state) :hostname])
                         :port (get-in state [:peers (:leader-id state) :port])
                         :leader-id (:leader-id state)}))

(defn- handle [f s p]
  (if (utils/leader? s)
    (f s p)
    (redirect-to-leader s p)))

(defn- handle-get [state p]
  (client-read state p))

(defn- handle-set [state p]
  (utils/transmit
   (write state (select-keys p [:key :value]))
   {:client-dst (:client-id p)
    :value :ok}))

(defn- handle-cas [state p]
  (if (= (read-value state (:key (:old p))) (:old p))
    (write state (dissoc
                  (assoc p :key (:key (:new p)) :value (:value (:new p)))
                  :old :new))
    ({:client-dst (:client-id p)
      :type :fail})))

(defn- handle-packet [state]
  (let [p (first (:rx-queue state))
        s (check-term state p)]
    (condp = (:type p)
      :get (handle handle-get s p)
      :set (handle handle-set s p)
      :cas (handle handle-cas s p)
      :request-vote (handle-request-vote s p)
      :request-vote-reply (handle-request-vote-response s p)
      :append-entries (handle-append-entries s p)
      :append-entries-response (handle-append-entries-response s p))))

(defn- process-rx-packets [state]
  (if (empty? (:rx-queue state))
    state
    (recur (-> state
               (handle-packet)
               (update-in [:rx-queue] rest)))))

(defn update-state [dt state]
  (->
   (update-in state [:election-timeout] - dt)
   (apply-commit-index)
   (utils/if-not-leader? check-election-timeout)
   (process-rx-packets)
   (utils/if-leader? check-backlog dt)
   (utils/if-leader? cs/check-clients)))

(defn set-peers [state peers]
  (assoc state :peers peers))

(defn handle-write [state kv]
  (handle-set state {:key (first (keys kv)) :value (first (vals kv))}))

(defn create-server [id]
  {:current-term 0
   :log []
   :id id
   :tx-queue '()
   :rx-queue '()
   :commit-index 0
   :last-applied 0
   :type :follower
   :election-timeout (utils/random-election-timeout)
   :peers {}
   :clients {}
   :db {}})
