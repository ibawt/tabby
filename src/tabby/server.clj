(ns tabby.server
  (:require [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

(def election-timeout-max 150)

(defn- random-election-timeout []
  (+ (rand-int election-timeout-max) election-timeout-max))

(defn- invalid-term? [state params]
  (< (:term params) (:current-term @state)))

(defn- prev-log-term-equals? [state params]
  false)

(defn- append-log [state params]
  ; I think we need to do step 4 here
  (swap! state conj :log (:entries params))
  (when (> (:leader-commit @state) (:commit-index @state))
    (swap! state assoc :commit-index (min (:leader-commit @state) (count (:log @state))))))

(defn- append-entries [state params]
;  (choose-election-timeout state)
  (when-not (= (:leader-id @state) (:leader-id params))
    (swap! state assoc :leader-id (:leader-id params)))
  {:term (:current-term @state)
   :success
   (if (or (invalid-term? state params) ; step 1
           (not (prev-log-term-equals? state params))) ; step 2
     false ; think should be step 3
     (append-log state params))})

(defn- request-vote [state params]
  (info  " - received request-vote: " params)
  {:term (:current-term state)
   :vote-granted?
   (if (invalid-term? state params)
     false
     (if-not (:voted-for state)
       (do
         (swap! assoc :voted-for (:candidate-id params))
         true)
       false))})

(defn- get-log-index [state] 0)

(defn- get-log-term [state] 0)

(defn- heart-beat [state peer]
  {:term (:current-term state)
   :leader-id (:id state)
   :prev-log-index (get-log-index state)
   :prev-log-term (get-log-term state)
   :entries []
   :leader-commit (:commit-index state)})

(defn- send-request-vote [state peer]
  (request-vote peer {:term (:current-term state)
                      :candidate-id (:id state)
                      :last-log-index (:commit-index state)
                      :last-log-term (get-log-term state)}))


(defn- broadcast-heartbeat [state]
  (doall (pmap heart-beat (:peers state))))

(defn- become-leader [state]
  (info "I AM LEADER!!")
  (->
   state
   (assoc :type :leader)
   (assoc :next-index (map 0 (:peers state))) ; these values are wrong
   (assoc :match-index (map 0 (:peers state)))
   (broadcast-heartbeat)))

(defn- won-election? [state results])

(defn- trigger-election [state]
  (-> state
      (update-in [:current-term] inc))
      (assoc :type :canditate)
      (assoc :voted-for (:id state))
      (assoc :election-timeout (random-election-timeout)))
  ;; (let [results (map (partial send-request-vote state) (:peers @state))]
  ;;   (info "results: " results)
  ;;   (when (won-election? state results)
  ;;     (info "not in here I guess")
  ;;     (become-leader state)))

(defn- election-timeout? [state]
  (<= (:election-timeout state) 0))

(defn set-peers [state peers]
  (swap! state assoc :peers peers))

(defn update [state dt]
  (swap! state #(->
                 %
                 (update-in [:election-timeout] - dt))))

(defn create-server [id]
  {:current-term 0 ; persist
   :voted-for nil  ; persist
   :log []         ; persist
   :id id          ; user assigned
   :commit-index 0
   :last-applied 0
   :type :follower
   :election-timeout (random-election-timeout)
   :peers []
   :db {}})
