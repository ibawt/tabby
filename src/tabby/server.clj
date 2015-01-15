(ns tabby.server
  (:require [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

(def election-timeout-max 150)

(defn- random-election-timeout []
  (+ (rand-int election-timeout-max) election-timeout-max))

(defn- invalid-term? [state params]
  (< (:term params) (:current-term state)))

(defn- prev-log-term-equals? [state params]
  (= (:term (get (:log state)
                 (:prev-log-index params)))
     (:prev-log-term params)))

(defn- append-log [state params]
  ; I think we need to do step 4 here
  (swap! state conj :log (:entries params))
  (when (> (:leader-commit @state) (:commit-index @state))
    (swap! state assoc :commit-index
           (min (:leader-commit @state) (count (:log @state))))))

(defn- append-entries [state params]
  (when-not (= (:leader-id state) (:leader-id params))
    (swap! state assoc :leader-id (:leader-id params)))
  {:term (:current-term state)
   :success
   (if (or (invalid-term? state params) ; step 1
           (not (prev-log-term-equals? state params))) ; step 2
     false ; think should be step 3
     (append-log state params))})

(defn- request-vote [state params]
  (info  " - received request-vote: " params)
  (let [s @state]
    {:term (:current-term s)
     :vote-granted?
     (if (invalid-term? s params)
       false
       (if (and (nil? (:voted-for s)) (prev-log-term-equals? s params))
         (do
           (swap! state assoc :voted-for (:candidate-id params))
           true)
         false))}))

(defn- get-log-index [state] 0)

(defn- get-log-term [state] 0)

(defn- send-heart-beat [state peer]
  (let [p-index (dec (count (:log state)))
        p-term (:term (get (:log state) p-index))]
    (append-entries peer {:term (:current-term state)
                          :leader-id (:id state)
                          :prev-log-index p-index
                          :prev-log-term p-term
                          :entries []
                          :leader-commit (:commit-index state)})))

(defn- send-request-vote [state peer]
  (request-vote peer {:term (:current-term state)
                      :candidate-id (:id state)
                      :last-log-index (:commit-index state)
                      :last-log-term (get-log-term state)}))


(defn- broadcast-heartbeat [state]
  (let [r (map send-heart-beat (:peers state))])
  state)

(defn- become-leader [state peer-terms]
  (info "I AM LEADER!!")
  (->
   state
   (assoc :type :leader)
   (assoc :next-index (vec (repeat (count (:peers state)) (:last-applied state))))
   (assoc :match-index (vec (repeat (count (:peers state)) 0)))
   (broadcast-heartbeat)))

(defn- election-timeout? [state]
  (<= (:election-timeout state) 0))

(defn set-peers [state peers]
  (swap! state assoc :peers peers))

(defn apply-commit-index [state]
  (when (> (:commit-index state) (:last-applied state))
    ; TODO: actually do the operation in the log
    (update-in state [:last-applied] inc))
  state)

(defn broadcast-request-vote [state]
  (let [r (map (partial send-request-vote state) (:peers state))]
    (if (> (/ (count (:peers state)) 2)
           (reduce #(+ %1 (if (:vote-granted? %2) 1 0)) 0 r))
      (become-leader state (map :term r))
      state)))

(defn check-election-timeout [state]
  (if (election-timeout? state)
    (-> state
        (assoc :type :candidate)
        (assoc :voted-for (:id state))
        (update-in [:current-term] inc)
        (assoc :election-timeout (random-election-timeout))
        (broadcast-request-vote))
    state))

(defn update [state dt]
  (swap! state #(-> %1
                    (update-in [:election-timeout] - dt)
                    (apply-commit-index)
                    (check-election-timeout)))
  state)

(defn create-server [id]
  {:current-term 0 ; persist
   :voted-for nil  ; persist
   :log [{:term 0 :cmd :init}]         ; persist start at 1
   :id id          ; user assigned
   :commit-index 0
   :last-applied 0
   :type :follower
   :election-timeout (random-election-timeout)
   :peers []
   :db {}})
