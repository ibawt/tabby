(ns tabby.server
  (:require [tabby.utils :as u]))

(def election-timeout-max 150)

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

(defn random-election-timeout
  "returns election-timeout-max + rand(0, election-timeout-max)"
  []
  (+ (rand-int election-timeout-max) election-timeout-max))

(defn- heart-beat-timeout []
  (+ 1 (rand-int 9)))

(defn- quorum? [state c]
  (>= c (inc (/ (count (:peers state)) 2))))

;;; Log functions
(defn last-log-index
  "return the index of the last log entry (1 based)"
  [state]
  (count (:log state)))

(defn get-log-at
  "gets the log entry at the index specified (1 based)"
  [state idx]
  (if (or (neg? idx) (> idx (last-log-index state)))
    (throw (IndexOutOfBoundsException.
            (format "Invalid index: %d, last-log-index: %d"
                    idx (last-log-index state))))
    (get (:log state) (dec idx))))

(defn get-log-term [state idx]
  (if (or (< (last-log-index state) 1) (<= idx 0))
    0
    (:term (get-log-at state idx))))

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

(defn- valid-term? [{c-t :current-term} {term :term}]
  (>= term c-t))

(defn prev-log-term-equals?
  [state {p-index :prev-log-index p-term :prev-log-term}]
  (or (= 0 p-index (last-log-index state))
      (= (get-log-term state p-index) p-term)))

(defn- apply-log [state entries]
  (if (pos? (count entries))
    (update-in state [:log] #(apply conj % entries))
    state))

(defn append-log [state params]
  (let [s (-> state
              (assoc :election-timeout (random-election-timeout))
              (apply-log (:entries params)))]
    (if (> (:leader-commit params) (:commit-index state))
      (assoc s :commit-index
             (min (:leader-commit params) (count (:log state))))
      s)))

;;; TODO: refactor this shit show
(defn append-entries [state params]
  (let [r {:term (:current-term state)
           :count (count (:entries params)) ; this should be replaced with the actual number
           :success (and (valid-term? state params)
                         (prev-log-term-equals? state params))}]
    {:state (if (:success r) (append-log state params) state)
     :result r}))

(defn transmit [state request]
  (update-in state [:tx-queue] conj request))

(defn request-vote [state params]
  (let [r {:term (:current-term state)
           :vote-granted?
           (and (valid-term? state params)
                (nil? (:voted-for state))
                (prev-log-term-equals? state params))}]
    {:response r
     :state (if (:vote-granted? r)
              (assoc state :voted-for (:candidate-id params))
              state)}))

(defn make-heart-beat-pkt [state peer]
  (let [p-index (count (:log state))
        p-term (get-log-term state p-index)]
    {:dst peer
     :type :append-entries
     :src (:id state)
     :body {:term (:current-term state)
            :leader-id (:id state)
            :prev-log-index p-index
            :prev-log-term p-term
            :entries []
            :leader-commit (:commit-index state)}}))

(defn make-append-log-pkt [state peer]
  (let [p-index (dec (get (:next-index state) peer))
        p-term (get-log-term state p-index)]
    {:dst peer
     :type :append-entries
     :src (:id state)
     :body {:term (:current-term state)
            :leader-id (:id state)
            :prev-log-index p-index
            :prev-log-term p-term
            :entries [(get-log-at state (inc p-index))]
            :leader-commit (:commit-index state)}}))

(defn foreach-peer [state f & args]
  "calls f with current state, the peer and etc."
  (loop [s state
         p (:peers state)]
    (if (empty? p) s
      (recur (apply f s (first p) args)
             (rest p)))))

(defn broadcast-append-log [state]
  (foreach-peer state #(transmit %1 (make-append-log-pkt %1 %2))))

(defn write [state kv]
  (->
   state
   (update-in [:log] conj {:term (:current-term state) :cmd kv})
   (broadcast-append-log)))

(defn make-heart-beat-pkt [state peer]
  (let [p-index (last-log-index state)
        p-term (get-log-term state p-index)]
    {:dst peer :type :append-entries
     :src (:id state)
     :body {:term (:current-term state)
            :leader-id (:id state)
            :prev-log-index p-index
            :prev-log-term p-term
            :entries []
            :leader-commit (:commit-index state)}}))

(defn make-request-vote-pkt [state peer]
  {:dst peer
   :src (:id state)
   :type :request-vote
   :body {:term (:current-term state)
          :candidate-id (:id state)
          :prev-log-index (count (:log state))
          :prev-log-term (get-log-term state (last-log-index state))}})

(defn broadcast-heartbeat [state]
  (foreach-peer state #(transmit %1 (make-heart-beat-pkt %1 %2))))

(defn trace-s [state]
  (when (= (:id state) 0)
    (println (select-keys state [:id :type :election-timeout
                                 :current-term :commit-index])))
  state)

(defn become-leader [state]
  (->
   state
   (assoc :type :leader)
   (assoc :next-timeout (reduce #(merge %1 {%2 (heart-beat-timeout)}) {} (:peers state)))
   (assoc :next-index (reduce #(merge %1 {%2 (inc (count (:log state)))}) {}
                              (:peers state)))
   (assoc :match-index (reduce #(merge %1 {%2 0}) {} (:peers state)))
   (broadcast-heartbeat)))

(defn election-timeout? [state]
  (<= (:election-timeout state) 0))

(defn set-peers [state peers]
  (assoc state :peers peers))

(defn apply-entry [{log :log index :last-applied} db]
  (let [cmd (:cmd (get log index))]
    (merge db cmd)))

(defn apply-commit-index [state]
  (if (> (:commit-index state) (:last-applied state))
    (->
     state
     (update-in [:last-applied] inc)
     (update-in [:db] (partial apply-entry state)))
    state))

(defn broadcast-request-vote [state]
  (foreach-peer state #(transmit %1 (make-request-vote-pkt %1 %2))))

(defn check-election-timeout [state]
  (if (election-timeout? state)
    (-> state
        (assoc :type :candidate)
        (assoc :voted-for (:id state))
        (update-in [:current-term] inc)
        (assoc :election-timeout (random-election-timeout))
        (broadcast-request-vote)
        (assoc :votes {(:id state) true}))
    state))

(defn handle-request-vote
  "incoming request to for a vote"
  [state p]
  (let [{s :state r :response} (request-vote state (:body p))]
    (transmit s  {:dst (:src p)
                  :src (:id state)
                  :type :request-vote-reply
                  :body r})))

(defn handle-request-vote-response
  "response to a request to vote"
  [state p]
  (if (not= :candidate (:type state))
    state
    (let [s (update-in state [:votes]
                       (fn [votes] (assoc votes (:src p)
                                          (:vote-granted? (:body p)))))
          c (count (filter identity (vals (:votes s))))]
      (if (quorum? s c)
        (become-leader s)
        s))))

(defn handle-append-entries [state p]
  (let [r (append-entries state (:body p))]
    (transmit (:state r) {:dst (:src p) :src (:id state)
                              :type :append-entries-response
                              :body (:result r)})))

(defn- update-match-and-next [state p]
  (let [s (if-not (:success (:body p))
            (update-in state [:next-index (:src p)] dec)
            state)]
    (assoc-in s [:match-index (:src p)] (get (:next-index state) (:src p)))))

(defn check-commit-index [state]
  (let [f (frequencies (vals (:match-index state)))
        [index c] (first f)]
    (if (and (quorum? state (inc c)) (> index (:commit-index state)))
      (assoc state :commit-index index)
      state)))

(defn handle-append-entries-response [state p]
  (if (pos? (-> p :body :count)) ; heart beat response
    (-> state
        (update-match-and-next p)
        (check-commit-index))
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

(defn- peer-timeout? [state peer]
  (<= (get (:next-timeout state) peer) 0))

(defn- apply-peer-timeouts [state dt]
  (update-in state [:next-timeout]
             (fn [timeouts]
               (reduce-kv #(assoc %1 %2 (- %3 dt)) {} timeouts))))

(defn- update-peer-timeout [state peer]
  (update-in state [:next-timeout peer] + 10))

(defn send-peer-update [state peer]
  (transmit state (if (> (:commit-index state) (get (:match-index state) peer))
                      (make-append-log-pkt state peer)
                      (make-heart-beat-pkt state peer))))

(defn check-backlog [state dt]
  (if (leader? state)
    (foreach-peer (apply-peer-timeouts state dt)
                  (fn [s p]
                    (if (peer-timeout? s p)
                      (-> s
                          (send-peer-update p)
                          (update-peer-timeout p))
                      s)))
    state))

(defn update [state dt]
  (-> state
      (update-in [:election-timeout] - dt)
      (apply-commit-index)
      (check-election-timeout)
      (process-rx-packets)
      (check-backlog dt)))

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
