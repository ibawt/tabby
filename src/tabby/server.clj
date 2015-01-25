(ns tabby.server
  (:require [tabby.utils :as u]))

(def election-timeout-max 150)

(defn random-election-timeout []
  (+ (rand-int election-timeout-max) election-timeout-max))

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

(defn prev-log-term-equals? [state params]
  (= (:term (get (:log state)
                 (:prev-log-index params)))
     (:prev-log-term params)))

(defn- apply-log [state entries]
  (if (> (count entries) 0)
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

(defn append-entries [state params]
  (let [s (check-term state params)
        r {:term (:current-term state)
           :success
           (if (or (invalid-term? s params) ; step 1
                   (not (prev-log-term-equals? s params))) ; step 2
             false ; think should be step 3
             true)}]

    {:state (if (:success r) (append-log s params) s)
     :result r}))

(defn transmit [state request]
  (update-in state [:tx-queue] conj request))

(defn request-vote [state params]
  (let [s (check-term state params)
        r {:term (:current-term s)
           :vote-granted?
           (if (invalid-term? s params)
             false
             (if (and (nil? (:voted-for s)) (prev-log-term-equals? s params))
               true
               false))}]
    {:response r
     :state (if (:vote-granted? r) (assoc s :voted-for (:candidate-id params)) s)}))

(defn make-heart-beat-pkt [state peer]
  (let [p-index (count (:log state))
        p-term (:term (get (:log state) p-index))]
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
  (let [p-index (- (get (:next-index state) peer) 1)
        p-term (:term (get (:log state) p-index))]
    {:dst peer
     :type :append-entries
     :src (:id state)
     :body {:term (:current-term state)
            :leader-id (:id state)
            :prev-log-index p-index
            :prev-log-term p-term
            :entries [(get (:log state) (inc p-index))]
            :leader-commit (:commit-index state)}}))

(defn foreach-peer [state f & args]
  "calls f with current state, the peer and etc."
  (loop [s state
         p (:peers state)]
    (if (empty? p)
      s
      (recur (apply f s (first p) args)
             (rest p)))))

(defn broadcast-append-log [state]
  (foreach-peer state #(transmit % (make-append-log-pkt %1 %2))))

(defn write [state kv]
  (->
   state
   (update-in [:log] conj {:term (:current-term state) :cmd kv})
   (broadcast-append-log)))

(defn get-log-index [state]
  (count (:log state)))

(defn get-log-term [state]
  (:term (peek (:log state))))

(defn make-heart-beat-pkt [state peer]
  (let [p-index (count (:log state))
        p-term (:term (get (:log state) p-index))]
    {:dst peer
     :type :append-entries
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
          :last-log-index (:commit-index state)
          :last-log-term (get-log-term state)}})

(defn broadcast-heartbeat [state]
  (println "broadcast")
  (foreach-peer state #(transmit %1 (make-heart-beat-pkt %1 %2))))

(defn trace-s [state]
  (when (= (:id state) 0)
    (println (select-keys state [:id :type :election-timeout :current-term :commit-index] )))
  state)

(defn become-leader [state]
  (->
   state
   (assoc :type :leader)
   (assoc :next-index (reduce #(merge %1 {%2 (count (:log state))}) {} (:peers state)))
   (assoc :match-index (reduce #(merge %1 {%2 0}) {} (:peers state)))
   (broadcast-heartbeat)))

(defn election-timeout? [state]
  (<= (:election-timeout state) 0))

(defn set-peers [state peers]
  (assoc state :peers peers))

(defn apply-entry [{log :log index :last-applied} db]
  (let [cmd (:cmd (get log index))]
    (if (= :init cmd)
      {}
      (merge db cmd))))

(defn apply-commit-index [state]
  (if (>= (:commit-index state) (:last-applied state))
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

(defn leader-heartbeat [state]
  (if (and (= :leader (:type state))
           (= (count (:tx-queue state)) 0)
           (= (count (:rx-queue state)) 0)
           (< (:election-timeout state) election-timeout-max))
    (do (println "leader sending heartbeat") (broadcast-heartbeat state))
    state))

(defn handle-request-vote [state p]
  "incoming request to for a vote"
  (let [{s :state r :response} (request-vote state (:body p))]
    (transmit s  {:dst (:src p)
                  :src (:id state)
                  :type :request-vote-reply
                  :body r})))

(defn handle-request-vote-response [state p]
  "response to a request to vote"
  (if (not= :candidate (:type state))
    state
    (let [s (update-in state [:votes]
                       (fn [votes] (assoc votes (:src p) (:vote-granted? (:body p)))))
          c (count (filter identity (vals (:votes s))))]
      (if (> c (/ (inc (count (:peers s))) 2))
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
    (if (and (> c (/ (count (:peers state)) 2)) (>= index (:commit-index state)))
      (assoc state :commit-index index)
      state)))

(defn handle-append-entries-response [state p]
  (-> state
      (update-match-and-next p)
      (check-commit-index)))

(defn handle-packet [state]
  (let [p (first (:rx-queue state))]
    (condp = (:type p)
      :request-vote (handle-request-vote state p)
      :request-vote-reply (handle-request-vote-response state p)
      :append-entries (handle-append-entries state p)
      :append-entries-response (handle-append-entries-response state p))))

(defn process-rx-packets [state]
  (loop [s state]
    (if (empty? (:rx-queue s)) s
        (recur (-> s
                   (handle-packet)
                   (update-in [:rx-queue] rest))))))

(defn update [state dt]
  (-> state
      (update-in [:election-timeout] - dt)
      (apply-commit-index)
      (check-election-timeout)
      (process-rx-packets)
      (leader-heartbeat)))

(defrecord Server
    [current-term voted-for log id tx-queue rx-queue
     commit-index last-applied type election-timeout peers db])

(defn create-server [id]
  (map->Server
   {:current-term 0 ; persist
    :voted-for nil  ; persist
    :log [{:term 0 :cmd :init}]
    :id id          ; user assigned
    :tx-queue '()
    :rx-queue '()
    :commit-index 0
    :last-applied 0
    :type :follower
    :election-timeout (random-election-timeout)
    :peers []
    :db {}}))
