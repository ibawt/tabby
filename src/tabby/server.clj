(ns tabby.server)

(defmacro dbg [& body]
  `(let [x# ~body]
     (println (quote ~body) "=" x#) x#))

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
(defn append-log [state params]
  (let [s (-> state
              (assoc :election-timeout (random-election-timeout))
              (update-in [:log] conj (:entries params)))]
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
     :state (if (:vote-granted? r) (assoc state :voted-for (:candidate-id params)) s)}))

(defn write [state kv]
  state
  )

(defn get-log-index [state]
  (count (:log state)))

(defn get-log-term [state]
  (:term (peek (:log state))))

(defn make-heart-beat-pkt [state peer]
  (let [p-index (dec (count (:log state)))
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
  (loop [s state
         p (:peers state)]
    (if (empty? p)
      s
      (recur (transmit s (make-heart-beat-pkt s (first p)))
             (rest p)))))

(defn trace-s [state]
  (when (= (:id state) 0)
    (println (select-keys state [:id :type :election-timeout :current-term :commit-index] )))
  state)

(defn become-leader [state]
  (println (:id state) " becoming leader")
  (->
   state
   (assoc :type :leader)
   (assoc :next-index (vec (repeat (count (:peers state)) (:last-applied state))))
   (assoc :match-index (vec (repeat (count (:peers state)) 0)))
   (broadcast-heartbeat)))

(defn election-timeout? [state]
  (<= (:election-timeout state) 0))

(defn set-peers [state peers]
  (assoc state :peers peers))

(defn apply-commit-index [state]
  (when (> (:commit-index state) (:last-applied state))
    ; TODO: actually do the operation in the log
    (update-in state [:last-applied] inc))
  state)

(defn broadcast-request-vote [state]
  (loop [s state
         p (:peers state)]
    (if (empty? p)
      s
      (recur (transmit s (make-request-vote-pkt s (first p)))
             (rest p)))))

(defn check-election-timeout [state]
  (if (election-timeout? state)
    (do
      (println (:id state) " triggered election")
      (-> state
          (assoc :type :candidate)
          (assoc :voted-for (:id state))
          (update-in [:current-term] inc)
          (assoc :election-timeout (random-election-timeout))
          (broadcast-request-vote)
          (assoc :votes {})))
    state))

(defn leader-heartbeat [state]
  (if (= :leader (:type state))
    (broadcast-heartbeat state)
    state))

(defn handle-request-vote [state p]
  "incoming request to for a vote"
  (let [r (request-vote state (:body p))]

    (transmit (:state r)  {:dst (:src p)
                           :src (:id state)
                           :type :request-vote-reply
                           :body (:response r)})))

(defn handle-request-vote-response [state p]
  "response to a request to vote"
  (let [s (update-in state [:votes] (fn [votes] (assoc votes (:src p) (:vote-granted? (:body p)))))
        c (count (filter identity (vals (:votes s))))]
    (if (> c (/ (count (:peers s)) 2))
      (become-leader s)
      s)))

(defn handle-append-entries [state p]
  (let [r (append-entries state (:body p))]
    (transmit (:state r) {:dst (:src p) :src (:id state)
                          :type :append-entries-response
                          :body (:result r)})
    (:state r)))

(defn handle-append-entries-response [state p]
  state)

(defn handle-packet [state]
  (let [p (first (:rx-queue state))]
    (condp = (:type p)
      :request-vote (handle-request-vote state p)
      :request-vote-reply (handle-request-vote-response state p)
      :append-entries (handle-append-entries state p)
      :append-entries-reply (handle-append-entries-response state p))))

(defn process-rx-packets [state]
  (loop [s state]
    (if (empty? (:rx-queue s)) s
        (recur (-> s
                   (handle-packet)
                   (update-in [:rx-queue] rest))))))

(defn update [state dt]
  (-> state
      (leader-heartbeat)
      (update-in [:election-timeout] - dt)
      (apply-commit-index)
      (check-election-timeout)
      (process-rx-packets)))

(defn create-server [id]
  {:current-term 0 ; persist
   :voted-for nil  ; persist
   :log [{:term 0 :cmd :init}]         ; persist start at 1
   :id id          ; user assigned
   :tx-queue '()
   :rx-queue '()
   :commit-index 0
   :last-applied 0
   :type :follower
   :election-timeout id;(random-election-timeout)
   :peers []
   :db {}})
