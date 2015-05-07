(ns tabby.leader
  (:require [tabby.log :refer :all]
            [tabby.utils :refer :all]
            [clojure.tools.logging :refer :all]))

(defn- heart-beat-timeout []
  (inc (rand-int 9)))

(defn- make-append-log-pkt [state peer]
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

(defn- make-heart-beat-pkt [state peer]
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

(defn- broadcast-heartbeat [state]
  (foreach-peer state (comp transmit make-heart-beat-pkt)))

(defn- peer-timeout? [state peer]
  (<= (get (:next-timeout state) peer) 0))

(defn- apply-peer-timeouts [state dt]
  (update-in state [:next-timeout] mapf - dt))

(defn- update-peer-timeout [state peer]
  (assoc-in state [:next-timeout peer] 75)) ; TODO: this shouldn't be so high

(defn- send-peer-update [state peer]
  (transmit state (if (> (last-log-index state) (get (:match-index state) peer))
                    (make-append-log-pkt state peer)
                    (make-heart-beat-pkt state peer))))

(defn- broadcast-heart-beat [state]
  (foreach-peer state send-peer-update))

(defn- update-match-and-next [state p]
  (let [s (if-not (:success (:body p))
            (update-in state [:next-index (:src p)] dec)
            state)]
    (assoc-in s [:match-index (:src p)] (get (:next-index state) (:src p)))))

(defn- check-commit-index [state]
  (let [f (frequencies (vals (:match-index state)))
        [index c] (first f)]
    (if (and (quorum? state (inc c)) (> index (:commit-index state)))
      (assoc state :commit-index index)
      state)))

(defn- make-peer-map [state f]
  (into {} (for [p (:peers state)]
             [p (f)])))

(defn become-leader [state]
  (warn (:id state) " becoming leader")
  (->
   state
   (assoc :type :leader)
   (assoc :next-timeout (make-peer-map state heart-beat-timeout))
   (assoc :next-index (make-peer-map state  #(inc (count (:log state)))))
   (assoc :match-index (make-peer-map state (constantly 0)))
   (broadcast-heart-beat)))

(defn inc-hb-counts
  [state p]
  (update-in state [:clients] #(map (fn [client]
                                      (update-in client [:hb-count] conj (:src p))) %)))

(defn check-read
  [state client index]
  (if (quorum? state (count (:hb-count client)))
    (transmit state {:src (:id state) :client-dst index :body {:foo "bar"}})
    state))

(defn check-reads
  [state]
  (loop [s state
         index 0
         c (:clients state)]
    (if (empty? c)
      s
      (recur (check-read state (first c) index)
             (inc index)
             (rest c)))))

(defn handle-append-entries-response [state p]
  (if (pos? (get-in p [:body :count])) ; heart beat response
    (-> state
        (update-match-and-next p)
        (check-commit-index))
    (inc-hb-counts state p)))

(defn client-read
  [state {client-id :client-id key :key uuid :uuid}]
  (if (= uuid (get-in state [:clients client-id :last-uuid]))
    (get-in state [:clients client-id :last-response])
    (-> (update-in state [:clients client-id] merge {:hb-count #{}
                                                     :key key
                                                     :uuid uuid})
        (broadcast-heart-beat))))

(defn write [state kv]
  (->
   state
   (update-in [:log] conj {:term (:current-term state) :cmd kv})
   (broadcast-heart-beat)))

(defn check-backlog
  "broadcast peer updates by checking against
  an internal throttle"
  [state dt]
  (foreach-peer (apply-peer-timeouts state dt)
                (fn [s p]
                  (if (peer-timeout? s p)
                    (-> s
                        (send-peer-update p)
                        (update-peer-timeout p))
                    s))))
