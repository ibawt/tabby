(ns tabby.leader
  (:require [tabby.log :refer :all]
            [tabby.utils :refer :all]))

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
  (foreach-peer state #(transmit %1 (make-heart-beat-pkt %1 %2))))

(defn- peer-timeout? [state peer]
  (<= (get (:next-timeout state) peer) 0))

(defn- apply-peer-timeouts [state dt]
  (update-in state [:next-timeout]
             (fn [timeouts]
               (reduce-kv #(assoc %1 %2 (- %3 dt)) {} timeouts))))

(defn- update-peer-timeout [state peer]
  (update-in state [:next-timeout peer] + 10))

(defn- send-peer-update [state peer]
  (transmit state (if (> (last-log-index state) (get (:match-index state) peer))
                    (make-append-log-pkt state peer)
                    (make-heart-beat-pkt state peer))))


(defn- broadcast-heart-beat [state]
  (foreach-peer state (fn [s p] (send-peer-update s p))))

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

(defn become-leader [state]
  (->
   state
   (assoc :type :leader)
   (assoc :next-timeout (reduce #(merge %1 {%2 (heart-beat-timeout)}) {} (:peers state)))
   (assoc :next-index (reduce #(merge %1 {%2 (inc (count (:log state)))}) {}
                              (:peers state)))
   (assoc :match-index (reduce #(merge %1 {%2 0}) {} (:peers state)))
   (broadcast-heart-beat)))

(defn handle-append-entries-response [state p]
  (if (pos? (-> p :body :count)) ; heart beat response
    (-> state
        (update-match-and-next p)
        (check-commit-index))
    state))

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
