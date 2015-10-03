(ns tabby.leader
  (:require [tabby.log :refer :all]
            [tabby.utils :refer :all]
            [clojure.tools.logging :refer :all]))

(defn- heart-beat-timeout []
  (+ (rand-int 15) 15))

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

(defn broadcast-heartbeat [state]
  (foreach-peer state (fn [s [p v]]
                        (transmit s (make-heart-beat-pkt s p)))))

(defn- peer-timeout?
  "checks if the peer is ready to send another heartbeat"
  [state peer]
  (<= (get (:next-timeout state) peer) 0))

(defn- apply-peer-timeouts [state dt]
  (update-in state [:next-timeout] mapf - dt))

(def ^:private peer-next-timeout 75)

(defn- update-peer-timeout [state peer]
  (assoc-in state [:next-timeout peer] peer-next-timeout))

(defn- send-peer-update [state [peer value]]
  (transmit state (if (> (last-log-index state) (get (:match-index state) peer))
                    (do
                      (warn "sending an append log")
                      (make-append-log-pkt state peer))
                    (do
;                      (warn "heart beak pkt")
                      (make-heart-beat-pkt state peer)))))

(defn- broadcast-heart-beat [state]
  (->
   (foreach-peer state send-peer-update)
   (update :next-timeout mapf (constantly peer-next-timeout))))

(defn- update-match-and-next [state p]
  (let [s (if-not (:success (:body p))
            (update-in state [:next-index (:src p)] dec)
            state)]
    (-> (assoc-in s [:match-index (:src p)] (get (:next-index state) (:src p)))
        (update-in [:next-index (:src p)]
                   (fn [next-index]
                     (if (< next-index (inc (count (:log s))))
                       (inc next-index)
                       next-index))))))

(def ^:private highest-match-index
  "Returns frequencies of all of the match indices and the count,
   sorted descending."
  (comp first reverse frequencies vals :match-index))

(defn- check-commit-index [state]
  (let [[index c] (highest-match-index state)]
    (if (and (quorum? (count (:peers state)) (inc c))
             (> index (:commit-index state)))
      (assoc state :commit-index index)
      state)))

(defn- make-peer-map [state f]
  (into {} (for [[p _] (:peers state)]
             [p (f)])))

(defn become-leader [state]
  (warn (:id state) " becoming leader")
  (broadcast-heart-beat
   (merge state
          {:type :leader
           :next-timeout (make-peer-map state (constantly peer-next-timeout))
           :next-index (make-peer-map state  #(inc (count (:log state))))
           :match-index (make-peer-map state (constantly 0))})))

(defn check-and-update-append-entries [state p]
  (check-commit-index (update-match-and-next state p)))

(defn write [state kv]
  (->
   state
   (update :log conj {:term (:current-term state) :cmd kv})
   (broadcast-heart-beat)))

(defn check-backlog
  "broadcast peer updates by checking against
  an internal throttle"
  [state dt]
  (foreach-peer (apply-peer-timeouts state dt)
                (fn [s [p v]]
                  (if (peer-timeout? s p)
                    (-> (send-peer-update s [p])
                        (update-peer-timeout p))
                    s))))
