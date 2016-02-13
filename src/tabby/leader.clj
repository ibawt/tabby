(ns tabby.leader
  (:require [tabby.log :as log]
            [tabby.utils :as utils]
            [clojure.tools.logging :refer [warn info]]))

(defn- heart-beat-timeout []
  (+ (rand-int 15) 15))

(defn- make-append-log-pkt [state peer]
  (assert peer "peer shouldn't be nil")
  (let [prev-index (dec (get (:next-index state) peer))
        prev-term (log/get-log-term state prev-index)]
    {:dst peer
     :type :append-entries
     :src (:id state)
     :body {:term (:current-term state)
            :leader-id (:id state)
            :prev-log-index prev-index
            :prev-log-term prev-term
            :entries [(log/get-log-at state (inc prev-index))]
            :leader-commit (:commit-index state)}}))

(defn- make-heart-beat-pkt [state peer]
  (assert peer "peer shouldn't be nil")
  (let [p-index (count (:log state))
        p-term (log/get-log-term state p-index)]
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
  (utils/foreach-peer state (fn [s [p v]]
                              (utils/transmit s (make-heart-beat-pkt s p)))))

(defn- peer-timeout?
  "checks if the peer is ready to send another heartbeat"
  [state peer]
  (<= (get-in state [:next-timeout peer]) 0))

(defn- apply-peer-timeouts [state dt]
  (update state :next-timeout utils/mapf - dt))

(def ^:private default-peer-next-timeout 30)

(defn- peer-next-timeout [state]
  (or (:peer-next-timeout state) default-peer-next-timeout))

(defn- update-peer-timeout [state peer]
  (assoc-in state [:next-timeout peer] (peer-next-timeout state)))

(defn- send-peer-update [state [peer value]]
  (let [match-index (get-in state [:match-index peer])
        last-log-index (log/last-log-index state)]
    (utils/transmit state (if (> last-log-index match-index)
                            (make-append-log-pkt state peer)
                            (make-heart-beat-pkt state peer)))))

(defn- broadcast-heart-beat [state]
  (->
   (utils/foreach-peer state send-peer-update)
   (update :next-timeout utils/mapf (fn [_]
                                      (peer-next-timeout state)))))

(defn- update-match-and-next [state p]
  (let [s (if-not (:success (:body p))
            (update-in state [:next-index (:src p)] dec)
            (assoc-in state [:match-index (:src p)]
                      (get-in state [:next-index (:src p)])))]
    (update-in s [:next-index (:src p)]
               (fn [next-index]
                 (if (< next-index (inc (count (:log s))))
                   (inc next-index)
                   next-index)))))

(defn- match-sort
  "Sorting function for a collection of [index freq] pairs.
   If the frequencies are equivalent the highest index is taken."
  [[a-index a-freq] [b-index b-freq]]
  (if (= a-freq b-freq)
    (- b-index a-index)
    (- b-freq a-freq)))

(def highest-match-index
  "Returns frequencies of all of the match indices and the count,
   sorted descending."
  (comp first (partial sort match-sort) frequencies vals :match-index))

(defn- check-commit-index [state]
  (let [[index c] (highest-match-index state)]
    (if (and index c
         (utils/quorum? (count (:peers state)) (inc c))
         (> index (:commit-index state)))
      (assoc state :commit-index index)
      state)))

(defn- make-peer-map [state f]
  (into {} (for [[p _] (:peers state)]
             [p (f)])))

(defn check-and-update-append-entries [state p]
  (check-commit-index (update-match-and-next state p)))

(defn write
  "Appends the command into the log and then broadcasts
   apply entries to all peers"
  [state cmd]
  (->
   (update state :log conj {:term (:current-term state) :cmd cmd})
   (broadcast-heart-beat)))

(defn become-leader [state]
  (warn (:id state) " becoming leader: term = " (:current-term state))
  (-> (update state :log conj {:term (:current-term state) :cmd {:op :noop}})
      (merge
          {:type :leader
           :leader-id (:id state)
           :next-timeout (make-peer-map state (fn [] (peer-next-timeout state)))
           :next-index (make-peer-map state  #(inc (count (:log state))))
           :match-index (make-peer-map state (constantly 0))})
      (broadcast-heart-beat)))

(defn check-backlog
  "broadcast peer updates by checking against
  an internal throttle"
  [state dt]
  (utils/foreach-peer (apply-peer-timeouts state dt)
                (fn [s [p v]]
                  (if (peer-timeout? s p)
                    (-> (send-peer-update s [p])
                        (update-peer-timeout p))
                    s))))
