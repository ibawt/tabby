(ns tabby.leader
  (:require [tabby.log :as log]
            [tabby.utils :as utils]
            [clojure.tools.logging :refer [warn info]]))

(defn- heart-beat-timeout []
  (+ (rand-int 15) 15))

(defn- make-append-log-pkt [state peer]
  (let [prev-index (dec (get (:next-index state) peer))
        prev-term (log/get-log-term state prev-index)]
    ;; (warn "sending to:" peer " log[" p-index "] term = " p-term)
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
  (<= (get (:next-timeout state) peer) 0))

(defn- apply-peer-timeouts [state dt]
  (update state :next-timeout utils/mapf - dt))

(def ^:private peer-next-timeout 35)

(defn- update-peer-timeout [state peer]
  (assoc-in state [:next-timeout peer] peer-next-timeout))

(defn- send-peer-update [state [peer value]]
  (utils/transmit state
            (if (> (log/last-log-index state) (get (:match-index state) peer))
              (make-append-log-pkt state peer)
              (make-heart-beat-pkt state peer))))

(defn- broadcast-heart-beat [state]
  (->
   (utils/foreach-peer state send-peer-update)
   (update :next-timeout utils/mapf (constantly peer-next-timeout))))

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
    (if (and (utils/quorum? (count (:peers state)) (inc c))
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
  (info (:id state) " becoming leader: term = " (:current-term state))
  (-> (update state :log conj {:term (:current-term state) :cmd {:op :noop}})
      (merge
          {:type :leader
           :next-timeout (make-peer-map state (constantly peer-next-timeout))
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
