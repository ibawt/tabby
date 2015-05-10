(ns tabby.log
  (:require [tabby.utils :refer :all]))

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

(defn prev-log-term-equals?
  [state {p-index :prev-log-index p-term :prev-log-term}]
  (or (= 0 p-index (last-log-index state))
      (= (get-log-term state p-index) p-term)))

(defn apply-entry [{log :log index :last-applied} db]
  (let [{key :key value :value} (:cmd (get log index))]
    (merge db {key value})))

(defn read-value [state key]
  (get (:db state) key))
