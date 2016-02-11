(ns tabby.log
  (:require [clojure.tools.logging :refer [warn info]]
            [tabby.utils :as utils]))

;;; Log functions
;;; TODO: this should be + 1
(defn last-log-index
  "return the index of the last log entry (1 based)"
  [state]
  (count (:log state)))

(defn get-log-at
  "gets the log entry at the index specified (1 based)"
  [state idx]
  ;; (warn (:id state) " log = " (:log state))
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
  (if (seq entries)
    (do
      (update state :log (fn [log]
                           (into [] (concat log entries)))))
    state))

(defn append-log [state params]
  (let [s (-> state
              (assoc :election-timeout (utils/random-election-timeout))
              (apply-log (:entries params)))]
    (if (> (:leader-commit params) (:commit-index state))
      (assoc s :commit-index
             (min (:leader-commit params) (inc (count (:log state)))))
      s)))

(defn prev-log-term-equals?
  [state {p-index :prev-log-index p-term :prev-log-term}]
  (try
    (or (= 0 p-index (last-log-index state))
        (= (get-log-term state p-index) p-term))
    (catch Exception ex
      ;; FIXME: shitty
      (warn ex "caught exception in prev-log-term-equals?")
        false)))

(defn apply-entry [state db]
  (let [log (:log state)
        index (:last-applied state)
        {key :key value :value op :op} (:cmd (get log index))]
    (condp = op
      :set (merge db {key value})
      :reset db
      :noop db
      (do
        (warn "[" (:id state) "]" "invalid operation: " op "index: " index " cmd: " (:cmd (get log index)))
        db))))

(defn read-value [state key]
  (get (:db state) key))
