(ns tabby.state-store
  (:require [clojure.tools.logging :refer [warn info]]
            [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [tabby.log :as log]))

(defn- serialize-state [state]
  (select-keys state [:commit-index :current-term :log]))

(defn- log-file [state]
  (str (:data-dir state) "/" (:id state) "-log.bin"))

(defn save [state]
  (if (and (:data-dir state) (> (:last-applied state) (or (:last-saved-index state) 0)))
   (with-open [w (io/output-stream (log-file state))]
     (info (:id state) " writing log to disk")
     (.write w (nippy/freeze (serialize-state state)))
     (assoc state :last-saved-index (:last-applied state)))
   state))

(defn- replay-log [state]
  (loop [s state]
    (if (>= (:last-applied state) (count (:log s)))
      (do
        (info (:id state) " applied log up to: " (:last-applied s))
        s)
      (recur (-> (update s :db #(log/apply-entry s %))
                (update :last-applied inc))))))

(defn restore [state]
  (let [file (io/file (log-file state))]
    (if (.exists file)
      (replay-log
       (merge state (nippy/thaw-from-file file)))
      (do
        (warn (:id state) " log not found")
        state))))
