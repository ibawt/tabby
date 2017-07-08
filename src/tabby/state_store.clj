(ns tabby.state-store
  (:require [clojure.tools.logging :refer [warn info]]
            [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [tabby.log :as log]))

(defn- current-date []
  (/ (System/currentTimeMillis) 1000))

(defn- serialize-state [state]
  (select-keys state [:commit-index :current-term :log]))

(defn save [state]
  (when (:data-dir state)
   (with-open [w (io/output-stream
                  (str (:data-dir state) "/" (:id state) "-log-" (current-date) ".bin"))]
     (.write w (nippy/freeze (serialize-state state)))))
  state)

(defn- parse-date [file]
  (let [[_ id _ date] (re-matches #"(.+)-log-(\d+)\.bin" (.getName file) "-")]
    [(Integer/parseInt date) file]))

(defn- by-date [[a _] [b _]]
  (- b a))

(defn- get-latest-backup [state]
  (->>
   (io/as-file (:data-dir state))
   (file-seq)
   (map parse-date)
   (sort by-date)
   (first)
   (second)))

(defn- replay-log [state]
  (loop [s state]
    (if (>= (:last-applied state) (count (:log s)))
      s
      (recur (update :db #(log/apply-entry s %))))))

(defn restore [state]
  (replay-log
   (merge state (nippy/thaw-from-file (io/file (get-latest-backup state))))))
