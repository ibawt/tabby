(ns user
  (:require [tabby.server :as server]
            [tabby.client :as client]
            [tabby.utils :as utils]
            [tabby.cluster :as cluster]
            [clojure.tools.logging :refer :all]
            [tabby.local-net :as local-net]
            [clojure.tools.namespace.repl :refer [refresh]]))

(defn cluster-maker
  "Makes a cluster"
  []
  (local-net/create-network-cluster 10 8090))

(def cluster
  (cluster-maker))

(defn- local-client []
  (client/make-local-client [{:host "127.0.0.1" :port 8090}
                             {:host "127.0.0.1" :port 8091}
                             {:host "127.0.0.1" :port 8092}]))

(def klient (local-client))

(defn unatom [x]
  (if (instance? clojure.lang.Atom x)
    @x
    x))

(defmacro setc [& body]
  `(alter-var-root #'cluster
                   (fn ~@body)))

(defn init []
  (setc [c] (cluster/init-cluster c 3)))

(defn step [dt]
  (setc [c] (cluster/step-cluster c dt)))

(defn servers []
  (:servers cluster))

(defn start []
  (info "----------------------------------------------------")
  (setc [c] (cluster/start-cluster c)))

(defn stop []
  (setc [c] (cluster/stop-cluster c)))

(defn go
  []
  (init)
  (start)
  :ready)

(defn to-name [x]
  (if (instance? String x)
    x
    (str x ".localnet:" x)))

(defn kill
  "not done yet"
  [n]
  (alter-var-root #'cluster cluster/kill-server (to-name n)))

(defn rez
  "not done yet"
  [n]
  (alter-var-root #'cluster cluster/rez-server (to-name n)))

(defn reset []
  (try
    (stop)
    (alter-var-root #'cluster (fn [s] (cluster-maker)))
    (alter-var-root #'klient (fn [s] (local-client)))
    (refresh :after 'user/go)
    (catch Exception e
      (warn e "caught exception in reset!!!"))))

(defn set-value [key value]
  (let [[kk {code :value}] @(client/set-or-create klient key value)]
    (when kk
      (alter-var-root #'klient (constantly kk)))
    code))

(defn get-value [key]
  (let [[kk {value :value}] @(client/get-value klient key)]
    (alter-var-root #'klient (constantly kk))
    value))

(defn compare-and-swap [key new old]
  (let [[kk {value :value}] @(client/compare-and-swap klient key new old)]
    (alter-var-root #'klient (constantly kk))
    value))

(defn server-at [key]
  (unatom (get (:servers cluster) key)))

(defn types []
  (map (fn [x]
         (-> (unatom x)
             (select-keys [:type :id]))) (vals (:servers cluster))))

(defn logs []
  (map (fn [x]
         (-> (unatom x)
             (select-keys [:log :id :last-applied]))) (vals (:servers cluster))))

(defn find-leader []
  (reduce-kv (fn [_ k v]
               (if (= :leader (:type (unatom v)))
                 (reduced [k v]) _)) nil (:servers cluster)))

(defn leader-clients []
  (:clients (unatom (second (find-leader)))))

(defn followers []
  (map first (filter (fn [[k v]]
                       (not= :leader (:type (unatom v)))) (:servers cluster))))

(defn kill-random-follower []
  (let [id (first (shuffle (followers)))]
    (kill id)
    id))

(defn while-not-leader []
  (future (loop [l (find-leader)]
            (Thread/yield)
            (if l
              l
              (recur (find-leader))))))

(defn testy []
  (reset)
  @(while-not-leader)
  (println "leader: " (first (find-leader)))
  (assert (= :ok (set-value :a "a")))
  (kill-random-follower)
  (assert (= :ok (set-value :b "b"))))
