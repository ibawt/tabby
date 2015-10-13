(ns tabby.core
  (:require [tabby.cluster :as cluster]
            [tabby.local-net :as local-net]
            [clojure.tools.logging :refer [warn info]]
            [tabby.net :as net]
            [tabby.server :as server]
            [tabby.utils :as u])
  (:gen-class))

(def test-args
  ["-port" "1337" "-peers" "foo:1,foo:2,foo:3"])

(def expected
  {:port 1337
   :peers [{:hostname "foo" :port 1}
           {:hostname "foo" :port 2}
           {:hostname "foo" :port 3}]})

(defn- parse-int [x]
  (Integer/parseInt x))

(defn- parse-peers [^String s]
  (map (fn [^String x]
         (let [[hostname port] (.split x ":")]
           {:hostname hostname
            :id x
            :port (parse-int port)})) (.split s ",")))

(defn- convert [options]
  (-> options
      (update :port parse-int)
      (update :peers parse-peers)))

(defn- parse-args [args]
  (loop [args args
         opts {}]
    (if (empty? args)
      opts
      (let [^String a (first args)]
        (if (= (first a) \-)
          (recur (rest (rest args))
                 (assoc opts (keyword (.substring a 1)) (second args))))))))

(defn -main [& args]
  (try
    (let [options (parse-args args)]
     (-> (server/create-server (str (:hostname options) ":" (:port options)))
         (net/start-server (:port options))
         (server/set-peers (:peers options))))
    (catch Exception e
      (warn e "man caught exception exiting..."))))
