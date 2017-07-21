(ns tabby.local-net-test
  (:require [clojure.test :refer :all]
            [tabby.client :as client]
            [clojure.tools.logging :refer [warn info]]
            [tabby.cluster :as cluster]
            [tabby.local-net :refer :all]
            [tabby.utils :as utils]
            [manifold.time :as t]
            [manifold.deferred :as d]))


(defn test-cluster []
  (-> (create-network-cluster 20 9495)
      (cluster/init-cluster 3)
      (update :servers utils/mapf assoc :data-dir nil)
      (assoc-in [:servers "0.localnet:0" :election-timeout] 0)
      (assoc-in [:servers "1.localnet:1" :election-timeout-fn]
                (constantly 150))
      (assoc-in [:servers "2.localnet:2" :election-timeout-fn]
                (constantly 300))))

(defmacro with-cluster [bindings & body]
  `(let [~@bindings]
     (try
       ~@body
       (finally
         (cluster/stop-cluster ~(first bindings))))))

(defmacro with-client [bindings & body]
  `(let [~@bindings]
     (try
      ~@body
      (finally
        (client/close! ~(first bindings))))))

(defn create-client [& {:keys [timeout] :or {timeout 15000}}]
  (client/make-network-client [{:host "127.0.0.1" :port 9495}
                               {:host "127.0.0.1" :port 9496}
                               {:host "127.0.0.1" :port 9497}]
                              :timeout timeout))

(defn create-http-client [& {:keys [timeout] :or {timeout 15000}}]
  (client/make-http-client [{:host "127.0.0.1" :http-port 10495}
                            {:host "127.0.0.1" :http-port 10496}
                            {:host "127.0.0.1" :http-port 10497}]
                              :timeout timeout) )

(defn- unatom [x]
  (if (instance? clojure.lang.Atom x)
    @x
    x))

(defn- find-leader [c]
  (let [[k v] (first (filter
                      (fn [[k v]]
                        (= :leader (:type @v))) (:servers c)))]
    (if v @v v)))

(defn wait-for-a-leader [c]
  (let [t0 (System/currentTimeMillis)]
    (loop []
      (cond
        (find-leader c) true
        (> (- (System/currentTimeMillis) t0) 30000) (throw (RuntimeException. "can't find a leader"))
        :else (do
                (Thread/yield)
                (recur))))))

(deftest simple-test
  (testing "start elects a leader"
    (with-cluster [c (cluster/start-cluster (test-cluster))]
      (is (wait-for-a-leader c))
      (is (= 1 (count (filter #(= :leader %)
                              (map (fn [[id s]] (:type @s)) (:servers c))))))
      (is (find-leader c))))
  (testing "start and write a value and get it back"
    (with-cluster [c (cluster/start-cluster (test-cluster))]
      (wait-for-a-leader c)
      (with-client [c (create-client)]
        (is (client/success? (client/set-value! c :a "a")))
        (is (= "a" (:value (client/get-value! c :a)))))))

  (testing "compare and swap"
    (with-cluster [c (cluster/start-cluster (test-cluster))]
      (wait-for-a-leader c)
      (with-client [c (create-client)]
        (is (client/success? (client/set-value! c :a "a")))
        (is (client/success? (client/compare-and-swap! c :a "b" "a")))
        (is (= "b" (:value (client/get-value! c :a))))
        (is (= :invalid-value (:value (client/compare-and-swap! c :a "c" "a"))))))))

(defmacro with-dead-server [c id & body]
  `(try
     (cluster/kill-server ~c ~id)
     ~@body
     (finally
       (cluster/rez-server ~c ~id))))

(deftest simple-failures
  (testing "leaders goes down"
    (doseq [client [(create-http-client :timeout 1000)
                    (create-client :timeout 1000)]]
      (with-cluster [c (cluster/start-cluster (test-cluster))]
        (is (wait-for-a-leader c))
        (with-client [k client]
          (with-dead-server c (:id (find-leader c))
            (is (wait-for-a-leader c))
            (is (client/success? (client/set-value! k :a "a")))
            (with-dead-server c (:id (find-leader c))
              (is (= :timeout (client/set-value! k :b 1))))
            (is (wait-for-a-leader c))
            (is (client/success? (client/set-value! k :c 2))))
          (is (= (map (fn [x] (:log @x)) (:servers c)))))))))
