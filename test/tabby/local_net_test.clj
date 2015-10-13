(ns tabby.local-net-test
  (:require [clojure.test :refer :all]
            [tabby.client :as client]
            [tabby.cluster :as cluster]
            [tabby.local-net :refer :all]
            [tabby.utils :as utils]))


;;; TODO: remove the sleep with a poller + timeout

(defn test-cluster []
  (cluster/init-cluster (create-network-cluster 10 9090) 3))


(defmacro with-cluster [bindings & body]
  `(let [~@bindings]
     (try
       ~@body
       (finally
         (cluster/stop-cluster ~(first bindings))))))

(defmacro with-client [bindings & body]
  (let [last-binding (get bindings (- (count bindings) 2))
        klient (or (first last-binding) last-binding)]
    `(let [~@bindings]
       (try
         ~@body
         (finally
           (client/close ~klient))))))

(defn create-client []
  (client/make-local-client [{:host "127.0.0.1" :port 9090}
                             {:host "127.0.0.1" :port 9091}
                             {:host "127.0.0.1" :port 9092}]))

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
  (loop [times 0]
    (cond
      (find-leader c) true
      (> times 1000) false
      :else (do
              (Thread/sleep 10)
              (recur (inc times))))))

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
      (with-client [klient (create-client)
                    [k v] (client/set-or-create klient :a "a")
                    [k' v'] (client/get-value k :a)]
        (is (= {:value :ok} v))
        (is (= {:value "a"} v')))))
  (testing "compare and swap"
    (with-cluster [c (cluster/start-cluster (test-cluster))]
      (wait-for-a-leader c)
       (with-client [klient (create-client)
                     [k v] (client/set-or-create klient :a "a")
                     [k' v'] (client/compare-and-swap k :a "b" "a")
                     [k'' v''] (client/get-value k' :a)
                     [k''' v'''] (client/compare-and-swap k'' :a "c" "a")]
         (is (= {:value :ok} v))
         (is (= {:value :ok} v'))
         (is (= {:value "b"} v''))
         (is (= {:value :invalid-value} v'''))))))
