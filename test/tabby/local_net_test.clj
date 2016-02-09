(ns tabby.local-net-test
  (:require [clojure.test :refer :all]
            [tabby.client :as client]
            [tabby.cluster :as cluster]
            [tabby.local-net :refer :all]
            [tabby.utils :as utils]
            [manifold.deferred :as d]))


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
  (let [t0 (System/currentTimeMillis)]
    (loop []
      (cond
        (find-leader c) true
        (> (- (System/currentTimeMillis) t0) 30000) false
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
      (let [resp @(d/chain
                    (create-client)
                    (fn [klient]
                      (client/set-or-create klient :a "a"))
                    (fn [[klient resp]]
                      (is (= :ok (:value resp)))
                      (client/get-value klient :a))
                    (fn [[klient resp]]
                      resp))]
        (is (= {:value "a"} resp)))))

  (testing "compare and swap"
    (with-cluster [c (cluster/start-cluster (test-cluster))]
      (wait-for-a-leader c)
      (with-client [[k v] (utils/thr (create-client)
                               (client/set-or-create :a "a")
                               (client/compare-and-swap :a "b" "a")
                               (client/get-value :a)
                               (client/compare-and-swap :a "c" "a"))]
        (is (= {:value :ok} (first v)))
        (is (= {:value :ok} (second v)))
        (is (= {:value "b"} (nth v 2)))
        (is (= {:value :invalid-value} (nth v 3)))))))
