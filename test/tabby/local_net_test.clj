(ns tabby.local-net-test
  (:require [tabby.local-net :refer :all]
            [tabby.cluster :as cluster]
            [tabby.utils :as utils]
            [clojure.test :refer :all]))

(defn test-cluster []
  (cluster/init-cluster (create-network-cluster 10 9090) 3))

(defmacro with-cluster [bindings & body]
  `(let [~@bindings]
     (try
       ~@body
       (finally
         (cluster/stop-cluster ~(first bindings))))))

(deftest simple-test
  (testing "start elects a leader"
    (with-cluster [c (cluster/start-cluster (test-cluster))]
      (Thread/sleep 500)
      (is (= 1 (count (filter #(= :leader %)
                              (map (fn [[id s]] (:type @s)) (:servers c)))))))))
