(ns tabby.client-state-test
  (:require [clojure.test :refer :all]
            [tabby.client-state :refer :all]))

(deftest create-client-test
  (testing "should make a new client"
    (is (= {:a {:pending-read {}
                :pending-write {}
                :history {}
                :socket :foo}} (create-client {} :a :foo))))
  (testing "should add to the map not overwrite it"
    (is (= {:b :a
            :a {:pending-read {}
                :pending-write {}
                :history {}
                :socket :foo}} (create-client {:b :a} :a :foo)))))

(deftest pending-reads-test
  (testing "returns true when theres something in pending"
    (is (needs-broadcast? {:a {:pending-read {:foo :bar}}
                           :b {:pending-read {}}})))
  (testing "returns false when there is nothing to send"
    (is (not (needs-broadcast? {:a {:pending-read {}}
                                :b {:pending-read {}}})))))

(deftest inc-heartbeat-test
  (testing "simple add"
    (is (= {:a {:pending-read {:hb-count #{1}}}}
           (inc-heartbeats {:a {:pending-read {:hb-count #{}}}} 1)))
    (is (= {:a {:pending-read {:hb-count #{1,2}}}}
           (inc-heartbeats {:a {:pending-read {:hb-count #{1}}}} 2)))
    (is (= {:a {:pending-read {:hb-count #{1}}}
            :b {:pending-read {:hb-count #{1}}}}
           (inc-heartbeats {:a {:pending-read {:hb-count #{}}}
                            :b {:pending-read {:hb-count #{}}}} 1)))))

(deftest check-clients-test
  (testing "no-op"
    (let [s {:peers [0 1 2]
             :clients {:a {:pending-write {}
                           :pending-read {:0 {:hb-count #{}}}
                           :history {}}}
             :tx-queue '()}]
      (is (= s (check-clients s)))))
  (testing "one pkt"
    (let [s {:peers [0 1 2]
             :tx-queue '()
             :db {:foo "foo"}
             :clients {:a {:pending-read {:hb-count #{1,2} :key :foo :uuid :0}
                           :pending-write {}}}}]
      (is (= {:peers [0 1 2]
              :clients {:a {:pending-read {}
                            :pending-write {}
                            ;:history {:0 {:key :foo :value "foo"}}
                            }}
              :db {:foo "foo"}
              :tx-queue '({:client-dst :a
                           :uuid :0
                           :body {:value "foo"}})} (check-clients s)))))
  (testing "multiple clients"
    (is (= {:peers [0 1 2]
            :db {:foo "foo"}
            :clients {:a {
                          ;:history {:0 {:hb-count #{1,2} :key :foo :value "foo"}}
                          :pending-read {}
                          :pending-write {}}
                      :b {
                         ; :history {:0 {:hb-count #{1,2} :key :foo :value "foo"}}
                          :pending-read {}
                          :pending-write {}}}
            :tx-queue '({:client-dst :b :uuid :0 :body {:value "foo"}}
                        {:client-dst :a :uuid :0 :body {:value "foo"}})}
           (check-clients {:peers [0 1 2]
                           :db {:foo "foo"}
                           :clients {:a {:pending-write {}
                                         :pending-read {:uuid :0 :hb-count #{1,2} :key :foo}}
                                     :b {:pending-write {}
                                         :pending-read {:uuid :0 :hb-count #{1,2} :key :foo}}}
                           :tx-queue '()})))))

(deftest pending-writes
  (let [default-state {:peers {1 {} 2 {}} :tx-queue '() :db {}
                       :clients {:a {:pending-write {}
                                     :history {}
                                     :pending-read {}}}
                       :commit-index 0}]
    (testing "no quorum on log commit index"
      (let [s (assoc-in default-state [:clients :a :pending-write :target-commit-index] 1)]
        (is (= s (check-clients s)))))
    (testing "sending a client response"
      (let [s (-> (assoc-in default-state [:clients :a :pending-write :target-commit-index] 1)
                  (assoc :commit-index 1)
                  (check-clients))]
        (is (= :a (:client-dst (first (:tx-queue s)))))
        (is (pos? (count (:tx-queue s))))
        (is (= :a (:client-dst (first (:tx-queue s)))))
        (is (zero? (count (get-in s [:clients :a :pending-writes]))))))))

(deftest compare-and-swap
  (let [default-state {:peers {1 {} 2 {}} :tx-queue '() :db {}
                       :clients {:a {:pending-write {}
                                     :history {}
                                     :pending-read {}}}
                       :match-index {1 0 2 0}
                       :next-index {1 1 2 1}
                       :commit-index 0}]
    (testing "adding a cas operation"
      (let [s (-> (add-cas default-state {:type :cas :body {:key :foo :old "bar" :new "baz"}
                                          :uuid "thingy" :client-id "client-0"}))
            client (get-in s [:clients "client-0"])]
        (is (=  {:pending-read {:hb-count #{}
                                :uuid "thingy"
                                :key :foo
                                :cas {:key :foo :old "bar" :new "baz"}}} client))))
    (testing "will turn into a write if the value is equal to the old"
      (let [s (add-cas default-state {:type :cas :body {:key :foo :old nil :new "bar"}
                                       :uuid "thingy" :client-id "client-0"})
            s' (-> (update s :clients inc-heartbeats 1)
                   (update :clients inc-heartbeats 2)
                   (check-clients))
            client (get-in s' [:clients "client-0"])]
        (is (= {:pending-read {}
                :pending-write {:uuid "thingy" :target-commit-index 1}} client))))
    (testing "will return an version mismatch error if the value is not equal to the old"
      (let [s (add-cas default-state {:type :cas :body {:key :foo :old "baz" :new "bar"}
                                        :uuid "thingy" :client-id "client-0"})
            s' (-> (update s :clients inc-heartbeats 1)
                   (update :clients inc-heartbeats 2)
                   (check-clients))
            client (get-in s' [:clients "thingy"])]
        (is (= {:client-dst "client-0" :uuid "thingy" :body {:value :invalid-value}
                :type :cas-reply} (first (:tx-queue s'))))))
    (testing "will write the result of the set to the client"
      (let [s (add-cas default-state {:type :cas :body {:key :foo :old nil :new "bar"}
                                        :uuid "thingy" :client-id "client-0"})
            s' (-> (update s :clients inc-heartbeats 1)
                   (update :clients inc-heartbeats 2)
                   (check-clients)
                   (assoc :commit-index 1)
                   (assoc :db {:foo "bar"})
                   (check-clients))
            client (get-in s' [:clients "client-0"])]
        (is (= {:pending-read {}
                :pending-write {}} client))
        (is (= {:client-dst "client-0"
                :type :write-reply
                :uuid "thingy"
                :body {:value :ok}} (first (:tx-queue s'))))))))
