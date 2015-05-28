(ns tabby.client-state-test
  (:require [tabby.client-state :refer :all]
            [clojure.test :refer :all]))

(deftest create-client-test
  (testing "should make a new client"
    (is (= {:a {:pending-reads {}
                :pending-writes {}
                :history {}
                :socket :foo}} (create-client {} :a :foo))))
  (testing "should add to the map not overwrite it"
    (is (= {:b :a
            :a {:pending-reads {}
                :pending-writes {}
                :history {}
                :socket :foo}} (create-client {:b :a} :a :foo)))))

(deftest pending-reads-test
  (testing "returns true when theres something in pending"
    (is (needs-broadcast? {:a {:pending-reads {:foo :bar}}
                           :b {:pending-reads {}}})))
  (testing "returns false when there is nothing to send"
    (is (not (needs-broadcast? {:a {:pending-reads {}}
                                :b {:pending-reads {}}})))))

(deftest inc-heartbeat-test
  (testing "simple add"
    (is (= {:a {:pending-reads {:id {:hb-count #{1}}}}}
           (inc-heartbeats {:a {:pending-reads {:id {:hb-count #{}}}}} 1))))
  (testing "multiple keys"
    (is (= {:a {:pending-reads {:id {:hb-count #{1,2}}
                                :fo {:hb-count #{2}}}}}
           (inc-heartbeats {:a {:pending-reads {:id {:hb-count #{1}}
                                                :fo {:hb-count #{2}}}}} 2)))))

(deftest check-clients-test
  (testing "no-op"
    (let [s {:peers [0 1 2]
             :clients {:a {:pending-reads {:0 {:hb-count #{}}}
                           :history {}}}
             :tx-queue '()}]
      (is (= s (check-clients s)))))
  (testing "one pkt"
    (let [s {:peers [0 1 2]
             :tx-queue '()
             :db {:foo "foo"}
             :clients {:a {:pending-reads {:0 {:hb-count #{1,2} :key :foo}}}}}]
      (is (= {:peers [0 1 2]
              :clients {:a {:pending-reads {}
                            :history {:0 {:hb-count #{1,2} :key :foo :value "foo"}}}}
              :db {:foo "foo"}
              :tx-queue '({:client-dst :a
                           :uuid :0
                           :value "foo"})} (check-clients s)))))
  (testing "multiple packets one client"
    (is (= {:peers [0 1 2]
            :db {:foo "foo"}
            :clients {:a {:history {:0 {:hb-count #{1,2} :key :foo :value "foo"}
                                    :1 {:hb-count #{1,2} :key :foo :value "foo"}}
                          :pending-reads {}}}
            :tx-queue '({:client-dst :a :uuid :1 :value "foo"}
                        {:client-dst :a :uuid :0 :value "foo"})}
           (check-clients {:peers [0 1 2]
                           :tx-queue '()
                           :db {:foo "foo"}
                           :clients {:a {:pending-reads {:0 {:hb-count #{1,2} :key :foo}
                                                         :1 {:hb-count #{1,2} :key :foo}}}}}))))
  (testing "multiple clients"
    (is (= {:peers [0 1 2]
            :db {:foo "foo"}
            :clients {:a {:history {:0 {:hb-count #{1,2} :key :foo :value "foo"}}
                          :pending-reads {}}
                      :b {:history {:0 {:hb-count #{1,2} :key :foo :value "foo"}}
                          :pending-reads {}}}
            :tx-queue '({:client-dst :b :uuid :0 :value "foo"}
                        {:client-dst :a :uuid :0 :value "foo"})}
           (check-clients {:peers [0 1 2]
                           :db {:foo "foo"}
                           :clients {:a {:pending-reads{:0 {:hb-count #{1,2} :key :foo}}}
                                     :b {:pending-reads{:0 {:hb-count #{1,2} :key :foo}}}}
                           :tx-queue '()})))))

