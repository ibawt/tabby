(ns tabby.server-test
  (:require [clojure.test :refer :all]
            [tabby.server :refer :all]))

(deftest create-server-test
  (testing "initial state"
    (let [s (create-server 42)]
      (is (= 0 (:current-term s)))
      (is (= nil (:voted-for s)))
      (is (= [{:term 0 :cmd {:op :reset}}] (:log s)))
      (is (= 0 (:commit-index s)))
      (is (= 42 (:id s)))
      (is (= 0 (:last-applied s)))
      (is (= :follower (:type s)))
      (is (= {} (:peers s)))
      (is (= {} (:db s))))))
