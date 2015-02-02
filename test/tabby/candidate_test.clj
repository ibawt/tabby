(ns tabby.candidate-test
  (:require [clojure.test :refer :all]
            [tabby.candidate :refer :all]))

(def state
  {:last-applied 0,
   :db {},
   :peers [1 2],
   :tx-queue '(),
   :type :follower,
   :id 0,
   :election-timeout 0,
   :commit-index 0,
   :rx-queue '(),
   :current-term 0,
   :log []})

(deftest test-become-candidate
  (let [s (become-candidate state)]
    (testing "type should be candidate"
      (is (= :candidate (:type s))))
    (testing "set voted-for to myself"
      (is (= 0 (:voted-for s)))
      (is (= {0 true} (:votes s))))
    (testing "current term will be incremented"
      (is (= 1 (:current-term s))))
    (testing "election-timeout gets reset"
      (is (pos? (:election-timeout s))))
    (testing "request-vote rpcs will be queued")
    ))
