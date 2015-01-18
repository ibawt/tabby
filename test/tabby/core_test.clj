(ns tabby.core-test
  (:require [clojure.test :refer :all]
            [tabby.core :refer :all]))

(deftest test-all-the-things
  (init)
  (testing "everyone's type"
    (is (= '(:follower :follower :follower) (map :type (:servers @cluster-states)))))
  (testing "first election"
    (update-in-srv 0 :election-timeout (constantly 0))
    (step 0)
    (is (= :candidate (:type (srv 0))))
    (is (= :follower (:type (srv 1))))
    (is (= :follower (:type (srv 2))))
    (is (> (:election-timeout (srv 0)) 0)))
  (testing "1 - 2 vote"
    (step 0) ; process vote prepare reply
    (step 0) ; process replies
    (is (= '(:leader :follower :follower) (map :type (servers))))))
