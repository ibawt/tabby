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
    (init)
    (update-in-srv 0 :election-timeout (constantly 0))
    (step 0)
    (step 0)
    (step 0)
    (step 0)
    (is (= '(:leader :follower :follower) (map :type (servers))))
    (is (= '(1 1 1) (map :current-term (servers)))))

  (testing "commit-index will updated when consesus is made"
    (init)
    (update-in-srv 0 :election-timeout (constantly 0))
    (is (= '(0 0 0) (map :commit-index (servers))))
    (is (= '(0 0 0) (map :current-term (servers))))
    (is (= '(0 0 0) (map :last-applied (servers))))
    (step 0) ; step 1 become candidate & send request-vote
    (is (= '(:candidate :follower :follower) (map :type (servers))))
    (is (= '(1 0 0) (map :current-term (servers))))
    (is (= '({:dst 2 :src 0 :type :request-vote
              :body {:term 1 :candidate-id 0, :last-log-index 0
                     :last-log-term 0}}
             {:dst 1 :src 0 :type :request-vote
              :body {:term 1 :candidate-id 0, :last-log-index 0
                     :last-log-term 0}}) (:tx-queue (srv 0))))
    (step 0) ; others respond to request-vote
    (is (= '(1 1 1) (map :current-term (servers))))
    (is (= '(0 0 0) (map :voted-for (servers))))
    (is (= '({:dst 0 :src 1 :type :request-vote-reply
              :body {:term 1 :vote-granted? true}}) (:tx-queue (srv 1)) ))
    (is (= '({:dst 0 :src 2 :type :request-vote-reply
              :body {:term 1 :vote-granted? true}}) (:tx-queue (srv 2))))
    (step 0) ; process request-vote become leader, send heart beat
    (is (= '(:leader :follower :follower) (map :type (servers))))
    (is (= '({:dst 2 :type :append-entries :src 0
              :body {:term 1 :leader-id 0
                     :prev-log-index 1 :prev-log-term nil
                     :entries [] :leader-commit 0}}
             {:dst 1 :type :append-entries :src 0
              :body {:term 1 :leader-id 0
                     :prev-log-index 1 :prev-log-term nil
                     :entries [] :leader-commit 0}})
           (:tx-queue (srv 0))))
    (is (= {2 0 1 0} (:match-index (srv 0))))
    (is (= {2 1 1 1} (:next-index (srv 0))))
    (step 0) ; get heart beat commits
    (is (= '({:dst 0 :src 1 :type :append-entries-response
              :body {:term 1 :success true}}) (:tx-queue (srv 1))))
    (is (= '({:dst 0 :src 2 :type :append-entries-response
              :body {:term 1 :success true}}) (:tx-queue (srv 2))))

    (step 0) ; proccess heart beat response
    (is (= {2 1 1 1} (:match-index (srv 0))))
    (step 0)
    (system-write {:a "a"}) ; broadcast write packets

    (is (= '({:dst 2 :src 0
              :type :append-entries
              :body {:term 1 :leader-id 0
                     :prev-log-index 0
                     :prev-log-term 0
                     :entries [{:term 1 :cmd {:a "a"}}]
                     :leader-commit 1}}
             {:dst 1 :src 0
              :type :append-entries
              :body {:term 1 :leader-id 0
                     :prev-log-index 0
                     :prev-log-term 0
                     :entries [{:term 1 :cmd {:a "a"}}]
                     :leader-commit 1}})
           (:tx-queue (srv 0))))
    (step 0)
    (is (= '(1 1 1) (map :commit-index (servers))))))
