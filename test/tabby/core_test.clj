(ns tabby.core-test
  (:require [clojure.test :refer :all]
            [tabby.core :refer :all]))

(defn create-and-elect []
  (init)
  (update-in 0 :election-timeout (constantly 0))
  (step 0)
  (step 0)
  (step 0)
  (step 0))

(deftest test-all-the-things
  (testing "everyone's type"
    (init)
    (is (= '(:follower :follower :follower) (map :type (:servers @cluster-states)))))
  (testing "first election"
    (init)
    (update-in-srv 0 :election-timeout (constantly 0))
    (step 0)
    (is (= :candidate (:type (srv 0))))
    (is (= :follower (:type (srv 1))))
    (is (= :follower (:type (srv 2))))
    (is (> (:election-timeout (srv 0)) 0))
    )
  (testing "1 - 2 vote"
    (init)
    (update-in-srv 0 :election-timeout (constantly 0))
    (step 0)
    (step 0)
    (step 0)
    (step 0)
    (is (= '(:leader :follower :follower) (map :type (servers))))
    (is (= '(1 1 1) (map :current-term (servers))))
    (is (= '(0 0 0) (map :commit-index (servers)))))

  (testing "commit-index will updated when consesus is made ( full write )"
    (init)
    (update-in-srv 0 :election-timeout (constantly 0))
    (is (= '(0 0 0) (map :commit-index (servers))))
    (is (= '(0 0 0) (map :current-term (servers))))
    (is (= '(0 0 0) (map :last-applied (servers))))
    (step 0) ; step 1 become candidate & send request-vote
    (is (= '(:candidate :follower :follower) (map :type (servers))))
    (is (= '(1 0 0) (map :current-term (servers))))
    (is (= '({:dst 2 :src 0 :type :request-vote
              :body {:term 1 :candidate-id 0, :prev-log-index 0
                     :prev-log-term 0}}
             {:dst 1 :src 0 :type :request-vote
              :body {:term 1 :candidate-id 0, :prev-log-index 0
                     :prev-log-term 0}}) (:tx-queue (srv 0))))
    (step 0) ; others respond to request-vote
    (is (= '(1 1 1) (map :current-term (servers))))
    (is (= '(0 0 0) (map :voted-for (servers))))
    (is (= '({:dst 0 :src 1 :type :request-vote-reply
              :body {:term 1 :vote-granted? true}}) (:tx-queue (srv 1)) ))
    (is (= '({:dst 0 :src 2 :type :request-vote-reply
              :body {:term 1 :vote-granted? true}}) (:tx-queue (srv 2))))
    (step 0) ; process request-vote become leader, send heart beat
    (is (= '(:leader :follower :follower) (map :type (servers))))
    (is (= '({:dst 1 :type :append-entries :src 0
              :body {:term 1 :leader-id 0
                     :prev-log-index 0 :prev-log-term 0
                     :entries [] :leader-commit 0}}
             {:dst 2 :type :append-entries :src 0
              :body {:term 1 :leader-id 0
                     :prev-log-index 0 :prev-log-term 0
                     :entries [] :leader-commit 0}})
           (sort-by :dst (:tx-queue (srv 0)))))
    (is (= {2 0 1 0} (:match-index (srv 0))))
    (is (= {2 1 1 1} (:next-index (srv 0))))
    (step 0) ; get heart beat commits
    (is (= '({:dst 0 :src 1 :type :append-entries-response
              :body {:term 1 :success true :count 0}}) (:tx-queue (srv 1))))
    (is (= '({:dst 0 :src 2 :type :append-entries-response
              :body {:term 1 :success true :count 0}}) (:tx-queue (srv 2))))
    (step 0) ; proccess heart beat response
    (is (= {2 0 1 0} (:match-index (srv 0))))

    (step 0)
    (system-write {:a "a"}) ; broadcast write packets

    (is (= '({:dst 2 :src 0
              :type :append-entries
              :body {:term 1 :leader-id 0
                     :prev-log-index 0
                     :prev-log-term 0
                     :entries [{:term 1 :cmd {:a "a"}}]
                     :leader-commit 0}}
             {:dst 1 :src 0
              :type :append-entries
              :body {:term 1 :leader-id 0
                     :prev-log-index 0
                     :prev-log-term 0
                     :entries [{:term 1 :cmd {:a "a"}}]
                     :leader-commit 0}})
           (:tx-queue (srv 0))))

    (step 0)
    (step 150)
    (step 0)
    (is (= '(1 1 1) (map :commit-index (servers))))))

(deftest test-election-responses
  (testing "election with one server not responding"
    (init)
    (add-packet-loss 1 0)
    (step 0)
    (step 0)
    (step 0)
    (is (= '(:leader :follower :follower) (map :type (servers)))))
  (testing "election with two servers not responding, (election should fail)"
    (init)
    (add-packet-loss 1 0)
    (add-packet-loss 2 0)
    (step 0)
    (step 0)
    (step 0)
    (is (= '(:candidate :follower :follower) (map :type (servers)))))
  (testing "election requests from out of date caniditate"
    ;; we should detect that the client term is greater than ours
    ;; convert to follower and increment current-term
    (init)
    (update-in-srv 1 :current-term (constantly 2))
    (update-in-srv 2 :current-term (constantly 2))
    (step 0)
    (step 0)
    (step 0)
    (is (= '(:follower :follower :follower) (map :type (servers))))
    (is (= 2 (:current-term (srv 0))))))

(deftest test-log-catch-up
  (testing "log catchup"
    (init)
    (until-empty)
    (add-packet-loss 1 0)
    (system-write {:a "a"})
    (until-empty)))
