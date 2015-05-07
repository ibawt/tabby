(ns tabby.cluster-test
  (:require [clojure.test :refer :all]
            [tabby.cluster :refer :all]))

(defn fields-by-id [cluster field]
  (map field (vals (sort (:servers cluster)))))

(defn test-cluster [n]
  (-> (create n)
      (update-in-srv 0 :election-timeout (constantly 0))))

(defn create-and-elect []
  (until-empty (test-cluster 3)))

(deftest simple-things
  (testing "everyone's type"
    (let [s (create 3)]
      (is (= '(:follower :follower :follower) (fields-by-id s :type)))))
  (testing "first election"
    (let [s (step 0 (test-cluster 3))]
      (is (= :candidate (get-in s [:servers 0 :type])))
      (is (= :follower (get-in s [:servers 1 :type])))
      (is (= :follower (get-in s [:servers 2 :type])))
      (is (> (:election-timeout (get-in s [:servers 0])) 0))))

  (testing "1 - 2 vote"
    (let [s (create-and-elect)]
      (is (= '(:leader :follower :follower) (fields-by-id s :type)))
      (is (= '(1 1 1) (fields-by-id s :current-term)))
      (is (= '(0 0 0) (fields-by-id s :commit-index))))))

(deftest full-write-in-detail
  (testing "initial state"
    (let [s (test-cluster 3)]
      (is (= '(0 0 0) (fields-by-id s :commit-index)))
      (is (= '(0 0 0) (fields-by-id s :current-term)))
      (is (= '(0 0 0) (fields-by-id s :last-applied)))))

  (testing "step 1 - become candidate & send request-vote"
    (let [s1 (step 0 (test-cluster 3))] ; step 1 become candidate & send request-vote
      (is (= '(:candidate :follower :follower) (fields-by-id s1 :type)))
      (is (= '(1 0 0) (fields-by-id s1 :current-term)))
      (is (= '({:dst 1 :src 0 :type :request-vote
                :body {:term 1 :candidate-id 0, :prev-log-index 0
                       :prev-log-term 0}}
               {:dst 2 :src 0 :type :request-vote
                :body {:term 1 :candidate-id 0, :prev-log-index 0
                       :prev-log-term 0}}) (:tx-queue (get (:servers s1) 0))))))
  (testing "step 2 - peers respond to request vote"
    (let [s2 (->> (test-cluster 3)
                  (step 0)
                  (step 0))]
      ;; others respond to request-vote
      (is (= '(1 1 1) (fields-by-id s2 :current-term)))
      (is (= '(0 0 0) (fields-by-id s2 :voted-for)))
      (is (= '({:dst 0 :src 1 :type :request-vote-reply
                :body {:term 1 :vote-granted? true}}) (:tx-queue (get (:servers s2) 1)) ))
      (is (= '({:dst 0 :src 2 :type :request-vote-reply
                :body {:term 1 :vote-granted? true}}) (:tx-queue (get (:servers s2) 2))))))

  (testing "step 3 - become leader, send heart beat"
    (let [s (->> (test-cluster 3)
                 (step 0)
                 (step 0)
                 (step 0))]
      (is (= '(:leader :follower :follower) (fields-by-id s :type)))
      (is (= '({:dst 1 :type :append-entries :src 0
                :body {:term 1 :leader-id 0
                       :prev-log-index 0 :prev-log-term 0
                       :entries [] :leader-commit 0}}
               {:dst 2 :type :append-entries :src 0
                :body {:term 1 :leader-id 0
                       :prev-log-index 0 :prev-log-term 0
                       :entries [] :leader-commit 0}})
             (sort-by :dst (:tx-queue (srv s 0)))))
      (is (= {2 0 1 0} (:match-index (srv s 0))))
      (is (= {2 1 1 1} (:next-index (srv s 0))))))

  (testing "step 4 - process heart beat responses"
    (let [s (->> (test-cluster 3)
                 (step-times 0 4))]
      (is (= '({:dst 0 :src 1 :type :append-entries-response
                :body {:term 1 :success true :count 0}}) (:tx-queue (srv s 1))))
      (is (= '({:dst 0 :src 2 :type :append-entries-response
                :body {:term 1 :success true :count 0}}) (:tx-queue (srv s 2))))))
  (testing "step 5 - heart beat response"
    (let [s (->> (test-cluster 3)
                 (step-times 0 5))]
      (is (= {2 0 1 0} (:match-index (srv s 0))))))

  (testing "step 6 - system write"
    (let [s (->> (test-cluster 3)
                 (until-empty)
                 (write {:a "a"}))]

      (is (= '({:dst 1 :src 0
                :type :append-entries
                :body {:term 1 :leader-id 0
                       :prev-log-index 0
                       :prev-log-term 0
                       :entries [{:term 1 :cmd {:a "a"}}]
                       :leader-commit 0}}
               {:dst 2 :src 0
                :type :append-entries
                :body {:term 1 :leader-id 0
                       :prev-log-index 0
                       :prev-log-term 0
                       :entries [{:term 1 :cmd {:a "a"}}]
                       :leader-commit 0}})
             (:tx-queue (srv s 0))))))

  (testing "step 7 wait for commit index"
    (let [s (->> (test-cluster 3)
                 (until-empty)
                 (write {:a "a"})
                 (until-empty)
                 (step 10)
                 (until-empty))]
      (is (= '(1 1 1) (fields-by-id s :commit-index))))))

(deftest test-election-responses
  (testing "election with one server not responding"
    (let [s (->> (test-cluster 3)
                 (add-packet-loss 0 1)
                 (step-times 0 3))]
      (is (= '(:leader :follower :follower) (fields-by-id s :type)))))

  (testing "election with two servers not responding, (election should fail)"
    (let [s (->> (test-cluster 3)
                 (add-packet-loss 1 0)
                 (add-packet-loss 2 0)
                 (step-times 0 3))]
      (is (= '(:candidate :follower :follower) (fields-by-id s :type)))))

  (testing "election requests from out of date candidates"
    ;; we should detect that the client term is greater than ours
    ;; convert to follower and increment current-term
    (let [s (-> (test-cluster 3)
                (assoc-in [:servers 1 :current-term] 2)
                (assoc-in [:servers 2 :current-term] 2)
                ((partial step-times 0 3)))]
      (is (= '(:follower :follower :follower) (fields-by-id s :type)))
      (is (= 2 (get-in s [:servers 0 :current-term]))))))

(deftest test-log-catch-up
  (testing "log is missing 1"
    (let [s (->> (test-cluster 3)
                 (until-empty)
                 (add-packet-loss 0 1)
                 (write {:a "a"})
                 (until-empty)
                 (step 10)
                 (until-empty))]
      (is (= '(1 0 1) (map count (fields-by-id s :log))))
      (is (= '(1 0 1) (fields-by-id s :commit-index)))
      (is (= '({:a "a"} {} {:a "a"}) (fields-by-id s :db)))

      (let [s1 (->> s
                    (clear-packet-loss)
                    (step 80)
                    (until-empty)
                    (step 80)
                    (until-empty))]
        (is (= '(1 1 1) (fields-by-id s1 :last-applied)))
        (is (= '(1 1 1) (fields-by-id s1 :commit-index)))
        (is (= '({:a "a"} {:a "a"} {:a "a"}) (fields-by-id s1 :db)))))))

(deftest test-bigger-cluster
  (testing "election"
    (let [s (-> (test-cluster 5) (until-empty))]
      (is (= '(:leader :follower :follower :follower :follower) (fields-by-id s :type)))))

  (testing "write"
    (let [s (->> (test-cluster 5)
                 (until-empty)
                 (write {:a "a"})
                 (until-empty)
                 (step 10)
                 (until-empty))]
      (is (= (take 5 (repeat {:a "a"})) (fields-by-id s :db)))))

  (testing "missing two"
    (let [s (->> (test-cluster 5)
                 (until-empty)
                 (add-packet-loss 0 1)
                 (add-packet-loss 0 2)
                 (write {:a "a"})
                 (until-empty)
                 (step 10)
                 (until-empty)
                 (step 10)
                 (until-empty))]
      (is (= '({:a "a"} {} {} {:a "a"} {:a "a"}) (fields-by-id s :db)))))
  (testing "missing 3 - no quorum"
    (let [s (->> (test-cluster 5)
                 (until-empty)
                 (add-packet-loss 0 1)
                 (add-packet-loss 0 2)
                 (add-packet-loss 0 3)
                 (write {:a "a"})
                 (until-empty)
                 (step 10)
                 (until-empty)
                 (step 10)
                 (until-empty))]
      (is (= '(1 0 0 0 1) (map count (fields-by-id s :log))))
      (is (= '(0 0 0 0 0) (fields-by-id s :last-applied)))
      (is (= '({} {} {} {} {}) (fields-by-id s :db))))))

(deftest test-write-no-response
  (testing "shouldn't write the same log entry over if the same one is sent"
    (let [s (->> (test-cluster 3)
                 (until-empty)
                 (add-packet-loss 0 1)
                 (write {:a "a"})
                 (until-empty)
                 (step 10)
                 (until-empty)
                 (step 10)
                 (until-empty))]
      (is (= '(1 0 1) (map count (fields-by-id s :log))))
      (is (= '({:a "a"} {} {:a "a"}) (fields-by-id s :db))))))
