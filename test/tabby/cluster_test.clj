(ns tabby.cluster-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :refer :all]
            [tabby.cluster :refer :all]
            [tabby.utils :as utils]))

(defn fields-by-id [cluster field]
  (map field (vals (sort (:servers cluster)))))

(defn s-at [i]
  (str i ".localnet:" i))

(defn test-cluster [n]
  (let [c (create 8090 n)]
    (update-in c [:servers "0.localnet:0" :election-timeout] (constantly 0))))

(defn create-and-elect []
  (until-empty (test-cluster 3)))

(deftest simple-things
  (testing "everyone's type"
    (let [s (create 80 3)]
      (is (= '(:follower :follower :follower) (fields-by-id s :type)))))
  (testing "first election"
    (let [s (step 0 (test-cluster 3))]
      (is (= :candidate (get-in s [:servers (s-at 0) :type])))
      (is (= :follower (get-in s [:servers (s-at 1) :type])))
      (is (= :follower (get-in s [:servers (s-at 2) :type])))
      (is (> (:election-timeout (get-in s [:servers (s-at 0)])) 0))))

  (testing "1 - 2 vote"
    (let [s (create-and-elect)]
      (is (= '(:leader :follower :follower) (fields-by-id s :type)))

      (is (= '(0 0 0) (fields-by-id s :commit-index))))))

(defn sort-queue [q]
  (sort-by :dst q))

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
      (is (= '({:dst "1.localnet:1" :src "0.localnet:0" :type :request-vote
                :body {:term 1 :candidate-id "0.localnet:0", :prev-log-index 0
                       :prev-log-term 0}}
               {:dst "2.localnet:2" :src "0.localnet:0" :type :request-vote
                :body {:term 1 :candidate-id "0.localnet:0", :prev-log-index 0
                       :prev-log-term 0}}) (sort-queue (:tx-queue (get (:servers s1) (s-at 0))))))))
  (testing "step 2 - peers respond to request vote"
    (let [s2 (->> (test-cluster 3)
                  (step 0)
                  (step 0))]
      ;; others respond to request-vote
      (is (= '(1 1 1) (fields-by-id s2 :current-term)))
      (is (= '("0.localnet:0" "0.localnet:0" "0.localnet:0") (fields-by-id s2 :voted-for)))
      (is (= '({:dst "0.localnet:0" :src "1.localnet:1" :type :request-vote-reply
                :body {:term 1 :vote-granted? true}}) (:tx-queue (get (:servers s2) (s-at 1))) ))
      (is (= '({:dst "0.localnet:0" :src "2.localnet:2" :type :request-vote-reply
                :body {:term 1 :vote-granted? true}}) (:tx-queue (get (:servers s2) (s-at 2)))))))

  (testing "step 3 - become leader, send heart beat"
    (let [s (->> (test-cluster 3)
                 (step 0)
                 (step 0)
                 (step 0))]
      (is (= '(:leader :follower :follower) (fields-by-id s :type)))
      (is (= (list {:dst (s-at 1) :type :append-entries :src (s-at 0)
                    :body {:term 1 :leader-id (s-at 0)
                           :prev-log-index 0 :prev-log-term 0
                           :entries [] :leader-commit 0}}
                   {:dst (s-at 2) :type :append-entries :src (s-at 0)
                    :body {:term 1 :leader-id (s-at 0)
                           :prev-log-index 0 :prev-log-term 0
                           :entries [] :leader-commit 0}})
             (sort-by :dst (:tx-queue (srv s (s-at 0))))))
      (is (= {(s-at 2) 0 (s-at 1) 0} (:match-index (srv s (s-at 0)))))
      (is (= {(s-at 2) 1 (s-at 1) 1} (:next-index (srv s (s-at 0)))))))

  (testing "step 4 - process heart beat responses"
    (let [s (->> (test-cluster 3)
                 (step-times 0 4))]
      (is (= (list {:dst (s-at 0) :src (s-at 1) :type :append-entries-response
                :body {:term 1 :success true :count 0}}) (:tx-queue (srv s (s-at 1)))))
      (is (= (list {:dst (s-at 0) :src (s-at 2) :type :append-entries-response
                :body {:term 1 :success true :count 0}}) (:tx-queue (srv s (s-at 2)))))))
  (testing "step 5 - heart beat response"
    (let [s (->> (test-cluster 3)
                 (step-times 0 5))]
      (is (= {(s-at 2) 0 (s-at 1) 0} (:match-index (srv s (s-at 0)))))))


  (testing "step 7 wait for commit index"
    (let [s (->> (test-cluster 3)
                 (until-empty)
                 (write {:a "a"})
                 (until-empty)
                 (step 150)
                 (until-empty))]
      (is (= '(1 1 1) (fields-by-id s :commit-index))))))

(deftest test-election-responses
  (testing "election with one server not responding"
    (let [s (->> (test-cluster 3)
                 (add-packet-loss (s-at 0) (s-at 1))
                 (step-times 0 3))]
      (is (= '(:leader :follower :follower) (fields-by-id s :type)))))

  (testing "election with two servers not responding, (election should fail)"
    (let [s (->> (test-cluster 3)
                 (add-packet-loss (s-at 1) (s-at 0))
                 (add-packet-loss (s-at 2) (s-at 0))
                 (step-times 0 3))]
      (is (= '(:candidate :follower :follower) (fields-by-id s :type)))))

  (testing "election requests from out of date candidates"
    ;; we should detect that the client term is greater than ours
    ;; convert to follower and increment current-term
    (let [s (-> (test-cluster 3)
                (assoc-in [:servers (s-at 1) :current-term] 2)
                (assoc-in [:servers (s-at 2) :current-term] 2)
                ((partial step-times 0 3)))]
      (is (= '(:follower :follower :follower) (fields-by-id s :type)))
      (is (= 2 (get-in s [:servers (s-at 0) :current-term]))))))

(defn testy []
  (->> (test-cluster 3)
       (until-empty)
       (write {:a "a"})
       (until-empty)
       (step 10)
       (until-empty)))

(defn server-types
  "returns a set of the server types"
  [s]
  ;; (transduce (comp (map (comp :type second)) (filter #(not= (s-at 0) (first %))))
  ;;            conj #{} (:servers s))
  (into #{} (map (comp :type second) (filter #(not= (s-at 0) (first %)) (:servers s)))))

(deftest leadership-change
  (testing "a new leader should be chosen"
    (let [s (->
             (testy)
             (kill-server (s-at 0))
             (#(step 175 %))
             (until-empty)
             (#(step 75 %))
             (until-empty)
             (#(step 75 %)))
          ]
      (is (= #{:leader :candidate} (server-types s))))))

(deftest test-log-catch-up
  (testing "log is missing 1"
    (let [s (->> (test-cluster 3)
                 (until-empty)
                 (add-packet-loss (s-at 0) (s-at 1))
                 (write {:a "a"})
                 (until-empty)
                 (step 75)
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
                 (step 75)
                 (until-empty))]
      (is (= (take 5 (repeat {:a "a"})) (fields-by-id s :db)))))

  (testing "missing two"
    (let [s (->> (test-cluster 5)
                 (until-empty)
                 (add-packet-loss (s-at 0) (s-at 1))
                 (add-packet-loss (s-at 0) (s-at 2))
                 (write {:a "a"})
                 (until-empty)
                 (step 75)
                 (until-empty)
                 (step 75)
                 (until-empty))]
      (is (= '({:a "a"} {} {} {:a "a"} {:a "a"}) (fields-by-id s :db)))))
  (testing "missing 3 - no quorum"
    (let [s (->> (test-cluster 5)
                 (until-empty)
                 (add-packet-loss (s-at 0) (s-at 1))
                 (add-packet-loss (s-at 0) (s-at 2))
                 (add-packet-loss (s-at 0) (s-at 3))
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
                 (add-packet-loss (s-at 0) (s-at 1))
                 (write {:a "a"})
                 (until-empty)
                 (step 75)
                 (until-empty)
                 (step 75)
                 (until-empty))]
      (is (= '(1 0 1) (map count (fields-by-id s :log))))
      (is (= '({:a "a"} {} {:a "a"}) (fields-by-id s :db))))))
