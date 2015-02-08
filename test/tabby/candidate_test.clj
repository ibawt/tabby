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

(defn- make-state [n]
  (-> state
      (become-candidate)
      (assoc :peers (range 1 n))
      (assoc :tx-queue '())))

(defn- send-rvp-resp [state from-id granted?]
  (handle-request-vote-response state
                                {:src from-id :dst 0 :type :request-vote-reply
                                 :body {:vote-granted? granted? :term 1}}))

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
    (testing "request-vote rpcs will be queued"
      (is (= '({:dst 2 :src 0 :type :request-vote
                :body {:term 1 :candidate-id 0, :prev-log-index 0
                       :prev-log-term 0}}
               {:dst 1 :src 0 :type :request-vote
                :body {:term 1 :candidate-id 0, :prev-log-index 0
                       :prev-log-term 0}}) (:tx-queue s))))))

(deftest test-handle-request-vote-response
  (testing "should ignore if not a candidate"
    (is (= state (send-rvp-resp state 1 true))))
  (testing "will update votes structure based on the response"
    (is (= {0 true 1 true} (:votes (-> (make-state 3)
                                       (send-rvp-resp 1 true)))))
    (is (= {0 true 2 true} (:votes (-> (make-state 3)
                                       (send-rvp-resp 2 true)))))
    (is (= {0 true 2 false} (:votes (-> (make-state 3)
                                        (send-rvp-resp 2 false))))))
  (testing "n/2 +1 required"
    (let [s (-> (make-state 5)
                (send-rvp-resp 1 true)
                (send-rvp-resp 2 true))]
      (is (= :leader (:type s)))))
  (testing "multiples from 1"
    (let [s (-> (make-state 5)
                (send-rvp-resp 1 true)
                (send-rvp-resp 1 true))]
      (is (= {1 true 0 true} (:votes s)))
      (is (= :candidate (:type s)))))
  (testing "should always vote for myself"
    (is (= 0 (:voted-for (make-state 3))))
    (is (= {0 true} (:votes (make-state 3))))))
