(ns tabby.leader-test
  (:require [tabby.leader :refer :all]
            [clojure.test :refer :all]))

(deftest highest-match-index-test
  (testing "redundant case"
    (is (= [1 1] (highest-match-index {:match-index
                                       {:a 1}}))))
  (testing "if freq are equivalent take the highest index"
    (is (= [5 1] (highest-match-index {:match-index
                                       {:a 4 :b 5}}))))
  (testing "normal case"
    (is (= [5 2] (highest-match-index {:match-index
                                       {:a 2 :b 5 :c 5 :d 1}})))))
