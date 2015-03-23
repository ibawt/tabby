(ns tabby.client
  (:require [tabby.utils :as utils]
            [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.core.async :as a]
            [tabby.net :as net]
            [clojure.tools.logging :refer :all]))

(defn connect [host port]
  (let [client @(net/client host port)]
    (s/put! client {:type :client-handshake})
    client))

(defn get-value
  [client key]
  (s/put! client {:type :get :value key})
  (s/take! client))

(defn merge
  [client m]
  (s/put! client {:type :merge :value m})
  (s/take! client))
