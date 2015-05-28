(ns tabby.client
  (:require [tabby.utils :as utils]
            [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.core.async :as a]
            [tabby.net :as net]
            [tabby.utils :as utils]
            [clojure.tools.logging :refer :all]))

(defn- close-socket [client]
  (if-let [s (:socket client)]
    (do (s/close! s)
        (assoc client :socket nil))
    client))

(defn set-leader [this id]
  (warn "setting leader to: " id)
  (-> (close-socket this)
      (assoc :leader (get-in this [:servers id]))))

(defn- connect-to-leader
  "blocks"
  [client]
  (warn "connect to leader")
  (let [socket @(net/client (get-in client [:leader :host])
                            (get-in client [:leader :port]))]
    (s/put! socket {:type :client-handshake})
    (assoc client :socket socket)))

(defn- get-value-socket
  [client key]
  (d/loop [c client
           times 0]
    (if (and (< times 5) (not (:socket c)))
      (d/recur (connect-to-leader c) (inc times))
      (do
        (s/put! (:socket c) {:type :get :key key :uuid (utils/gen-uuid)})
        (-> (s/take! (:socket c) ::none)
            (d/chain
             (fn [msg]
               (when-not (= ::none msg)
                 (if (= :redirect (:type msg))
                   (d/recur (set-leader c (:leader-id msg)) (inc times))
                   [c (:value msg)])))))))))

(defn- send-pkt [client pkt]
  (d/loop [c client]
    (if-not (:socket c)
      (d/recur (connect-to-leader c))
      (do
        (s/put! (:socket c) pkt)
        (-> (s/take! (:socket c) ::none)
            (d/chain
             (fn [msg]
               (warn "msg: " msg)
               (when-not (= ::none msg)
                 (if (= :redirect (:type msg))
                   (d/recur (set-leader c (:leader-id msg)))
                   [c (:value msg)])))))))))

(defn- set-or-create-socket
  [client key value]
  (send-pkt client {:type :set :value value :key key
                    :uuid (utils/gen-uuid)}))

(defprotocol Client
  (close [this])
  (get-value [this key])
  (set-or-create [this key value]))

(defrecord LocalClient
    [servers socket leader]
  Client
  (close [this])
  (get-value [this key]
    (get-value-socket this key))
  (set-or-create [this key value]
    (set-or-create-socket this key value)))

(defn make-local-client [servers]
  (map->LocalClient {:servers servers :leader (first (shuffle servers))}))
