(ns tabby.client
  (:require [tabby.utils :as utils]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [tabby.net :as net]
            [tabby.utils :as utils]
            [clojure.tools.logging :refer [info warn]]))

(defn- close-socket [client]
  (if (:socket client)
    (do (s/close! (:socket client))
        (assoc client :socket nil))
    client))

(defn set-leader [this host port]
  (-> (close-socket this)
      (assoc :leader {:host host :port port})))

(defn- connect-to-leader
  "blocks"
  [client]
  (try
    (let [socket @(net/client (get-in client [:leader :host])
                              (get-in client [:leader :port]))]
      (s/put! socket {:type :client-handshake})
      (assoc client :socket socket))
    (catch Exception e
      (warn e "caught exception in connect")
      nil)))

(defn- send-pkt
  "this will block"
  [client pkt]
  (loop [c client]
    (if-not (:socket c)
      (recur (connect-to-leader c))
      (do
        @(s/put! (:socket c) pkt)
        (let [msg @(s/take! (:socket c) ::none)]
          (if-not (= :redirect (:type msg))
            [c (:body msg)]
            (recur (set-leader c (:hostname msg) (:port msg)))))))))

(defprotocol Client
  (close [this])
  (get-value [this key])
  (compare-and-swap [this key new old])
  (set-or-create [this key value]))

(defrecord LocalClient
    [servers socket leader]
  Client
  (close [this]
    (s/close! socket))

  (get-value [this key]
    (send-pkt this {:type :get :key key :uuid (utils/gen-uuid)}))
  (compare-and-swap [this key new old]
    (send-pkt this {:type :cas :body {:key key :new new :old old}
                    :uuid (utils/gen-uuid)}))
  (set-or-create [this key value]
    (send-pkt this {:type :set :value value :key key
                      :uuid (utils/gen-uuid)})))

(defn make-local-client [servers]
  (map->LocalClient {:servers servers :leader (first servers)}))
