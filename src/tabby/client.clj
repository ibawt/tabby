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
      (assoc :leader {:host (or (:host-override this) host) :port port})))

(defn- set-next-leader [client]
  (let [s (:servers client)
        n (concat (rest s) (list (first s)))]
    (-> (assoc client :leader (first n))
        (assoc :servers n))))

(defn- connect-to-leader
  [client]
  (d/catch
      (d/let-flow [socket (net/client (or (:host-override client) (get-in client [:leader :host]))
                                      (get-in client [:leader :port]))]
        (s/put! socket {:type :client-handshake})
        (assoc client :socket socket))
      (fn [e]
        (warn e "caught exception in connect")
        (set-next-leader client))))

(defn- connected? [{socket :socket}]
  (and socket ((complement s/closed?) socket)))

(defn- send-pkt
  [client pkt]
  (d/loop [c client, times 0]
    (d/let-flow [c c]
      (manifold.time/in
       (* 100 times)
       (fn []
         (cond
           (> times 10) [c :dead]
           (not (connected? c)) (d/recur (connect-to-leader c) (inc times))
           :else (d/let-flow [_ (s/put! (:socket c) pkt)
                              msg (s/take! (:socket c) ::none)]
                   (if-not (= :redirect (:type msg))
                     [c (:body msg)]
                     (d/recur (set-leader c (:hostname msg) (:port msg))
                              (inc times))))))))))

(defprotocol Client
  (close [this])
  (get-value [this key])
  (compare-and-swap [this key new old])
  (set-or-create [this key value]))

(defrecord LocalClient
    [servers socket leader host-override]
  Client
  (close [this]
    (when (and socket (not (s/closed? socket)))
      (s/close! socket))
    (assoc this :socket nil))

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
