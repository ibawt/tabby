(ns tabby.client
  (:require [tabby.utils :as utils]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [tabby.net :as net]
            [tabby.utils :as utils]
            [clojure.tools.logging :refer [info warn]]))

(defn- close-socket
  "closes the socket if it's open and clears it"
  [client]
  (update client :socket
          (fn [s]
            (when s
              (s/close! s)
              nil))))

(defn set-leader
  "sets the current leader to 'host':'port'"
  [this host port]
  (-> (close-socket this)
      (assoc :leader {:host (or (:host-override this) host) :port port})))

(defn- set-random-leader
  "picks a random leader"
  [client]
  (assoc client :leader (first (shuffle (:servers client)))))

(defn- connect-to-leader
  "connects to the leader, and sends handshake.
   returns a deferred"
  [client]
  (d/catch
      (d/chain (net/client (or (:host-override client) (get-in client [:leader :host]))
                           (get-in client [:leader :port]))
               (fn [x]
                 (s/put! x {:type :client-handshake})
                 (assoc client :socket x)))

      (fn [e]
        (warn "exception in connect, setting random leader")
        (set-random-leader client))))

(defn- connected?
  "is the connection open?"
  [{socket :socket}]
  (and socket ((complement s/closed?) socket)))

(defn- send-pkt-sync
  "sends the packet, will return a deferred"
  [client pkt]
  (warn "sending pkt?")
  (let [start-time (System/currentTimeMillis)]
    (loop [c client, times 0]
      (if (> (- (System/currentTimeMillis) start-time) (:timeout client))
        (do (warn "client timeout")
          [(close-socket c) :timeout])
        (do
          (when (pos? times)
            (Thread/sleep (* 10 (* times times))))
          (cond
            (> times (or (:max-tries c) 25))
            (do
              (warn "client exceeded max tries!")
              [(close-socket c) :timeout])
            (not (connected? c)) (do
                                   (warn "client not connected, connecting to leader")
                                   (recur @(connect-to-leader c) (inc times)))
            :else
            (let [_ @(s/try-put! (:socket c) pkt (:timeout client))
                  msg @(s/try-take! (:socket c) ::none (:timeout client) ::timeout)]
              (cond
                (= ::none msg) (do (warn "not connected")
                                   (recur (set-random-leader (close-socket c)) (inc times)))
                (= ::timeout msg) (do (warn "client timed out in reading response")
                                      [c :timeout])
                (not= :redirect (:type msg)) [c (:body msg)]
                :else (recur (if (:hostname msg)
                               (set-leader (close-socket c) (:hostname msg) (:port msg))
                               (set-random-leader (close-socket c)))
                             (inc times))))))))))

(defn- send-pkt
  "sends the packet, will return a deferred"
  [client pkt]
  (warn "sending pkt?")
  (let [start-time (System/currentTimeMillis)]
    (d/loop [c client, times 0]
      (if (> (- (System/currentTimeMillis) start-time) (:timeout client))
        (do (warn "client timeout")
          [(close-socket @c) :timeout])
        (d/let-flow [c c]
          @(manifold.time/in
           (* 10 (* times times)) ;; back off
           (fn []
             (cond
               (> times (or (:max-tries c) 25))
               (do
                 (warn "client exceeded max tries!")
                 [(close-socket c) :timeout])
               (not (connected? c)) (do
                                      (warn "client not connected, connecting to leader")
                                      (d/recur (connect-to-leader c) (inc times)))
               :else
               (d/let-flow [_ (s/try-put! (:socket c) pkt (:timeout client))
                            msg (s/try-take! (:socket c) ::none (:timeout client) ::timeout)]
                 (info "got msg:" msg)
                 (cond
                   (= ::timeout msg) (do (warn "client timed out in reading response")
                                         [c :timeout])
                   (not= :redirect (:type msg)) [c (:body msg)]
                   :else (d/recur (if (:hostname msg)
                                    (set-leader (close-socket c) (:hostname msg) (:port msg))
                                    (set-random-leader (close-socket c)))
                                  (inc times))))))))))))

(defn success? [value]
  (= :ok (:value value)))

(defprotocol Client
  "A set of functions each client must implement."
  (close [this]
    "Close the socket, returns this")
  (get-value [this key]
    "gets the specified value, returns a [this {:value v}] response
     [this :timeout] if the value exceeds the timeout")
  (compare-and-swap [this key new old]
    "sets the key to 'new' if the value is 'old', returns
     [this {:value :ok}] if successful, and [this {:value :fail}] if not")
  (set-or-create [this key value]
    "sets the key to value, returns [this {:value :ok}] if successfull"))

(defrecord NetworkClient
    [servers socket leader host-override timeout max-tries]
  Client
  (close [this]
    (close-socket this))
  (get-value [this key]
    (send-pkt-sync this {:type :get :key key :uuid (utils/gen-uuid)}))
  (compare-and-swap [this key new old]
    (send-pkt-sync this {:type :cas :body {:key key :new new :old old}
                    :uuid (utils/gen-uuid)}))
  (set-or-create [this key value]
    (send-pkt-sync this {:type :set :value value :key key
                      :uuid (utils/gen-uuid)})))

(defn make-network-client [servers]
  (set-random-leader (map->NetworkClient {:servers servers :timeout 15000})))
