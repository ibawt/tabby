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

(defn- set-next-leader
  [client]
  (let [s (:servers client)
        n (concat (rest s) (list (first s)))]
    (info "client picking new leader" (first n))
    (-> (assoc client :leader (first n))
        (assoc :servers n))))

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
        (set-next-leader client))))

(defn- connected?
  "is the connection open?"
  [{socket :socket}]
  (and socket ((complement s/closed?) socket)))

(defn- send-pkt-sync
  [client pkt]
  (let [start-time (System/currentTimeMillis)]
    (loop [c client, times 0]
      (if (> (- (System/currentTimeMillis) start-time) (:timeout client))
        [(close-socket c) :timeout]
        (do
          (when (pos? times)
            (Thread/sleep (* 10 (* times times))))
          (cond
            (> times (or (:max-tries c) 25)) [(close-socket c) :timeout]
            (not (connected? c)) (recur @(connect-to-leader c) (inc times))
            :else
            (let [_ @(s/try-put! (:socket c) pkt (:timeout client))
                  msg @(s/try-take! (:socket c) ::none (:timeout client) ::timeout)]
              (cond
                (= ::none msg) (recur (set-next-leader (close-socket c)) (inc times))
                (= ::timeout msg) [c :timeout]
                (not= :redirect (:type msg)) [c (:body msg)]
                :else (recur (if (:hostname msg)
                               (set-leader (close-socket c) (:hostname msg) (:port msg))
                               (set-next-leader (close-socket c)))
                             (inc times))))))))))

(defn success? [value]
  (= :ok (:value value)))

(defn make-network-client [servers]
  (atom (set-next-leader {:servers servers :timeout 15000})))


(defn get-value! [this key]
  (let [[c v] (send-pkt-sync @this {:type :get :key key :uuid (utils/gen-uuid)})]
    (reset! this c)
    v))

(defn set-value! [this key value]
  (let [[c v] (send-pkt-sync @this {:type :set :key key :value value
                                    :uuid (utils/gen-uuid)})]
    (reset! this c)
    v))

(defn compare-and-swap! [this key new old]
  (let [[c v] (send-pkt-sync @this {:type :cas :body {:key key :new new :old old}
                                   :uuid (utils/gen-uuid)})]
    (reset! this c)
    v))

(defn close! [this]
  (swap! this close-socket))
