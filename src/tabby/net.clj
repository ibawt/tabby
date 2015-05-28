(ns tabby.net
  (:require [tabby.utils :as utils]
            [clojure.edn :as edn]
            [gloss.core :as gloss]
            [tabby.client-state :as cs]
            [gloss.io :as io]
            [tabby.utils :as u]
            [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.core.async :as a]
            [clojure.tools.logging :refer :all]
            [tabby.server :as server]
            [tabby.cluster :as cluster]))

(def ^:private protocol
  (gloss/compile-frame
   (gloss/finite-frame :uint32
                       (gloss/string :utf-8))
   pr-str
   edn/read-string))

(defn- wrap-duplex-stream
  "wrap the stream in the protocol"
  [protocol s]
  (let [out (s/stream)]
    (s/connect
     (s/map (partial io/encode protocol) out)
     s)

    (s/splice
     out
     (io/decode-stream s protocol))))

(defn client
  [host port]
  (d/chain (tcp/client {:host host, :port port})
           (partial wrap-duplex-stream protocol)))

(defn- connection-handler
  "incoming connection handler"
  [state]
  (fn [s info]
    (d/let-flow
     [handshake (s/take! s)]
     (condp = (:type handshake)
       :peering-handshake (do
                            (when-not (get-in @state [:peers (:src handshake) :socket])
                              (swap! state assoc-in [:peers (:src handshake) :socket] s))
                            (s/connect s (:rx-chan @state)))

       :table-tennis (d/loop []
                       (-> (s/take! s ::none)
                           (d/chain
                            (fn [msg]
                              (when-not (= ::none msg)
                                (when (= :ping msg)
                                  (s/put! s :pong))
                                (d/recur))))))

       :client-handshake (let [client-index (utils/gen-uuid)]
                           (info (:id @state) "client connected")
                           (swap! state update-in [:clients] cs/create-client client-index s)
                           (d/loop []
                             (-> (s/take! s ::none)
                                 (d/chain
                                  (fn [msg]
                                    (when-not (= ::none msg)
                                      (a/go
                                         (a/>! (:rx-chan @state)
                                              (assoc msg :client-id client-index)))
                                      (d/recur)))))))))))

(definline ^:private now
  "current time in ms"
  []
  (System/currentTimeMillis))

(defn- handle-timeout
  "timeout of dt seconds, just run the update"
  [state dt]
  (swap! state (fn [s] (server/update-state dt s)))
  true)

(defn connect-to-peer
  "when the peer socket isn't present connect to it
   (will block)"
  [state [id peer]]
  (let [socket @(client (:hostname peer) (:port peer))]
    (d/let-flow [_ (s/put! socket {:src (:id @state) :type :peering-handshake})]
               (swap! state assoc-in [:peers id :socket] socket))))

(defn- send-pkt
  "TODO: this should be more lazy and less swappy"
  [state pkt]
  (when-let [peer-id (:dst pkt)]
    (when-not (get-in @state [:peers peer-id :socket])
      @(connect-to-peer state peer-id))
    (s/put! (get-in @state [:peers peer-id :socket]) pkt))
  (when-let [client (:client-dst pkt)]
    (s/put! (get-in @state [:clients client :socket]) pkt))
  true)

(defn- transmit [state]
  (swap! state (fn [s]
                 (doseq [pkt (:tx-queue @state)]
                   (send-pkt state pkt))
                 (assoc s :tx-queue '()))))

(defn- handle-rx-pkt [state dt pkt]
  (swap! state
         (fn [s]
           (server/update-state dt (update-in s [:rx-queue] conj pkt))))
  true)

(defn event-loop [state {timeout :timeout}]
  (let [stop (a/chan)]
    (a/go-loop [t (now)]
      (transmit state)
      (if (a/alt!
            (:rx-chan @state) ([v] (if v (handle-rx-pkt state (- (now) t) v) false))
            stop false
            (a/timeout 10) (handle-timeout state (- (now) t)))
        (recur (now))
        :stopped))
    stop))

(defn start-server
  [server port]
  (info "starting server on port: " port)
  (tcp/start-server
   (fn [s info]
     ((connection-handler server) (wrap-duplex-stream protocol s) info))
   {:port port}))

(defn create-server [server port]
  (info "creating server: " (:id server) " on port: " port)
  (let [s (atom server)
        socket (start-server s port)]
    (swap! s merge {:server-socket socket
                    :rx-chan (a/chan)})
    s))
