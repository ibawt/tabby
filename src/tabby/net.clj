(ns tabby.net
  (:require [tabby.utils :as utils]
            [clojure.edn :as edn]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.core.async :as a]
            [clojure.tools.logging :refer :all]
            [clojure.tools.namespace.repl :refer [refresh]]
            [tabby.server :as server]
            [tabby.cluster :as cluster]))

(declare client)

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

(defn- connection-handler
  "incoming connection handler"
  [state]
  (fn [s info]
    (d/let-flow
     [handshake (s/take! s)]
     (condp = (:type handshake)
       :peering-handshake (do
                            (when-not (get-in @state [:peer-sockets (:src handshake)])
                              (swap! state assoc-in [:peer-sockets (:src handshake)] s))
                            (s/connect s (:rx-chan @state)))

       :table-tennis (d/loop []
                       (-> (s/take! s ::none)
                           (d/chain
                            (fn [msg]
                              (when (= :ping msg)
                                (s/put! s :pong))
                              (d/recur)))))

       :client-handshake (let [client-index
                               (count (:clients (swap! state update-in [:clients] conj {:socket s})))]
                           (d/loop []
                             (-> (s/take! s ::none)
                                 (d/chain
                                  (fn [msg]
                                    (a/go
                                      (a/>! (:rx-chan @state) (merge msg {:client-id client-index})))
                                    (d/recur))))))))))

(defn- now
  "current time in ms"
  []
  (System/currentTimeMillis))

(defn- handle-timeout
  "timeout of dt seconds, just run the update"
  [state dt]
  (swap! state (fn [s] (server/update dt s)))
  true)

(defn connect-to-peer
  "when the peer socket isn't present connect to it
   (will block)"
  [state peer]
  (let [socket @(client "localhost" (+ 8090 peer))]
    (d/let-flow [_ (s/put! socket {:src (:id @state) :type :peering-handshake})]
                (swap! state assoc-in [:peer-sockets peer] socket))))

(defn- send-pkt [state pkt]
  (when-let [peer (:dst pkt)]
    (when-not (get-in @state [:peer-sockets peer])
      @(connect-to-peer state peer))
    (s/put! (get-in @state [:peer-sockets peer]) pkt))
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
           (server/update dt (utils/update s :rx-queue conj pkt))))
  true)


(defn event-loop-no-timeout [state]
  (let [stop (a/chan)]
    (a/go-loop []
      (transmit state)
      (if (a/alt!
            stop false
            (:rx-chan @state) ([v] (if v (handle-rx-pkt state 0) false)))
        (recur)
        :stopped))
    stop))

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

(defn client
  [host port]
  (d/chain (tcp/client {:host host, :port port})
           (partial wrap-duplex-stream protocol)))

(defn start-server
  [server port]
  (tcp/start-server
   (fn [s info]
     ((connection-handler server) (wrap-duplex-stream protocol s) info))
   {:port port}))

(defn create-server [server port]
  (let [s (atom server)
        socket (start-server s port)]
    (swap! s merge {:server-socket socket
                    :rx-chan (a/chan)})
    s))
