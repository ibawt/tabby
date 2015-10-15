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
  (assert host "host is nil")
  (assert port "port is nil")
  (d/chain (tcp/client {:host host, :port port})
           (partial wrap-duplex-stream protocol)))

(defn- connection-handler
  "incoming connection handler"
  [state]
  (fn [s info]
    ;;; -------------------------------------------
    ;;; state is an atom. this is multi-threaded
    ;;; -------------------------------------------
    (d/let-flow
     [handshake (s/take! s)]
     (condp = (:type handshake)
       :peering-handshake (do
                            (info (:id @state) " accepting peer connection from: " (:src handshake))
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
                           (info (:id @state) "client connected: " client-index)
                           (swap! state update :clients cs/create-client client-index s)
                           (d/loop []
                             (-> (s/take! s ::none)
                                 (d/chain
                                  (fn [msg]
                                    (when-not (= ::none msg)
                                      (a/go
                                         (a/>! (:rx-chan @state)
                                              (assoc msg :client-id client-index)))
                                      (d/recur)))))))))))

(definline ^:private current-time
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
  (try
    (let [socket @(client (:hostname peer) (:port peer))]
      (d/let-flow [_ (s/put! socket {:src (:id @state) :type :peering-handshake})]
                  (swap! state assoc-in [:peers id :socket] socket)))
    (catch Exception e
      (warn e "caught exception in connect to peer")
      state)))


(defn connect-to-peer2
  "when the peer socket isn't present connect to it
   (will block)"
  [state [id peer]]
  (try
    (let [socket @(client (:hostname peer) (:port peer))]
      (d/let-flow [_ (s/put! socket {:src (:id state) :type :peering-handshake})]
                  (assoc-in state [:peers id :socket] socket)))
    (catch Exception e
      (warn e "caught exception in connect to peer")
      state)))

(defn- send-pkt
  "TODO: this should be more lazy and less swappy"
  [state pkt]
  (when-let [peer-id (:dst pkt)]
    (let [socket (get-in @state [:peers peer-id :socket])]
      (when (or (nil? socket) (s/closed? socket))
        @(connect-to-peer state [peer-id (get-in @state [:peers peer-id])]))
      (s/put! (get-in @state [:peers peer-id :socket]) pkt)))
  (when-let [client (:client-dst pkt)]
    (let [socket (get-in @state [:clients client :socket])]
      (if (s/closed? socket)
        (swap! state assoc-in [:clients client :socket] nil))
      (s/put! (get-in @state [:clients client :socket]) (dissoc pkt :client-dst))))
  nil)

(defn- send-peer-packet [state p]
  (let [peer-id (:dst p)]
   (loop [s state times 0]
     (let [socket (get-in s [:peers peer-id :socket])]
       (if (and socket (not (s/closed? socket)))
         (do
           (s/put! socket p) s)
         (recur @(connect-to-peer2 s [peer-id (get-in s [:peers peer-id])])
                (inc times)))))))

(defn- send-client-packet [state p]
  (let [client (:client-dst p)
        socket (get-in state [:clients client :socket])]
    (if (and socket (s/closed? socket))
      (assoc-in state [:clients client :socket] nil)
      (do
        (s/put! socket (dissoc p :client-dst))
        state))))

(defn- send-pkt [state p]
  (cond
    (:dst p) (send-peer-packet state p)
    (:client-dst p) (send-client-packet state p)
    :else (assert false "invalid packet")))

(defn- transmit [state]
  (loop [pkts (:tx-queue state)
         s state]
    (if (empty? pkts)
      (assoc s :tx-queue '())
      (recur (rest pkts) (send-pkt s (first pkts))))))

(defn- handle-rx-pkt [state dt pkt]
  (swap! state
         (fn [s]
           (server/update-state dt (update s :rx-queue conj pkt))))
  true)

(defn event-loop [state {timeout :timeout}]
  (let [stop (a/chan)]
    ;;; ----------------------------------------------------------------
    ;;; state is an atom
    ;;; ----------------------------------------------------------------
    (a/go-loop [t (current-time)]
      (when (seq? (:tx-queue @state))
        (swap! state transmit))
      (if (a/alt!
            (:rx-chan @state) ([v] (if v (handle-rx-pkt state (- (current-time) t) v) false))
            stop false
            (a/timeout 10) (handle-timeout state (- (current-time) t)))
        (recur (current-time))
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
