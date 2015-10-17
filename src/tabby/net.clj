(ns tabby.net
  (:require [aleph.tcp :as tcp]
            [clojure.core.async :as a]
            [clojure.edn :as edn]
            [clojure.tools.logging :refer [warn info]]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [tabby.client-state :as cs]
            [tabby.cluster :as cluster]
            [tabby.server :as server]
            [tabby.utils :as u]
            [tabby.utils :as utils]))

(def ^:private protocol
  "The tabby protocol definition, serialized strings through EDN"
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
     (s/map #(io/encode protocol %) out)
     s)

    (s/splice
     out
     (io/decode-stream s protocol))))

(defn client
  "Tcp connection to the host:port combo provided.
   Returns a deferred."
  [host port]
  (assert host "host is nil")
  (assert port "port is nil")
  (d/chain (tcp/client {:host host, :port port})
           #(wrap-duplex-stream protocol %)))

(defn- connect-peer-socket
  "Connects the peer messages to the :rx-chan"
  [state socket handshake]
  (info (:id @state) " accepting peer connection from: " (:src handshake))
  (when-not (get-in @state [:peers (:src handshake) :socket])
    (swap! state assoc-in [:peers (:src handshake) :socket] socket))
  (s/connect socket (:rx-chan @state)))

(defn- connect-table-tennis-socket
  ":ping -> :pong"
  [state socket handshake]
  (d/loop []
    (-> (s/take! socket ::none)
        (d/chain
         (fn [msg]
           (when-not (= ::none msg)
             (when (= :ping msg)
               (s/put! socket :pong))
             (d/recur)))))))

(defn- connect-client-socket
  "Dispatches socket messages to the states :rx-queue"
  [state socket handshake]
  (let [client-index (utils/gen-uuid)]
    (warn (:id @state) "client connected: " client-index)
    (swap! state update :clients cs/create-client client-index socket)
    (d/loop []
      (-> (s/take! socket ::none)
          (d/chain
           (fn [msg]
             (when-not (= ::none msg)
               (a/go
                 (a/>! (:rx-chan @state)
                       (assoc msg :client-id client-index)))
               (d/recur))))))))

(defn- connection-handler
  "Returns a function that will handle the handshake for incoming connections."
  [state]
  (fn [s info]
    (d/let-flow
     [handshake (s/take! s)]
     (-> ((condp = (:type handshake)
            :peering-handshake connect-peer-socket 
            :table-tennis connect-table-tennis-socket
            :client-handshake connect-client-socket
            (fn [&_]
              (warn "invalid handshake")
              (s/close! s)))
          state s handshake)
         (d/catch (fn [ex]
                    (warn ex "caught exception! closing socket")
                    (s/close! s)))))))

(definline ^:private current-time
  "current time in ms"
  []
  (System/currentTimeMillis))

(defn- handle-timeout
  "timeout of dt seconds, just run the update"
  [state dt]
  (swap! state (fn [s] (server/update-state dt s)))
  true)

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

(defn connect-to-peer3
  "deferred connect version, returns just the socket in a defered"
  [state [id peer]]
  (-> (d/let-flow [socket (client (:hostname peer) (:port peer))
                   _ (s/put! socket {:src (:id state) :type :peering-handshake})]
                  [id (assoc peer :socket socket)])
      (d/catch (fn [ex]
                 (warn ex "caught exception in connect")))))

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
      (assoc s :tx-queue [])
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
      (when-not (empty? (:tx-queue @state))
        (transmit @state)
        (swap! state assoc :tx-queue []))
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
                    :rx-chan (a/chan 3)})
    s))
