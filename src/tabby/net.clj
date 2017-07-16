(ns tabby.net
  (:require [aleph.tcp :as tcp]
            [clojure.tools.logging :refer [warn info]]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [taoensso.nippy :as nippy]
            [manifold.deferred :as d]
            [tabby.leader :as l]
            [tabby.follower :as f]
            [manifold.stream :as s]
            [tabby.client-state :as cs]
            [tabby.server :as server]
            [tabby.utils :as utils]
            [tabby.follower :as f]))

(def ^:private protocol
  "The tabby protocol definition."
  (gloss/compile-frame
   (gloss/finite-block :uint16)
   (comp byte-streams/to-byte-buffer nippy/freeze)
   (comp nippy/thaw byte-streams/to-byte-array)))

(defn- stream-open? [x]
  (and x (not (s/closed? x))))

(def ^:private stream-closed? (complement stream-open?))

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

(defn- current-time
  "current time in ms"
  []
  (long (/ (System/nanoTime) 1000000)))

(defn- delta-t
  "Returns the time difference between t and now.  Will be >= 0"
  [t]
  (max (- (current-time) t) 0))

(defn client
  "Tcp connection to the host:port combo provided.
   Returns a deferred."
  [host port]
  (assert host "host is nil")
  (assert port "port is nil")
  (d/chain (tcp/client {:host host, :port port})
           #(wrap-duplex-stream protocol %)))

(declare transmit!)

(defn- connect-peer-socket!
  "Connects the peer messages to the :rx-stream"
  [state socket handshake]
  (info (:id @state) " accepting peer connection from: " (:src handshake))
  (swap! state assoc-in [:peers (:src handshake) :socket] socket)
  (when (= :leader (:type @state))
    (swap! state l/broadcast-heartbeat)
    (transmit! state))

  (s/connect socket (:rx-stream @state)
             {:downstream? false :upstream? true}))

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

(defn- connect-client-socket!
  "Dispatches socket messages to the states :rx-stream"
  [state socket handshake]
  (let [client-index (utils/gen-uuid)]
    (info (:id @state) "client connected: " client-index)
    (swap! state update :clients cs/create-client client-index socket)
    (s/connect (s/map #(assoc % :client-id client-index) socket)
               (:rx-stream @state)
               {:downstream? false})))

(defn- connection-handler
  "Returns a function that will handle the handshake for incoming connections."
  [state]
  (fn [s info]
    (d/let-flow [handshake (s/take! s 100)]
     (-> ((condp = (:type handshake)
            :peering-handshake connect-peer-socket!
            :table-tennis connect-table-tennis-socket
            :client-handshake connect-client-socket!
            (fn [&_]
              (warn "Invalid handshake: " (:type handshake))
              (s/close! s)))
          state s handshake)
         (d/catch (fn [ex]
                    (warn ex "Caught exception in connection-handler")
                    (s/close! s)))))))

(defn connect-to-peer
  "deferred connect version, returns just the socket in a defered"
  [state [id peer]]
  (info (:id state) " connecting to peer: " id)
  (if (stream-open? (:socket peer))
    [id peer]
    (-> (d/let-flow [socket (client (:hostname peer) (:port peer))
                     _ (s/try-put! socket {:src (:id state) :type :peering-handshake} 100)]
          (s/connect socket (:rx-stream state) {:downstream? false})
          [id (assoc peer :socket socket)])
        (d/catch (fn [ex]
                   (warn (:id state) "caught exception in connecting to: " id))))))

(defmacro dissoc-in [s ks k]
  `(update-in ~s [~@ks] dissoc ~k))

(defn- reconnect-to-peer! [state peer-id p]
  (assert peer-id)
  (when-not (get-in @state [:peers peer-id :connect-pending])
    (swap! state assoc-in [:peers peer-id :connect-pending] true)
    (-> (d/chain
         (connect-to-peer @state [peer-id (get-in @state [:peers peer-id])])
         (fn [[peer-id peer-value]]
           (when peer-id
             (swap! state assoc-in [:peers peer-id] (dissoc peer-value :connect-pending)))))
        (d/catch (fn [ex]
                   (swap! state update-in [:peers peer-id] dissoc :connect-pending)
                   (warn ex "[" (:id @state) "] caught exception in reconnect-to-peer"))))))

(defn- send-peer-packet
  "sends a packet to the appropriate peer socket"
  [state p]
  (if-let [socket (get-in state [:peers (:dst p) :socket])]
    (if (stream-open? socket)
      (s/put! socket p)
      false)))


(defn- send-client-packet
  "send a packect to the client, won't reconnect"
  [state p]
  (let [client (:client-dst p)
        socket (get-in state [:clients client :socket])]
    (if (stream-closed? socket)
      (dissoc-in state [:clients client] :socket)
      (do
        (s/try-put! socket (dissoc p :client-dst) 100)
        state))))

(defn- send-pkt
  "dispatch method for sending packets"
  [state p]
  (cond
    (:dst p) (send-peer-packet state p)
    (:client-dst p) (send-client-packet state p)
    :else (do
            (warn (:id state) ": invalid pkt: " p)
            state)))

(defn- transmit!
  "sends all the currently queued packets"
  [state]
  (let [pkts (:tx-queue @state)]
    (swap! state assoc :tx-queue '())
    (d/future (doseq [p pkts]
       (when-not (send-pkt @state p)
         (reconnect-to-peer! state (:dst p) p))))))

(defn- handle-rx-pkt!
  "appends the pkt and runs update"
  [state dt pkt]
  (swap! state
         (fn [s]
           (-> (update s :rx-queue conj pkt)
               (server/update-state dt))))
  true)

(defn- handle-timeout!
  "timeout of dt seconds, just run the update"
  [state dt]
  (swap! state server/update-state dt)
  true)

(def ^:private default-timeout 50)

(defn event-loop
  "Runs the event loop for a server instance.  Returns a deferred."
  [state timeout]
  (d/loop [t (current-time)]
    (when-not (empty? (:tx-queue @state))
      (transmit! state))

    (if-not (stream-open? (:rx-stream @state))
      (do
        (warn (:id @state) " stream closed! exiting")
        :exit)
      (-> (d/chain (s/try-take! (:rx-stream @state) ::none
                                (or timeout default-timeout) ::timeout)
                   (fn [msg]
                     (when (condp = msg
                             ::none false
                             ::timeout (handle-timeout! state (delta-t t))
                             (handle-rx-pkt! state (delta-t t) msg))
                       (d/recur (current-time)))))
          (d/catch (fn [ex]
                     (warn ex (:id @state) ": caught exception in event loop")
                     (s/close! (:rx-stream @state))))))))


(defn start-server!
  "Starts the server listening."
  [server port]
  (swap! server
         (fn [s]
           (-> (assoc s :election-timeout (utils/random-election-timeout s))
               (f/become-follower nil))))
  (tcp/start-server
   (fn [s info]
     ((connection-handler server) (wrap-duplex-stream protocol s) info))
    {:port port :epoll? true}))

(def ^:private rx-buffer-size 16) ; no data for this random choice

(defn create-server
  "Creates a network instance of the server."
  [server port]
  (let [s (if (instance? clojure.lang.Atom server)
            server
            (atom server))]
    (swap! s assoc :rx-stream (s/stream (or (:rx-buffer-size @s) rx-buffer-size)))
    (swap! s assoc :server-socket (start-server! s port))
    (info "Listening on " port)
    s))

(defn stop-server
  "Stops the server listening socket and rx stream"
  [server]
  (assert (not (instance? clojure.lang.Atom server)))
  (-> server
      (update :server-socket (fn [^java.io.Closeable s]
                               (when s
                                 (.close s))
                               nil))
      (update :peers utils/mapf (fn [x]
                                  (when-let [socket (:socket x)]
                                    (s/close! socket))
                                  (dissoc (dissoc x :socket) :connect-pending)))
      (update :rx-stream (fn [x]
                           (when x
                             (s/close! x))
                           nil))))
