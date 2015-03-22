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

(defmacro sleep
  ([ms stop-channel]
   `(a/alts! [(a/timeout ~ms) ~stop-channel]))
  ([ms]
   `(a/<! (a/timeout ~ms))))

(defmacro poll-go-loop [bindings & body]
  (let [stop (first bindings)]
    `(let [~stop (a/chan)]
       (a/go (while (a/alt! ~stop false :default :keep-going)
               ~@body))
       ~stop)))

(defn peering-handshake [state pkt]
  (if (= :peering-handshake (:type pkt))
    {:type :peering-ok :src (:id @state)}
    {:type :peering-fail :src (:id @state)}))

(def protocol
  (gloss/compile-frame
   (gloss/finite-frame :uint32
                       (gloss/string :utf-8))
   pr-str
   edn/read-string))

(defn wrap-duplex-stream
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

(defn start-server
  [handler port]
  (tcp/start-server
   (fn [s info]
     (handler (wrap-duplex-stream protocol s) info))
   {:port port}))

(defn connection-handler
  [state]
  (fn [s info]
    (d/let-flow
     [handshake (s/take! s)]
     (if (= :peering-handshake (:type handshake))
       (s/connect s (:rx-chan @state))
       (do
         (warn "ARGH")
         ;(client-message-loop s state)
         )))))

(defn now []
  (System/currentTimeMillis))

(defn handle-timeout [state dt]
  (swap! state (partial server/update dt))
  true)

(defn connect-to-peer [state peer]
  (let [socket @(client "localhost" (+ 8080 peer))]
    (d/let-flow [_ (s/put! socket {:src (:id @state) :type :peering-handshake})]
                (swap! state assoc-in [:peer-sockets peer] socket))))

(defn send-pkt [state pkt]
  (let [peer (:dst pkt)]
    (when-not (get-in @state [:peer-sockets peer])
      @(connect-to-peer state peer))
    (s/put! (get-in @state [:peer-sockets peer]) pkt))
  true)

(defn transmit [s]
  (loop []
    (if (empty? (:tx-queue @s))
      s
      (do
        (send-pkt s (first (:tx-queue @s)))
        (swap! s update-in [:tx-queue] rest)
        (recur)))))

(defn handle-rx-pkt [state dt pkt]
  (swap! state (fn [s]
                 (server/update dt (update-in s [:rx-queue] conj pkt))))
  true)

(defn event-loop [state]
  (let [stop (a/chan)]
    (a/go-loop [t (now)]
      (transmit state)
      (if (a/alt!
            (:rx-chan @state) ([v] (handle-rx-pkt state (- (now) t) v))
            stop false
            (a/timeout 10) (handle-timeout state (- (now) t))
            (:tx-chan @state) ([v] (send-pkt state v)))
        (recur (now))
        :stopped))
    stop))

(defn create-server [server]
  (let [s (atom (assoc server :time (System/currentTimeMillis)))
        socket (start-server (connection-handler s) (+ (:id server) 8080))]
    (swap! s merge {:server-socket socket
                    :rx-chan (a/chan) :tx-chan (a/chan)})
    s))

(def servers {})

(defn connect-to-peers [server]
  (doseq [peer (:peers @server)]
    @(connect-to-peer server peer)))

(defn start[]
  (alter-var-root #'servers (fn [x]
                              (utils/mapf (:servers (cluster/create 3)) create-server)))
  (doseq [[id server] servers]
    (connect-to-peers server)
    (swap! server assoc :event-loop (event-loop server))))

(defn stop []
  (doall
   (utils/mapf servers (fn [server]
                         (.close (:server-socket @server))
                         (when-let [e (:event-loop @server)]
                           (a/close! e))
                         (doall (utils/mapf (:peer-sockets @server) s/close!))
                         server))))
(defn reset []
  (stop)
  (refresh :after 'tabby.net/start))
