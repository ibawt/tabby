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
            [tabby.cluster :as cluster])
  (:import [java.net ServerSocket Socket]
           [java.io DataInputStream DataOutputStream]))

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

(defn incoming-message-loop
  [f state]
  (fn [s info]
    ;;; TODO: auth handshake
    (warn "connected..." info)
    (d/let-flow [handshake (s/take! s)]
                (warn "["(:id @state) "]" "handshake from " handshake )
                (swap! state assoc-in [:peer-sockets handshake] s)
                (d/loop []
                  (warn "do we get in here?" handshake)
                  (-> (s/take! s ::none)
                      (d/chain
                       (fn [msg]
                         (if (= ::none msg)
                           (s/close! s)
                           (do
                             (f msg)
                             (d/recur))))
                       (d/catch
                           (fn [ex]
                             (warn ex "in close")
                             (s/put! s (str "CLOSED:" ex))
                             (s/close! s)))))))))

(defn now []
  (System/currentTimeMillis))

(defn handle-timeout [state dt]
  (swap! state (partial server/update dt))
  true)

(defn handle-rx-pkt [state dt pkt]
  (warn "handle-rx-pkt")
  (swap! state (fn [s]
                 (-> s
                     (update-in [:rx-queue] conj pkt)
                     ((partial server/update dt)))))
  true)

(defn connect-to-peer [state peer]
  (let [socket @(client "localhost" (+ 8080 peer))]
    (warn "client connected")
    (utils/dbg s/put! socket (:id @state))
    (warn "after put")
    (swap! state assoc-in [:peer-sockets peer] socket)))

(defn send-pkt [state pkt]
  (warn "sending from: " (:id @state) " to: " (:dst pkt) " of type: " (:type pkt))
  (let [peer (:dst pkt)]
    (when-not (get-in @state [:peer-sockets peer])
      (warn "connecting to peer " peer)
      (connect-to-peer state peer))
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

(defn pkts-for-dst [state id]
  (filter #(= id (:dst %)) (:tx-queue state)))

(defn pkts-not-for-dst [state id]
  (filter #(not= id (:dst %)) (:tx-queue state)))

(defn handle-message [state]
  (fn [msg]
    (info "got message:" msg)
    (a/go
      (a/>! (:rx-chan @state) msg))))

(defn create-server [server]
  (let [s (atom (assoc server :time (System/currentTimeMillis)))
        socket (start-server (incoming-message-loop (handle-message s) s) (+ (:id server) 8080))
        e (event-loop s)]
    (swap! s merge {:event-loop e
                    :server-socket socket
                    :rx-chan (a/chan) :tx-chan (a/chan)})
    s))

(defn broadcast-pkts [s pkts])

(def servers {})

(defn start[]
  (alter-var-root #'servers (fn [x]
                              (utils/mapf (:servers (cluster/create 3)) create-server))))
(defn stop []
  (doall
   (utils/mapf servers (fn [server]
                         (.close (:server-socket @server))
                         (when-let [e (:event-loop @server)]
                           (a/close! e))
                         server))))
(defn reset []
  (stop)
  (refresh :after 'tabby.net/start))
