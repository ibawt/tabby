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
                                      (a/>! (:rx-chain @state) (merge msg {:client-id client-index})))
                                    (d/recur))))))))))

(defn now []
  (System/currentTimeMillis))

(defn handle-timeout [state dt]
  (swap! state (fn [s] (server/update dt s)))
  true)

(defn connect-to-peer [state peer]
  (let [socket @(client "localhost" (+ 8080 peer))]
    (d/let-flow [_ (s/put! socket {:src (:id @state) :type :peering-handshake})]
                (swap! state assoc-in [:peer-sockets peer] socket))))

(defn send-pkt [state pkt]
  (when-let [peer (:dst pkt)]
    (when-not (get-in @state [:peer-sockets peer])
      @(connect-to-peer state peer))
    (s/put! (get-in @state [:peer-sockets peer]) pkt))
  (when-let [client (:client-dst @state)]
    (s/put! (get-in @state [:clients client :socket]) pkt))
  true)

(defn transmit [state]
  (swap! state (fn [s]
                 (doseq [pkt (:tx-queue s)]
                   (send-pkt state pkt))
                 (assoc s :tx-queue '()))))

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
            (a/timeout 10) (handle-timeout state (- (now) t)))
        (recur (now))
        :stopped))
    stop))

(defn create-server [server]
  (let [s (atom server)
        socket (start-server (connection-handler s) (+ (:id server) 8080))]
    (swap! s merge {:server-socket socket
                    :rx-chan (a/chan)})
    s))

(def servers {})

(defn connect-to-peers [server]
  (doseq [peer (:peers @server)]
    (connect-to-peer server peer)))

(defn start[]
  (alter-var-root #'servers (fn [x]
                              (utils/mapf (:servers (cluster/create 3)) create-server)))
  (doseq [[id server] servers]
    (connect-to-peers server)
    (swap! server assoc :event-loop (event-loop server))))

(defn stop []
  (doall
   (utils/mapf servers (fn [server]
                         (when-let [e (:event-loop @server)]
                           (a/close! e))
                         (when-let [ s (:server-socket @server)]
                                   (.close s))
                         (doall (utils/mapf (:peer-sockets @server) s/close!))
                         (swap! server merge {:event-loop nil
                                              :server-socket nil})))))
(defn reset []
  (stop)
  (refresh :after 'tabby.net/start))
