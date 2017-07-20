(ns tabby.client
  (:require [tabby.utils :as utils]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clj-http.client :as http]
            [tabby.net :as net]
            [tabby.utils :as utils]
            [clojure.tools.logging :refer [info warn]]
            [cheshire.core :as json]))

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
  [this host & {:keys [port http-port]}]
  (-> (close-socket this)
      (assoc :leader {:host (or (:host-override this) host) :port port
                      :http-port http-port})))

(defn- set-next-leader
  [client]
  (let [s (:servers client)
        n (concat (rest s) (list (first s)))]
    (info "client picking new leader" (first n))
    (assoc client
           :leader (first n)
           :servers n)))

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
                               (set-leader (close-socket c) (:hostname msg) :port (:port msg))
                               (set-next-leader (close-socket c)))
                             (inc times))))))))))

(defn success? [value]
  (= :ok (:value value)))

(defprotocol Client
  (set-value! [this key value])
  (get-value! [this key])
  (compare-and-swap! [this key new old])
  (close! [this]))

(defrecord TcpClient
    [client]
  Client

  (set-value! [this key value]
    (let [[c v] (send-pkt-sync @client {:type :set :key key :value value
                                      :uuid (utils/gen-uuid)})]
      (reset! client c)
      v))

  (get-value! [this key]
    (let [[c v] (send-pkt-sync @client {:type :get :key key :uuid (utils/gen-uuid)})]
      (reset! client c)
      v))

  (compare-and-swap! [this key new old]
    (let [[c v] (send-pkt-sync @client {:type :cas :body {:key key :new new :old old}
                                      :uuid (utils/gen-uuid)})]
      (reset! client c)
      v))

  (close! [this]
    (reset! client close-socket)))


(defn make-network-client [servers & {:keys [timeout] :or {timeout 15000}}]
  (->TcpClient (atom (set-next-leader {:servers servers :timeout timeout}))))

(defn- http-url [this]
  (str "http://" (get-in this [:leader :host])
       ":" (get-in this [:leader :http-port])))

(defn- key-xform [x]
  (.replaceAll (.toLowerCase (str x)) " " "-"))

(defn- parse-url [x]
  (let [url (java.net.URL. x)]
    [(.getHost url) (.getPort url)]))

(defmacro with-retries [x & body]
  `(loop [times 0]
    (try
      ~@body
      (catch ex
          (if (>= times ~x)
            (throw ex)
            (recur (inc times)))))))

(defrecord HttpClient
    [client]
  Client
  (set-value! [this key value]
    (let [pkt {:type :set :key (key-xform key) :value value
               :uuid (utils/gen-uuid)}
          r (http/post (str (http-url @client) "/keys/" (key-xform key))
                       {:form-params {:key (key-xform key) :value value}
                        :throw-exceptions false
                        :socket-timeout (:timeout @client) :conn-timeout (:timeout @client)
                        :as :json
                        :content-type :json
                        :accept :json})]
      (condp = (:status r)
        302 (do
              (swap! client (fn [x]
                              (let [[host port] (parse-url (get (:headers r) "Location"))]
                                (set-leader x host :http-port port))))
              (set-value! this key value))
        200 (:value (:body r))
        404 :not-found
        :else
        (throw (java.lang.Exception. (str "error: " (:status r)))))))

  (get-value! [this key]
    (let [r (http/get (str (http-url @client) "/keys/" (key-xform key))
                    {:query-params {:key key}
                     :as :json
                     :socket-timeout (:timeout @client) :conn-timeout (:timeout @client)
                     :accept :json}) ]
      (condp = (:status r)
        200 (:value (:body r))
        404 :not-found
        :else
        (throw (java.lang.Exception. (str "error: " (:status r)))))))

  (compare-and-swap! [this key new old]
    (let [r (http/post (str (http-url @client) "/keys/" (key-xform key) "/cas")
                       {:form-params {:key (key-xform key) :new new :old old}
                        :throw-exceptions false
                        :socket-timeout (:timeout @client) :conn-timeout (:timeout @client)
                        :as :json
                        :content-type :json
                        :accept :json})]
      (condp = (:status r)
        302 (do
              (swap! client (fn [x]
                              (let [[host port] (parse-url (get (:headers r) "Location"))]
                                (set-leader x host port))))
              (compare-and-swap! this key new old))
        200 (:value (:body r))
        404 :not-found
        :else
        (throw (java.lang.Exception. (str "error: " (:status r)))))))

  (close! [this]
    (swap! this close-socket)))


(defn make-http-client [servers & {:keys [timeout] :or {timeout 15000}}]
  (->HttpClient (atom (set-next-leader {:servers servers :timeout timeout}))))
