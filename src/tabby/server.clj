(ns tabby.server
  (:require [compojure.core :refer :all]
            [ring.server.standalone :as ring]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.json :as json]
            [ring.adapter.jetty :as jetty]
            [ring.util.response :as response]
            [compojure.route :as route]
            [clj-http.client :as http]))

(defn- get-key [state params])

(defn- set-key [state params])

(defn- append-entries [state params])

(defn- get-state [state]
  (json/wrap-json-response (response/response @state)))

(defn- invalid-term? [state params]
  (>= (:term params) (:current-term @state)))

(defn- request-vote [state params]
  (print "[" (:id @state) "] - received request-vote: " params)
  (if (invalid-term? state params)
    false
    (if-not (:voted-for @state)
      (do
        (swap! state assoc :voted-for (:candidate-id params))
        (json/wrap-json-response (response/response {:term (:current-term @state)
                                                     :vote-granted true})))
      false)))

(defn- server-routes [state]
  (routes (GET "/:key" {params :params} (get-key state params))
          (POST "/:key" {params :params} (set-key state params))
          (POST "/append_entries" {params :params} (append-entries state params))
          (POST "/request_vote" {params :params} (request-vote state params))
          (GET "/state" request (get-state state))))

(defn- get-log-index [state] 0)

(defn- get-log-term [state] 0)

(defn- heart-beat [state peer]
  (http/post (str peer "/append_entries")
             {:form-params {:term (:current-term @state)
                            :leader-id (:id @state)
                            :prev-log-index (get-log-index state)
                            :prev-log-term (get-log-term state)
                            :entries []
                            :leader-commit (:commit-index @state)}
              :content-type :json}))

(defn- send-request-vote [state peer]
  (http/post (str peer "/request_vote")
             {:form-params {:term (:current-term @state)
                            :candidate-id (:id @state)
                            :last-log-index (:commit-index @state)
                            :last-log-term (get-log-term state)}
              :content-type :json}))


(defn- broadcast-heartbeat [state]
  (doall (pmap heart-beat (:peers state))))

(defn- become-leader [state]
  (swap! state #(->
                 %1
                 (assoc :type :leader)
                 (assoc :next-index (map 0 (:peers @state))) ; these values are wrong
                 (assoc :match-index (map 0 (:peers @state)))))
  (broadcast-heartbeat state))


(defn- won-election? [state results]
  (> (reduce #(+ %1 (if (:granted %2) 1 0)) results)) (/ (count (:peers @state)) 2))

(defn- trigger-election [state]
  (swap! state #(->
                 %1
                 (assoc :current-term inc)
                 (assoc :type :canditate)
                 (assoc :voted-for (:id state))))
  (let [results (pmap (comp send-request-vote state) (:peers @state))]
    (if (won-election? state results)
      (become-leader state))))

(defn- choose-election-timeout [state]
  (swap! state assoc :election-timeout (+ 150 (.nextInt (java.util.Random.) 150))))

(defn- election-timeout? [state])

(defn- spawn-election-timeout [state]
  (future (while (:keep-running @state)
            (let [t1 (System/currentTimeMillis)]
              (choose-election-timeout state)
              (Thread/sleep (:election-timeout @state))
              (if (election-timeout? state)
                (trigger-election state))))))

(defn set-peers [server peers])

(defn create-server [id port]
  (let [state (atom {:current-term 0 ; persist
                     :voted-for nil  ; persist
                     :log []         ; persist
                     :id id          ; user assigned
                     :commit-index 0
                     :last-applied 0
                     :type :follower
                     :election-timeout 0
                     :peers []
                     :keep-running true
                     :db {}})]

    (spawn-election-timeout state)
    {:server (ring/serve (wrap-defaults (server-routes state) api-defaults)
                         {:port port :open-browser? false})
     :state state}))

(defn close-server [server]
  (swap! (:state server) assoc :keep-running false)
  (.stop (:server server)))
