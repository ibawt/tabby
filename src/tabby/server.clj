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

(defn- valid-term? [state params]
  (>= (:term params) (get-in @state [:persistent :current-term])))

(defn- request-vote [state params]
  (if (valid-term? state params)
    (false)
    (if-not (get-in @state [:persistent :voted-for])
      (swap! state update-in [:persistent :voted-for] (:candidate-id params))
      (false))))

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

(defn- broadcast-heartbeat [state]
  (doall (pmap heart-beat (:peers state))))

(defn- become-leader [state]
  (swap! state #(->
                 %1
                 (assoc :type :leader)
                 (assoc :next-index (map 0 (:peers @state))) ; these values are wrong
                 (assoc :match-index (map 0 (:peers @state)))))
  (broadcast-heartbeat state))

(defn- send-request-vote [state peer])

(defn- won-election? [state results]
  (> (reduce #(+ %1 (if (:granted %2) 1 0)) results)) (/ (count (:peers @state)) 2))

(defn- trigger-election [state]
  (swap! state #(->
                 %1
                 (update-in [:persistent :current-term] inc)
                 (assoc :type :canditate)
                 (update-in [:persistent :voted-for] (:id state))))
  (let [results (pmap (comp send-request-vote state) (:peers @state))]
    (if (won-election? state results)
      (become-leader state))))

(defn- choose-election-timeout [state]
  (swap! state assoc :election-timeout (+ 150 (.nextInt (java.util.Random.) 150))))

(defn- election-timeout? [state])

(defn- spawn-election-timeout [state]
  (future (while true
            (let [t1 (System/currentTimeMillis)]
              (choose-election-timeout state)
              (Thread/sleep (:election-timeout @state))
              (if (election-timeout? state)
                (trigger-election state))))))

(defn create-server [id port]
  (let [state (atom {:persistent {:current-term 0
                                  :voted-for nil
                                  :log []}
                     :id id
                     :volatile {:commit-index 0
                                :last-applied 0}
                     :type :follower
                     :election-timeout 0
                     :peers []
                     :db {}})]

    (spawn-election-timeout state)
    {:server (ring/serve (wrap-defaults (server-routes state) api-defaults)
                         {:port port :open-browser? false})
     :state state}))

(defn close-server [server]
  (.stop (:server server)))
