(ns tabby.http-server
  (:require
   [tabby.utils :as utils]
   [cheshire.core :as json]
   [clojure.tools.logging :refer [warn info]]
   [tabby.client-state :as cs]
   [compojure.core :as compojure :refer [GET POST]]
   [ring.middleware.params :as params]
   [ring.middleware.json :refer [wrap-json-params wrap-json-response]]
   [compojure.route :as route]
   [aleph.http :as http]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [tabby.client-state :as cs]))

(defn- do-request [state pkt]
  (let [client-id (utils/gen-uuid)
        request-id (utils/gen-uuid)
        rx-stream (s/stream)]
    (swap! state update :clients cs/create-client client-id rx-stream)
    (-> (d/chain
         (s/try-put! (:rx-stream @state) (assoc pkt :client-id client-id
                                                :uuid request-id) 1000)
          (fn [_]
            (s/try-take! rx-stream ::none 10000 ::timeout))
          (fn [msg]
            (s/close! rx-stream)
            (swap! state update :clients dissoc client-id)
            (cond
              (= ::none msg) {:status 502 :body "stream is closed!"}
              (= ::timeout msg) {:status 504 :body "timeout"}
              (= :redirect (:type msg)) {:status 302 :headers
                                         {"Location" (str (or (:http-scheme @state) "http://") (:hostname msg) ":" (:http-port msg))}}
              (= nil (:value (:body msg))) {:status 404 :body "Not found"}
              :else
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string (:body msg))})))
         (d/catch (fn [^java.lang.Exception ex]
                    (warn ex "caught exception")
                    {:status 500 :body (.getMessage ex)})))))

(defn start! [state]
  (let [s (-> (compojure/routes
               (GET "/keys/:key" req
                    (do-request state {:type :get :key (get-in req [:params :key])}))
               (POST "/keys/:key" req
                     (do-request state {:type :set :key (get-in req [:params "key"])
                                        :value (get-in req [:params "value"])}))
               (POST "/keys/:key/cas" [key])
               (GET "/ping" []
                    {:status 200 :body ":PONG"})
               (GET "/status" []
                    {:status 200
                     :headers {"Content-Type" "application/json"}
                     :body (json/generate-string
                                        (select-keys @state [:leader-id :type :commit-index :current-term :id]))})
               (route/not-found "not found"))
              (wrap-json-response)
              (params/wrap-params)
              (wrap-json-params)
              (http/start-server {:port (or (:http-port @state) 8080)}))]
    (swap! state assoc :http-server s))
  state)

(defn stop! [state]
  (.close ^java.io.Closeable (:http-server @state))
  (swap! state dissoc :http-server))
