(ns bitool.handler
  (:require
    [bitool.middleware :as middleware]
    [bitool.lifecycle :as lifecycle]
    [bitool.layout :refer [error-page]]
    [bitool.routes.home :refer [home-routes]]
    [bitool.ops.routes :refer [ops-routes]]
    [bitool.pipeline.routes :refer [pipeline-routes]]
    [bitool.transform.routes :refer [transform-routes]]
    [bitool.semantic.routes :refer [semantic-routes]]
    [bitool.endpoint :as endpoint]
    [cheshire.core :as json]
    [reitit.ring :as ring]
    [ring.middleware.content-type :refer [wrap-content-type]]
    [ring.middleware.webjars :refer [wrap-webjars]]
    [bitool.env :refer [defaults]]
    [mount.core :as mount]))

(defn- json-response
  [status body]
  {:status status
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (json/generate-string body)})

(defn health-route
  [_request]
  (json-response 200 (lifecycle/health-payload)))

(defn ready-route
  [_request]
  (let [payload (lifecycle/readiness-payload)]
    (json-response (if (:ready payload) 200 503) payload)))

(mount/defstate init-app
  :start (do
           ((or (:init defaults) (fn []))))
  :stop  ((or (:stop defaults) (fn []))))

(defn- async-aware-default-handler
  ([_] nil)
  ([_ respond _] (respond nil)))


(mount/defstate app-routes
  :start
  (ring/ring-handler
    (ring/router
      [(home-routes)
       (ops-routes)
       (pipeline-routes)
       (transform-routes)
       (semantic-routes)
       ["/health" {:get health-route}]
       ["/ready" {:get ready-route}]])
    (ring/routes
      (ring/create-resource-handler
        {:path "/"})
      (fn [request]
        (when-let [resp (endpoint/dynamic-endpoint-handler request)]
          resp))
      (wrap-content-type
        (wrap-webjars async-aware-default-handler))
      (ring/create-default-handler
        {:not-found
         (constantly (error-page {:status 404, :title "404 - Page not found"}))
         :method-not-allowed
         (constantly (error-page {:status 405, :title "405 - Not allowed"}))
         :not-acceptable
         (constantly (error-page {:status 406, :title "406 - Not acceptable"}))}))))

(defn app []
  (middleware/wrap-base #'app-routes))
