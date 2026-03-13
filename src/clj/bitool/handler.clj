(ns bitool.handler
  (:require
    [bitool.middleware :as middleware]
    [bitool.layout :refer [error-page]]
    [bitool.routes.home :refer [home-routes]]
    [bitool.endpoint :as endpoint]
    [reitit.ring :as ring]
    [ring.middleware.content-type :refer [wrap-content-type]]
    [ring.middleware.webjars :refer [wrap-webjars]]
    [bitool.env :refer [defaults]]
    [mount.core :as mount]))

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
      [(home-routes)])
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
