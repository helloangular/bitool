(ns bitool.middleware
  (:require
    [bitool.env :refer [defaults]]
    [clojure.tools.logging :as log]
  ;;  (:require [taoensso.timbre :as log]
    [bitool.layout :refer [error-page]]
    [ring.middleware.anti-forgery :refer [wrap-anti-forgery]]
    [bitool.middleware.formats :as formats]
    [muuntaja.core :as m]
    [muuntaja.middleware :refer [wrap-format wrap-params]]
    [ring.middleware.keyword-params :refer [wrap-keyword-params]]
    [bitool.config :refer [env]]
    [bitool.exception_handler :as exc]
    [cheshire.core :as json] 
    [clojure.java.io :as io]
    [ring.middleware.json :refer [wrap-json-params wrap-json-body wrap-json-response]]
    [ring.middleware.flash :refer [wrap-flash]]
    [ring.adapter.undertow.middleware.session :refer [wrap-session]]
    [ring.middleware.session.cookie :refer [cookie-store]]
    [ring.middleware.defaults :refer [site-defaults wrap-defaults]])
  )

(defn wrap-internal-error [handler]
  (let [error-result (fn [^Throwable t]
                       (log/error t (.getMessage t))
                       (error-page {:status 500
                                    :title "Something very bad has happened!"
                                    :message "We've dispatched a team of highly trained gnomes to take care of the problem."}))]
    (fn wrap-internal-error-fn
      ([req respond _]
       (handler req respond #(respond (error-result %))))
      ([req]
       (try
         (handler req)
         (catch Throwable t
           (error-result t)))))))

(defn wrap-csrf [handler]
  (wrap-anti-forgery
    handler
    {:error-response
     (error-page
       {:status 403
        :title "Invalid anti-forgery token"})}))


(defn wrap-formats [handler]
  (let [wrapped (-> handler wrap-params (wrap-format formats/instance))]
    (fn
      ([request]
         ;; disable wrap-formats for websockets
         ;; since they're not compatible with this middleware
       ((if (:websocket? request) handler wrapped) request))
      ([request respond raise]
       ((if (:websocket? request) handler wrapped) request respond raise)))))


(defn read-body [request]
  "Safely reads and returns the body of the request as a string."
  (let [body (slurp (:body request))] ;; Read input-stream
    ;; Replace the input stream so the handler can still access the body
    (assoc request :body (io/input-stream (.getBytes body)))
    body))

(defn wrap-log-request
  "Middleware to log the request body safely without closing the stream."
  [handler]
  (fn [request]
    (let [body (slurp (:body request))] ;; Read and store the body as a string
      (log/info "Incoming Request:"
                {:method  (:request-method request)
                 :uri     (:uri request)
                 :headers (:headers request)
                 :body    body})
      ;; Replace the original consumed body with a fresh input-stream
      (handler (assoc request :body (io/input-stream (.getBytes body "UTF-8")))))))


(defn response-body-to-string [body]
  "Converts a Ring response body to a string, safely handling streams."
  (cond
    (string? body) body
    (instance? java.io.InputStream body) (let [body-str (slurp body)]
                                           ;; Replace the body stream after reading
                                           {:body-str body-str
                                            :new-body (io/input-stream (.getBytes body-str "UTF-8"))})
    (map? body) (cheshire.core/generate-string body)
    :else (str body)))

(defn wrap-log-response
  "Middleware to log response bodies safely and preserve the original stream."
  [handler]
  (fn [request]
    (let [response (handler request)
          {:keys [body-str new-body]} (response-body-to-string (:body response))]
      (log/info "Outgoing Response:"
                {:status  (:status response)
                 :headers (:headers response)
                 :body    body-str})
      ;; Replace the response body with the new input stream
      (if new-body
        (assoc response :body new-body)
        response))))


(defn wrap-log-request-response
  "Middleware to log the incoming request and outgoing response."
  [handler]
  (fn [request]
    ;; Log the incoming request
    (log/info "Incoming Request:"
              {:method (:request-method request)
               :uri    (:uri request)
               :headers (:headers request)
               :params (:params request)})
    (let [response (handler request)]
      ;; Log the outgoing response
      (log/info "Outgoing Response:"
                {:status  (:status response)
                 :headers (:headers response)
                 :body    (if (string? (:body response))
                            (:body response)
                            "Non-string body (e.g., stream)")})
      response)))

(def muuntaja-instance
  (m/create m/default-options))

(defn wrap-muuntaja [handler]
  (wrap-format handler muuntaja-instance))

;; Must be exactly 16, 24, or 32 bytes (ASCII characters = 1 byte each)
(def cookie-key (.getBytes "0123456789abcdef")) ;; 16 bytes = AES-128

(defn wrap-session-doctor [handler cookie-name]
  (fn [req]
    (let [in-cookie (get-in req [:cookies cookie-name :value])
          in-sess   (:session req)]
      (println ">>> SESSION-DOCTOR: incoming cookie?" (boolean in-cookie)
               "| incoming :session keys:" (when (map? in-sess) (keys in-sess)))
      (let [resp (handler req)
            out-sess (:session resp)
            out-cookies (:cookies resp)]
        (println ">>> SESSION-DOCTOR: outgoing :session keys:" (when (map? out-sess) (keys out-sess)))
        (println ">>> SESSION-DOCTOR: outgoing Set-Cookie keys:"
                 (when (map? out-cookies) (keys out-cookies)))
        resp))))

(comment
(defn wrap-base [handler]
   (println "Environment mode:" (if (env :dev) "DEVELOPMENT" "PRODUCTION"))
  (println "Full env keys:" (keys env))
  (-> handler 
      exc/wrap-exception-info
      ((:middleware defaults))
      wrap-flash
      (wrap-session {:store (cookie-store {:key cookie-key})
                     :cookie-name "sid"
                     :cookie-attrs {:http-only true
                                    :secure     true     ;; true in prod (HTTPS)
                                    :same-site  :lax}})  
;;      (wrap-session {:cookie-attrs {:http-only true}})
      (wrap-session-doctor "sid")
      (wrap-defaults
        (-> site-defaults
            (assoc-in [:security :anti-forgery] false)
            (assoc-in [:session :cookie-name] "example-app-sessions")))
      (wrap-json-body {:keywords? true})
      wrap-json-response 
      wrap-json-params
      wrap-keyword-params
   ;;     wrap-format 
   ;;   wrap-params 
    ;;    wrap-muuntaja
      wrap-log-request
      wrap-log-response
      wrap-internal-error))


)

(defn wrap-base [handler]
  (println "Environment mode:" (if (env :dev) "DEVELOPMENT" "PRODUCTION"))
  (println "Full env keys:" (keys env))
  (-> handler 
      exc/wrap-exception-info 
      wrap-flash
      (wrap-session {:store (cookie-store {:key cookie-key})
                     :cookie-name "sid"
                     :cookie-attrs {:http-only true
                                    :secure     true
                                    :same-site  :lax}})  
      (wrap-session-doctor "sid")
      (wrap-defaults
        (-> site-defaults
            (assoc-in [:security :anti-forgery] false)
            (assoc-in [:session :cookie-name] "example-app-sessions")
            (dissoc :params)))  ;; Remove params from defaults
      wrap-format        ;; Muuntaja handles everything
      wrap-keyword-params         ;; For query params
      wrap-params                 ;; For form/query params
      ;; REMOVE: wrap-json-body, wrap-json-params, wrap-json-response
      wrap-log-request
      wrap-log-response
      wrap-internal-error))
