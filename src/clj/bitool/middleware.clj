(ns bitool.middleware
  (:require
    [bitool.env :refer [defaults]]
    [bitool.layout :refer [error-page]]
    [bitool.middleware.formats :as formats]
    [bitool.config :refer [env]]
    [taoensso.telemere :as tel]
    [ring.middleware.reload :refer [wrap-reload]]
    [bitool.exception_handler :as exc]
       [clojure.tools.logging :as log]
    [ring.middleware.anti-forgery :refer [wrap-anti-forgery]]
    [muuntaja.middleware :refer [wrap-format wrap-params]]
    [muuntaja.core :as m]
    [ring.adapter.undertow.middleware.session :refer [wrap-session]]
    [ring.middleware.defaults :refer [site-defaults wrap-defaults]]
    [ring.middleware.flash :refer [wrap-flash]]
  ;;  [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
  ;;  [buddy.auth.accessrules :refer [restrict]]
 ;;   [buddy.auth :refer [authenticated?]]
 ;;   [buddy.auth.backends.session :refer [session-backend]]
    [clojure.java.io :as io]
    [ring.middleware.session.cookie :refer [cookie-store]]
    [ring.middleware.keyword-params :refer [wrap-keyword-params]]
    ;; REMOVE these - they conflict with Muuntaja:
    ;; [ring.middleware.json :refer [wrap-json-body wrap-json-response wrap-json-params]]
    [ring.middleware.params :refer [wrap-params]]))

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
  (let [wrapped (wrap-format handler formats/instance)]  ;; Remove wrap-params from here
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

(defn wrap-merge-params
  "Merge body-params into params for convenience"
  [handler]
  (fn [request]
    (handler (update request :params merge (:body-params request)))))


(defn wrap-log-request
  "Middleware to log the request after body has been parsed."
  [handler]
  (fn [request]
    (tel/log! {:level :info
               :msg "Incoming Request"
               :data {:method (:request-method request)
                      :uri (:uri request)
                      :headers (:headers request)
                      :body-params (:body-params request)  ;; Use parsed body
                      :params (:params request)}})
    (handler request)))

(defn wrap-log-response
  "Log outgoing responses"
  [handler]
  (fn [request]
    (let [response (handler request)]
      (log/info "Response:"
                {:status (:status response)
                 :uri (:uri request)})
      response)))

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
   ;;   wrap-json-response 
   ;;   wrap-json-params
        wrap-format 
      wrap-keyword-params
   ;;   wrap-params 
    ;;    wrap-muuntaja
      wrap-log-request
      wrap-log-response
      wrap-internal-error))


)

(defn wrap-debug-raw-body [handler]
  (fn [request]
    (println "=== RAW REQUEST DEBUG ===")
    (println "Method:" (:request-method request))
    (println "URI:" (:uri request))
    (println "Content-Type:" (get-in request [:headers "content-type"]))
    (println "Query params:" (:query-params request))
    (println "Form params:" (:form-params request))
    (println "Body-params:" (:body-params request))
    (println "Params:" (:params request))
    (let [response (handler request)]
      (println "=== AFTER HANDLER ===")
      (println "Body-params after:" (:body-params request))
      (println "Params after:" (:params request))
      response)))

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
                                    :secure     true
                                    :same-site  :lax}})  
      (wrap-session-doctor "sid")
      (wrap-defaults
        (-> site-defaults
            (assoc-in [:security :anti-forgery] false)
            (assoc-in [:session :cookie-name] "example-app-sessions")))
      
      ;; Use ONLY Muuntaja for content negotiation
      wrap-merge-params 
      wrap-format  ;; This is muuntaja.middleware/wrap-format
      
      ;; REMOVE these lines - they conflict:
      ;; (wrap-json-body {:keywords? true})
      ;; wrap-json-response            
      ;; wrap-json-params
      
      wrap-keyword-params
      wrap-debug-raw-body
      wrap-log-request
      wrap-log-response
      wrap-internal-error))
)
(comment
(defn wrap-base [handler]
  (-> handler 
      (wrap-defaults
        (-> site-defaults
            (assoc-in [:security :anti-forgery] false)
            (dissoc :params)))  ;; Remove params from defaults
      
      wrap-merge-params
      wrap-keyword-params
      wrap-params
      (wrap-format formats/instance)  ;; Muuntaja
      
      wrap-internal-error))
)

(defn wrap-base [handler]
  (println "Environment mode:" (if (env :dev) "DEVELOPMENT" "PRODUCTION"))
  (println "Full env keys:" (keys env))
  (-> (if (env :dev)
        (wrap-reload handler {:dirs ["src"]})  ;; Add hot-reloading in dev
        handler)
      
      ;; Exception handling
      exc/wrap-exception-info
      wrap-internal-error
      
      ;; Session & flash
      wrap-flash
      (wrap-session {:store (cookie-store {:key cookie-key})
                     :cookie-name "sid"
                     :cookie-attrs {:http-only true
                                    :secure     (not (env :dev))  ;; false in dev, true in prod
                                    :same-site  :lax}})
      (wrap-session-doctor "sid")
      
      ;; Site defaults (security, etc)
      (wrap-defaults
        (-> site-defaults
            (assoc-in [:security :anti-forgery] false)
            (dissoc :params)))
      
      ;; Logging (after body is parsed)
      wrap-log-response
      wrap-log-request
      wrap-merge-params      ;; MERGE happens HERE (after both sources exist)
      wrap-keyword-params    ;; Keywordize params
      wrap-params            ;; Parse query/form params -> :params
      wrap-format ))  ;
