(ns bitool.exception_handler
	(:require [taoensso.telemere :as tel]
                  [cheshire.core :as json]))

(defn error
  ([msg] 
   (tel/log! {:level :error} msg))
  ([msg data] 
   (tel/log! {:level :error :data data} msg)))

(defn info
  ([msg] 
   (tel/log! {:level :info} msg))
  ([msg data] 
   (tel/log! {:level :info :data data} msg)))

(defn warn
  ([msg] 
   (tel/log! {:level :warn} msg))
  ([msg data] 
   (tel/log! {:level :warn :data data} msg)))

(defn debug
  ([msg] 
   (tel/log! {:level :debug} msg))
  ([msg data] 
   (tel/log! {:level :debug :data data} msg)))

(def error-type-config
  {:valid-err {:status 400
                      :error "Validation failed"
                      :include-details? true}
   :not-found {:status 404
               :error "Not found"
               :include-details? false}
   :unauthorized {:status 401
                  :error "Unauthorized"
                  :include-details? false}
   :database-error {:status 500
                    :error "Database error"
                    :include-details? false
                    :user-message "A database operation failed"}})

(defn wrap-exception-info [handler]
  (fn wrap-exception-info-fn
    ([req]
     (try
       (handler req)
       (catch clojure.lang.ExceptionInfo e
         (let [data (ex-data e)
               type (:type data)
               config (get error-type-config type)]
           (error (ex-message e) data)
           
           (if config
             (let [body-data (cond-> {:error (:error config)
                                      :message (or (:user-message config)
                                                   (ex-message e))}
                               (:include-details? config)
                               (assoc :details (dissoc data :type)))]
               {:status (:status config)
                :headers {"Content-Type" "application/json"}
                :body (json/generate-string body-data)})
             
             ;; Unknown type - rethrow
             (throw e))))))))
