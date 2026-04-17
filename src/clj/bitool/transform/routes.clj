(ns bitool.transform.routes
  (:require [bitool.gil.compiler :as compiler]
            [bitool.transform :as transform]
            [cheshire.core :as json]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log]
            [ring.util.http-response :as http-response]))

(def ^:private max-text-length 4000)

(defn- ok [data]
  (-> (http-response/ok (json/generate-string {:ok true :data data}))
      (assoc-in [:headers "Content-Type"] "application/json; charset=utf-8")))

(defn- error-response [status error msg & [data]]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/generate-string (cond-> {:ok false
                                           :error error
                                           :message msg}
                                    data (assoc :data data)))})

(defn- body-params [request]
  (walk/keywordize-keys (merge (:params request) (:body-params request))))

(defn- graph-id-of [apply-result]
  (or (:graph-id apply-result)
      (:graph_id apply-result)))

(defn- graph-version-of [apply-result]
  (or (:version apply-result)
      (:graph-version apply-result)
      (:graph_version apply-result)))

(defn from-nl
  "POST /transform/from-nl
   Body: {text: '...', do_apply: false}
   Returns: constrained GIL + preview, and optionally applies it."
  [request]
  (try
    (let [{:keys [text do_apply]} (body-params request)
          input-text (str (or text ""))
          _      (when-not (seq input-text)
                   (throw (ex-info "text is required" {:status 400 :error "bad_request"})))
          _      (when (> (count input-text) max-text-length)
                   (throw (ex-info "text is too long" {:status 413
                                                       :error "payload_too_large"
                                                       :max_length max-text-length})))
          {:keys [plan planner gil validation preview text] :as result} (transform/plan-from-text input-text)
          preview-text text]
      (if-not (:valid validation)
        (error-response 400 "validation_failed" "Generated transform graph is invalid."
                        {:plan plan
                         :planner planner
                         :gil gil
                         :preview preview
                         :validation validation})
        (if do_apply
          (let [apply-result (compiler/apply-gil (:normalized-gil validation) (:session request))
                session (:session request)
                gid (graph-id-of apply-result)
                ver (graph-version-of apply-result)]
            (-> (ok {:plan plan
                     :planner planner
                     :gil (:normalized-gil validation)
                     :preview preview
                     :validation validation
                     :text preview-text
                     :result apply-result})
                (assoc :session (assoc session
                                       :gid gid
                                       :ver ver))))
          (ok {:plan plan
               :planner planner
               :gil (:normalized-gil validation)
               :preview preview
               :validation validation
               :text preview-text
               :plan_preview (compiler/plan-gil (:normalized-gil validation))}))))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400)
                        (or (:error d) "bad_request")
                        (ex-message e))))
    (catch Exception e
      (log/error e "Transform from-nl failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn apply-transform
  "POST /transform/apply
   Body: {gil: {...}}"
  [request]
  (try
    (let [{:keys [gil]} (body-params request)
          _ (when-not (map? gil)
              (throw (ex-info "gil is required" {:status 400 :error "bad_request"})))
          validation (transform/validate-transform-gil gil)]
      (if-not (:valid validation)
        (error-response 400 "validation_failed" "Transform graph is invalid." {:validation validation})
        (let [apply-result (compiler/apply-gil (:normalized-gil validation) (:session request))
              session (:session request)
              gid (graph-id-of apply-result)
              ver (graph-version-of apply-result)]
          (assoc
           (ok {:gil (:normalized-gil validation)
                :validation validation
                :result apply-result})
           :session
           (assoc session
                  :gid gid
                  :ver ver)))))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400)
                        (or (:error d) "bad_request")
                        (ex-message e))))
    (catch Exception e
      (log/error e "Transform apply failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn transform-routes []
  ["/transform"
   ["/from-nl" {:post from-nl}]
   ["/apply" {:post apply-transform}]])
