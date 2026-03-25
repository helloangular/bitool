(ns bitool.pipeline.routes
  "API routes for intent-based pipeline creation."
  (:require [bitool.pipeline.intent :as intent]
            [bitool.pipeline.planner :as planner]
            [bitool.pipeline.compiler :as compiler]
            [bitool.pipeline.preview :as preview]
            [bitool.pipeline.sdp :as sdp]
            [ring.util.http-response :as http-response]
            [cheshire.core :as json]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log]))

(defn- ok [data]
  (-> (http-response/ok (json/generate-string {:ok true :data data}))
      (assoc-in [:headers "Content-Type"] "application/json; charset=utf-8")))

(defn- error-response [status error msg]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/generate-string {:ok false :error error :message msg})})

;; ---------------------------------------------------------------------------
;; Route handlers
;; ---------------------------------------------------------------------------

(defn from-nl
  "POST /pipeline/from-nl
   Body: {text: '...natural language...', use_mock: true/false}
   Returns: PipelineSpec + preview"
  [request]
  (try
    (let [params (merge (:params request) (:body-params request))
          text   (or (:text params) (get params "text"))
          _      (when (empty? text)
                   (throw (ex-info "text is required" {:status 400 :error "bad_request"})))
          use-mock (boolean (or (:use_mock params) (get params "use_mock")))
          intent (if use-mock
                   (intent/parse-intent-mock text)
                   (try
                     (intent/parse-intent text)
                     (catch Exception llm-err
                       (log/warn llm-err "LLM intent parse failed, falling back to mock parser")
                       (intent/parse-intent-mock text))))
          spec   (planner/plan-pipeline intent)
          prev   (preview/generate-preview spec)]
      (ok {:intent  intent
           :spec    spec
           :preview prev
           :text    (preview/preview-text prev)}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline from-nl failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn preview-pipeline
  "POST /pipeline/preview
   Body: PipelineSpec (or PipelineIntent)
   Returns: structured preview + text"
  [request]
  (try
    (let [raw-params (merge (:params request) (:body-params request))
          params (walk/keywordize-keys raw-params)
          ;; Accept either a full spec or an intent to plan
          spec   (if (:bronze-nodes params)
                   params ;; already a PipelineSpec
                   (planner/plan-pipeline params))
          prev   (preview/generate-preview spec)]
      (ok {:spec    spec
           :preview prev
           :text    (preview/preview-text prev)}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline preview failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn apply-pipeline
  "POST /pipeline/apply
   Body: {spec: PipelineSpec, connection_id: int, created_by: string}
   Returns: {bronze: {...}, silver: [...], gold: [...]}
   Creates graph + proposal plans. Does NOT execute any runs."
  [request]
  (try
    (let [params        (merge (:params request) (:body-params request))
          spec          (or (:spec params) (get params "spec"))
          connection-id (some-> (:connection_id params) int)
          created-by    (or (:created_by params)
                            (get-in request [:session :user])
                            "system")
          _             (when-not spec
                          (throw (ex-info "spec is required" {:status 400 :error "bad_request"})))
          result        (compiler/apply-pipeline! spec
                                                  {:created-by created-by
                                                   :connection-id connection-id})]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline apply failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn list-connectors
  "GET /pipeline/connectors"
  [_request]
  (ok (planner/list-connectors)))

(defn get-connector
  "GET /pipeline/connectors/:system"
  [request]
  (let [system (get-in request [:path-params :system])]
    (if-let [ck (planner/load-connector-knowledge system)]
      (ok ck)
      (error-response 404 "not_found" (str "Connector not found: " system)))))

(defn list-metric-packages
  "GET /pipeline/metric-packages"
  [_request]
  (ok (planner/list-metric-packages)))

(defn generate-sdp
  "POST /pipeline/generate-sdp
   Body: PipelineSpec
   Returns: {silver: [...sql...], gold: [...sql...], combined: '...all sql...'}"
  [request]
  (try
    (let [spec (merge (:params request) (:body-params request))]
      (ok (sdp/generate-pipeline-sdp spec)))
    (catch Exception e
      (log/error e "SDP generation failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn edit-pipeline
  "POST /pipeline/edit
   Body: {spec: PipelineSpec, text: 'make fleet utilization weekly'}
   Returns: updated PipelineSpec + preview"
  [request]
  (try
    (let [params       (merge (:params request) (:body-params request))
          current-spec (or (:spec params) (get params "spec"))
          edit-text    (or (:text params) (get params "text"))
          _            (when (empty? edit-text)
                         (throw (ex-info "text is required" {:status 400 :error "bad_request"})))
          _            (when-not current-spec
                         (throw (ex-info "spec is required" {:status 400 :error "bad_request"})))
          updated-intent (try
                           (intent/edit-intent current-spec edit-text)
                           (catch Exception e
                             (log/warn e "LLM edit failed, returning original spec")
                             nil))
          spec         (if updated-intent
                         (planner/plan-pipeline updated-intent)
                         (walk/keywordize-keys current-spec))
          prev         (preview/generate-preview spec)]
      (ok {:intent  updated-intent
           :spec    spec
           :preview prev
           :text    (preview/preview-text prev)}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline edit failed")
      (error-response 500 "internal_error" (.getMessage e)))))

;; ---------------------------------------------------------------------------
;; Route table
;; ---------------------------------------------------------------------------

(defn pipeline-routes []
  ["/pipeline"
   ["/from-nl"                 {:post from-nl}]
   ["/edit"                    {:post edit-pipeline}]
   ["/preview"                 {:post preview-pipeline}]
   ["/apply"                   {:post apply-pipeline}]
   ["/connectors"              {:get list-connectors}]
   ["/connectors/:system"      {:get get-connector}]
   ["/metric-packages"         {:get list-metric-packages}]
   ["/generate-sdp"            {:post generate-sdp}]])
