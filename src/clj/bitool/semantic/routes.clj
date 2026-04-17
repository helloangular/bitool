(ns bitool.semantic.routes
  "HTTP route handlers for the Semantic Layer API."
  (:require [bitool.semantic.model :as sem]
            [bitool.semantic.perspective :as persp]
            [bitool.semantic.nl-edit :as nl-edit]
            [bitool.semantic.governance :as gov]
            [bitool.config :refer [env]]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]))

;; ---------------------------------------------------------------------------
;; Response helpers (same pattern as ops/routes.clj)
;; ---------------------------------------------------------------------------

(defn- ok [data]
  (-> {:status 200
       :headers {"Content-Type" "application/json; charset=utf-8"}
       :body (json/generate-string {:ok true :data data})}))

(defn- error-response [status error msg]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/generate-string {:ok false :error error :message msg})})

(defn- parse-int [v]
  (when (some? v)
    (cond
      (integer? v) v
      :else (try (Long/parseLong (str v)) (catch Exception _ nil)))))

;; ---------------------------------------------------------------------------
;; Auth (same pattern as ops/routes.clj)
;; ---------------------------------------------------------------------------

(defn- rbac-enabled? []
  (let [raw (get env :bitool-rbac-enabled)]
    (if (nil? raw)
      false
      (contains? #{"1" "true" "yes" "on"}
                 (some-> raw str string/trim string/lower-case)))))

(defn- request-roles [request]
  (let [session-roles (or (get-in request [:session :roles])
                          (some-> (get-in request [:session :role]) vector))]
    (->> (if (sequential? session-roles) session-roles [session-roles])
         (map #(some-> % str string/trim string/lower-case))
         (remove string/blank?)
         set)))

(defn- ensure-authorized!
  "Checks RBAC when enabled. Semantic operations require :api.ops."
  [request required-role]
  (when (rbac-enabled?)
    (let [required-role (some-> required-role name string/lower-case)
          admin-role    (or (some-> (get env :bitool-admin-role) str string/trim string/lower-case)
                            "admin")
          roles         (request-roles request)]
      (when-not (or (contains? roles admin-role)
                    (contains? roles required-role))
        (throw (ex-info "Forbidden"
                        {:status 403
                         :error "forbidden"
                         :required_role required-role
                         :roles (vec roles)}))))))

;; ---------------------------------------------------------------------------
;; Route handlers
;; ---------------------------------------------------------------------------

(defn generate-semantic-model
  "POST /api/semantic/generate
   Body: {conn_id: int, schema: string, graph_ids: [int], created_by?: string}
   Auto-generates a Semantic Model from existing Silver/Gold proposals."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params     (merge (:params request) (:body-params request))
          params     (walk/keywordize-keys params)
          conn-id    (parse-int (:conn_id params))
          _          (when-not conn-id
                       (throw (ex-info "conn_id is required" {:status 400 :error "bad_request"})))
          graph-ids  (or (:graph_ids params) [])
          _          (when (empty? graph-ids)
                       (throw (ex-info "graph_ids is required (non-empty array)" {:status 400 :error "bad_request"})))
          schema     (or (:schema params) "public")
          created-by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")
          result     (sem/propose-semantic-model!
                      {:conn-id    conn-id
                       :schema     schema
                       :graph-ids  (mapv #(if (integer? %) % (Long/parseLong (str %))) graph-ids)
                       :created-by created-by})]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Semantic model generation failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn list-models
  "GET /api/semantic/models?conn_id=&schema=&status=
   Lists semantic models for a connection."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params  (merge (:params request) (:query-params request))
          params  (walk/keywordize-keys params)
          conn-id (parse-int (:conn_id params))
          _       (when-not conn-id
                    (throw (ex-info "conn_id query param is required" {:status 400 :error "bad_request"})))
          schema  (:schema params)
          status  (:status params)
          models  (sem/list-semantic-models conn-id :schema schema :status status)]
      (ok {:models models}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "List semantic models failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn get-model
  "GET /api/semantic/models/:model_id"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))]
      (if-let [model (sem/get-semantic-model model-id)]
        (ok model)
        (error-response 404 "not_found" (str "Semantic model not found: " model-id))))
    (catch Exception e
      (log/error e "Get semantic model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn update-model
  "PUT /api/semantic/models/:model_id
   Body: {model_json: {...}, change_summary?: string, updated_by?: string}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id   (parse-int (get-in request [:path-params :model_id]))
          params     (merge (:params request) (:body-params request))
          params     (walk/keywordize-keys params)
          model-json (:model_json params)
          _          (when-not model-json
                       (throw (ex-info "model_json is required" {:status 400 :error "bad_request"})))
          ;; Validate model invariants before persisting
          validation (sem/validate-semantic-model model-json)
          _          (when-not (:valid validation)
                       (throw (ex-info "Model validation failed"
                                       {:status 422
                                        :error "validation_failed"
                                        :details (:errors validation)})))
          summary    (:change_summary params)
          updated-by (or (:updated_by params)
                         (get-in request [:session :user])
                         "system")
          result     (sem/update-semantic-model! model-id model-json summary updated-by)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Update semantic model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn publish-model
  "POST /api/semantic/models/:model_id/publish
   Body: {published_by?: string}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id     (parse-int (get-in request [:path-params :model_id]))
          params       (merge (:params request) (:body-params request))
          params       (walk/keywordize-keys params)
          published-by (or (:published_by params)
                           (get-in request [:session :user])
                           "system")
          result       (sem/publish-semantic-model! model-id published-by)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Publish semantic model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn delete-model
  "DELETE /api/semantic/models/:model_id"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          result   (sem/delete-semantic-model! model-id)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Delete semantic model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn list-versions
  "GET /api/semantic/models/:model_id/versions"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          versions (sem/list-model-versions model-id)]
      (ok {:versions versions}))
    (catch Exception e
      (log/error e "List model versions failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn get-version
  "GET /api/semantic/models/:model_id/versions/:version"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          version  (parse-int (get-in request [:path-params :version]))]
      (if-let [v (sem/get-model-version model-id version)]
        (ok v)
        (error-response 404 "not_found"
                        (str "Version " version " not found for model " model-id))))
    (catch Exception e
      (log/error e "Get model version failed")
      (error-response 500 "internal_error" (.getMessage e)))))

;; ---------------------------------------------------------------------------
;; Phase 2 handlers — calculated measures, restricted measures, hierarchies
;; ---------------------------------------------------------------------------

(defn validate-model
  "POST /api/semantic/models/:model_id/validate
   Validates calculated measures, restricted measures, and hierarchies."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          row      (sem/get-semantic-model model-id)]
      (if-not row
        (error-response 404 "not_found" (str "Semantic model not found: " model-id))
        (ok (sem/validate-semantic-model (:model row)))))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Validate semantic model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn add-calculated-measure
  "POST /api/semantic/models/:model_id/measures/calculated
   Body: {name, entity, expression, aggregation?, description?}
   Appends a calculated measure, validates, then persists."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          row      (sem/get-semantic-model model-id)
          _        (when-not row
                     (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
          model    (:model row)
          measure  {:name        (:name params)
                    :entity      (:entity params)
                    :expression  (:expression params)
                    :aggregation (or (:aggregation params) "row")
                    :description (:description params)}
          _        (when (or (string/blank? (:name measure))
                             (string/blank? (:entity measure))
                             (string/blank? (:expression measure)))
                     (throw (ex-info "name, entity, and expression are required"
                                     {:status 400 :error "bad_request"})))
          updated  (update model :calculated_measures (fnil conj []) measure)
          ;; Validate before persisting
          vresult  (sem/validate-semantic-model updated)
          _        (when-not (:valid vresult)
                     (throw (ex-info (str "Validation failed: "
                                          (string/join "; " (:errors vresult)))
                                     {:status 422 :error "validation_error"
                                      :errors (:errors vresult)})))
          by       (or (:updated_by params)
                       (get-in request [:session :user])
                       "system")
          result   (sem/update-semantic-model!
                    model-id updated
                    (str "Added calculated measure: " (:name measure))
                    by)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Add calculated measure failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn add-restricted-measure
  "POST /api/semantic/models/:model_id/measures/restricted
   Body: {name, entity, base_measure, filter_column, filter_values, via_relationship?, description?}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          row      (sem/get-semantic-model model-id)
          _        (when-not row
                     (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
          model    (:model row)
          measure  {:name             (:name params)
                    :entity           (:entity params)
                    :base_measure     (:base_measure params)
                    :filter_column    (:filter_column params)
                    :filter_values    (or (:filter_values params) [])
                    :via_relationship (:via_relationship params)
                    :description      (:description params)}
          _        (when (or (string/blank? (:name measure))
                             (string/blank? (:base_measure measure))
                             (string/blank? (:filter_column measure)))
                     (throw (ex-info "name, base_measure, and filter_column are required"
                                     {:status 400 :error "bad_request"})))
          updated  (update model :restricted_measures (fnil conj []) measure)
          vresult  (sem/validate-semantic-model updated)
          _        (when-not (:valid vresult)
                     (throw (ex-info (str "Validation failed: "
                                          (string/join "; " (:errors vresult)))
                                     {:status 422 :error "validation_error"
                                      :errors (:errors vresult)})))
          by       (or (:updated_by params)
                       (get-in request [:session :user])
                       "system")
          result   (sem/update-semantic-model!
                    model-id updated
                    (str "Added restricted measure: " (:name measure))
                    by)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Add restricted measure failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn add-hierarchy
  "POST /api/semantic/models/:model_id/hierarchies
   Body: {name, entity, levels: [{column, label?}]}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          row      (sem/get-semantic-model model-id)
          _        (when-not row
                     (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
          model    (:model row)
          hier     {:name   (:name params)
                    :entity (:entity params)
                    :levels (or (:levels params) [])}
          _        (when (or (string/blank? (:name hier))
                             (string/blank? (:entity hier))
                             (< (count (:levels hier)) 2))
                     (throw (ex-info "name, entity, and at least 2 levels are required"
                                     {:status 400 :error "bad_request"})))
          updated  (update model :hierarchies (fnil conj []) hier)
          vresult  (sem/validate-semantic-model updated)
          _        (when-not (:valid vresult)
                     (throw (ex-info (str "Validation failed: "
                                          (string/join "; " (:errors vresult)))
                                     {:status 422 :error "validation_error"
                                      :errors (:errors vresult)})))
          by       (or (:updated_by params)
                       (get-in request [:session :user])
                       "system")
          result   (sem/update-semantic-model!
                    model-id updated
                    (str "Added hierarchy: " (:name hier))
                    by)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Add hierarchy failed")
      (error-response 500 "internal_error" (.getMessage e)))))

;; ---------------------------------------------------------------------------
;; Phase 3 handlers — Perspectives
;; ---------------------------------------------------------------------------

(defn create-perspective
  "POST /api/semantic/models/:model_id/perspectives
   Body: {name, description?, audience?, spec: {entities?, columns?, measures?, hierarchies?}}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          row      (sem/get-semantic-model model-id)
          _        (when-not row
                     (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
          spec     (:spec params)
          _        (when-not spec
                     (throw (ex-info "spec is required" {:status 400 :error "bad_request"})))
          _        (when (string/blank? (:name params))
                     (throw (ex-info "name is required" {:status 400 :error "bad_request"})))
          ;; Validate spec against model
          vresult  (persp/validate-perspective-spec spec (:model row))
          _        (when-not (:valid vresult)
                     (throw (ex-info (str "Perspective validation failed: "
                                          (string/join "; " (:errors vresult)))
                                     {:status 422 :error "validation_error"
                                      :errors (:errors vresult)})))
          by       (or (:created_by params)
                       (get-in request [:session :user])
                       "system")
          result   (persp/create-perspective!
                    {:model-id    model-id
                     :name        (:name params)
                     :description (:description params)
                     :audience    (:audience params)
                     :spec        spec
                     :created-by  by})]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Create perspective failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn list-perspectives
  "GET /api/semantic/models/:model_id/perspectives"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id     (parse-int (get-in request [:path-params :model_id]))
          perspectives (persp/list-perspectives model-id)]
      (ok {:perspectives perspectives}))
    (catch Exception e
      (log/error e "List perspectives failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn get-perspective
  "GET /api/semantic/models/:model_id/perspectives/:perspective_id"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id       (parse-int (get-in request [:path-params :model_id]))
          perspective-id (parse-int (get-in request [:path-params :perspective_id]))]
      (if-let [p (persp/get-perspective perspective-id)]
        (if (= (:model_id p) model-id)
          (ok p)
          (error-response 404 "not_found" (str "Perspective not found: " perspective-id)))
        (error-response 404 "not_found" (str "Perspective not found: " perspective-id))))
    (catch Exception e
      (log/error e "Get perspective failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn delete-perspective
  "DELETE /api/semantic/models/:model_id/perspectives/:perspective_id"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id       (parse-int (get-in request [:path-params :model_id]))
          perspective-id (parse-int (get-in request [:path-params :perspective_id]))]
      (if-let [p (persp/get-perspective perspective-id)]
        (if (= (:model_id p) model-id)
          (let [result (persp/delete-perspective! perspective-id)]
            (ok result))
          (error-response 404 "not_found" (str "Perspective not found: " perspective-id)))
        (error-response 404 "not_found" (str "Perspective not found: " perspective-id))))
    (catch Exception e
      (log/error e "Delete perspective failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn preview-perspective
  "POST /api/semantic/models/:model_id/perspectives/preview
   Body: {spec: {...}}
   Returns the model filtered through the perspective spec without persisting."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          row      (sem/get-semantic-model model-id)
          _        (when-not row
                     (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
          spec     (:spec params)
          vresult  (persp/validate-perspective-spec (or spec {}) (:model row))
          _        (when-not (:valid vresult)
                     (throw (ex-info (str "Spec validation failed: "
                                          (string/join "; " (:errors vresult)))
                                     {:status 422 :error "validation_error"})))
          filtered (persp/apply-perspective (:model row) spec)]
      (ok {:model filtered}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Preview perspective failed")
      (error-response 500 "internal_error" (.getMessage e)))))

;; ---------------------------------------------------------------------------
;; Phase 3 handlers — NL model editing
;; ---------------------------------------------------------------------------

(defn nl-edit-model
  "POST /api/semantic/models/:model_id/edit
   Body: {command: 'add measure cost_per_mile as fuel_cost / miles on trips'}
   Parses the NL command, applies it, validates, and persists if valid."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          command  (:command params)
          _        (when (string/blank? command)
                     (throw (ex-info "command is required" {:status 400 :error "bad_request"})))
          row      (sem/get-semantic-model model-id)
          _        (when-not row
                     (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
          model    (:model row)
          result   (nl-edit/execute-and-validate model command)]
      (if (:error result)
        (error-response 400 "parse_error" (:error result))
        (if-not (get-in result [:validation :valid])
          (error-response 422 "validation_error"
                          (str "Command parsed but validation failed: "
                               (string/join "; " (get-in result [:validation :errors]))))
          ;; Valid — persist
          (let [by      (or (:updated_by params)
                            (get-in request [:session :user])
                            "system")
                updated (sem/update-semantic-model!
                         model-id (:model result)
                         (str "NL edit: " (:summary result))
                         by)]
            (ok {:result  updated
                 :summary (:summary result)
                 :action  (:action result)})))))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "NL edit model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn nl-edit-preview
  "POST /api/semantic/models/:model_id/edit/preview
   Body: {command: '...'}
   Parses and applies the command but does NOT persist. Returns the diff."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          command  (:command params)
          _        (when (string/blank? command)
                     (throw (ex-info "command is required" {:status 400 :error "bad_request"})))
          row      (sem/get-semantic-model model-id)
          _        (when-not row
                     (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
          result   (nl-edit/execute-and-validate (:model row) command)]
      (if (:error result)
        (error-response 400 "parse_error" (:error result))
        (ok {:action     (:action result)
             :summary    (:summary result)
             :model      (:model result)
             :validation (:validation result)
             :persisted  false})))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "NL edit preview failed")
      (error-response 500 "internal_error" (.getMessage e)))))

;; ---------------------------------------------------------------------------
;; Phase 4 handlers — Governance
;; ---------------------------------------------------------------------------

(defn submit-for-review
  "POST /api/semantic/models/:model_id/review/submit
   Body: {submitted_by?: string, requested_reviewer?: string}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          by       (or (:submitted_by params)
                       (get-in request [:session :user])
                       "system")
          reviewer (some-> (:requested_reviewer params) str string/trim not-empty)
          result   (gov/submit-for-review! model-id by
                     :requested-reviewer reviewer)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Submit for review failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn review-model
  "POST /api/semantic/models/:model_id/review
   Body: {reviewer, decision: 'approved'|'rejected'|'needs_changes', comment?}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          reviewer (or (:reviewer params)
                       (get-in request [:session :user]))
          result   (gov/review-model! model-id
                     {:reviewer reviewer
                      :decision (:decision params)
                      :comment  (:comment params)})]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Review model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn list-reviews
  "GET /api/semantic/models/:model_id/reviews"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          reviews  (gov/list-reviews model-id)]
      (ok {:reviews reviews}))
    (catch Exception e
      (log/error e "List reviews failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn pending-reviews
  "GET /api/semantic/reviews/pending?reviewer=
   List all models currently in_review, optionally filtered to a specific reviewer."
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params   (merge (:params request) (:query-params request))
          params   (walk/keywordize-keys params)
          reviewer (some-> (:reviewer params) str string/trim not-empty)
          models   (gov/list-pending-reviews :reviewer reviewer)]
      (ok {:pending models}))
    (catch Exception e
      (log/error e "List pending reviews failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn promote-model
  "POST /api/semantic/models/:model_id/promote
   Body: {target_conn_id?, target_schema?, target_name?, target_workspace_key?, promoted_by?}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          by       (or (:promoted_by params)
                       (get-in request [:session :user])
                       "system")
          result   (gov/promote-model! model-id
                     {:target-conn-id       (parse-int (:target_conn_id params))
                      :target-schema        (:target_schema params)
                      :target-name          (:target_name params)
                      :target-workspace-key (:target_workspace_key params)
                      :promoted-by          by})]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Promote model failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn add-rls-policy
  "POST /api/semantic/models/:model_id/rls
   Body: {entity, column_name, user_field, allowed_values?: [...], created_by?}"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          params   (walk/keywordize-keys (merge (:params request) (:body-params request)))
          _        (when (or (string/blank? (:entity params))
                             (string/blank? (:column_name params))
                             (string/blank? (:user_field params)))
                     (throw (ex-info "entity, column_name, and user_field are required"
                                     {:status 400 :error "bad_request"})))
          by       (or (:created_by params)
                       (get-in request [:session :user])
                       "system")
          result   (gov/add-rls-policy!
                    {:model-id       model-id
                     :entity         (:entity params)
                     :column-name    (:column_name params)
                     :user-field     (:user_field params)
                     :allowed-values (:allowed_values params)
                     :created-by     by})]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Add RLS policy failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn list-rls-policies
  "GET /api/semantic/models/:model_id/rls"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          policies (gov/list-rls-policies model-id)]
      (ok {:policies policies}))
    (catch Exception e
      (log/error e "List RLS policies failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn delete-rls-policy
  "DELETE /api/semantic/models/:model_id/rls/:policy_id"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id  (parse-int (get-in request [:path-params :model_id]))
          policy-id (parse-int (get-in request [:path-params :policy_id]))]
      ;; Enforce model_id ownership
      (if-let [p (gov/get-rls-policy policy-id)]
        (if (= (:model_id p) model-id)
          (let [result (gov/delete-rls-policy! policy-id)]
            (ok result))
          (error-response 404 "not_found" (str "RLS policy not found: " policy-id)))
        (error-response 404 "not_found" (str "RLS policy not found: " policy-id))))
    (catch Exception e
      (log/error e "Delete RLS policy failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn model-impact
  "GET /api/semantic/models/:model_id/impact"
  [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [model-id (parse-int (get-in request [:path-params :model_id]))
          result   (gov/model-impact-analysis model-id)]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Impact analysis failed")
      (error-response 500 "internal_error" (.getMessage e)))))

;; ---------------------------------------------------------------------------
;; Route table
;; ---------------------------------------------------------------------------

(defn semantic-routes []
  ["/api/semantic"
   ["/generate"                                  {:post generate-semantic-model}]
   ["/models"                                    {:get list-models}]
   ["/models/:model_id"                          {:get get-model
                                                   :put update-model
                                                   :delete delete-model}]
   ["/models/:model_id/publish"                  {:post publish-model}]
   ["/models/:model_id/validate"                 {:post validate-model}]
   ["/models/:model_id/measures/calculated"      {:post add-calculated-measure}]
   ["/models/:model_id/measures/restricted"      {:post add-restricted-measure}]
   ["/models/:model_id/hierarchies"              {:post add-hierarchy}]
   ["/models/:model_id/versions"                 {:get list-versions}]
   ["/models/:model_id/versions/:version"        {:get get-version}]
   ;; Phase 3 — Perspectives
   ["/models/:model_id/perspectives"             {:get  list-perspectives
                                                   :post create-perspective}]
   ["/models/:model_id/perspectives/preview"     {:post preview-perspective}]
   ["/models/:model_id/perspectives/:perspective_id" {:get    get-perspective
                                                       :delete delete-perspective}]
   ;; Phase 3 — NL model editing
   ["/models/:model_id/edit"                     {:post nl-edit-model}]
   ["/models/:model_id/edit/preview"             {:post nl-edit-preview}]
   ;; Phase 4 — Governance
   ["/reviews/pending"                           {:get  pending-reviews}]
   ["/models/:model_id/review/submit"            {:post submit-for-review}]
   ["/models/:model_id/review"                   {:post review-model}]
   ["/models/:model_id/reviews"                  {:get  list-reviews}]
   ["/models/:model_id/promote"                  {:post promote-model}]
   ["/models/:model_id/rls"                      {:get  list-rls-policies
                                                   :post add-rls-policy}]
   ["/models/:model_id/rls/:policy_id"           {:delete delete-rls-policy}]
   ["/models/:model_id/impact"                   {:get  model-impact}]])
