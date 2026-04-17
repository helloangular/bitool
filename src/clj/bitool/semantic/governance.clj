(ns bitool.semantic.governance
  "Semantic Layer Phase 4 — Governance.

   Provides:
   1. Approval workflow   — draft → in_review → published lifecycle
   2. Model promotion     — copy a model across workspaces
   3. Row-level security  — dimension-based access filters enforced at ISL query time
   4. Impact analysis     — find saved reports that depend on a model's entities"
  (:require [bitool.db :as db]
            [bitool.semantic.model :as sem]
            [cheshire.core :as json]
            [clojure.set :as cset]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- db-opts [ds]
  (jdbc/with-options ds {:builder-fn rs/as-unqualified-lower-maps}))

(defn- parse-json-safe [v]
  (cond
    (nil? v)     nil
    (map? v)     v
    (vector? v)  v
    (string? v)  (try (json/parse-string v true) (catch Exception _ nil))
    :else        nil))

;; ---------------------------------------------------------------------------
;; DDL — Reviews & RLS
;; ---------------------------------------------------------------------------

(def ^:private governance-ready? (atom false))
(def ^:private review-table "semantic_model_review")
(def ^:private rls-table "semantic_rls_policy")

(defn ensure-governance-tables!
  []
  (when-not @governance-ready?
    (locking governance-ready?
      (when-not @governance-ready?
        (doseq [ddl
                [(str "CREATE TABLE IF NOT EXISTS " review-table " ("
                      "review_id BIGSERIAL PRIMARY KEY, "
                      "model_id BIGINT NOT NULL, "
                      "version INTEGER NOT NULL, "
                      "reviewer TEXT NOT NULL, "
                      "decision TEXT NOT NULL, "
                      "comment TEXT, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE INDEX IF NOT EXISTS idx_review_model "
                      "ON " review-table " (model_id, version)")
                 (str "CREATE TABLE IF NOT EXISTS " rls-table " ("
                      "policy_id BIGSERIAL PRIMARY KEY, "
                      "model_id BIGINT NOT NULL, "
                      "entity TEXT NOT NULL, "
                      "column_name TEXT NOT NULL, "
                      "allowed_values_json TEXT, "
                      "user_field TEXT NOT NULL, "
                      "created_by TEXT NOT NULL DEFAULT 'system', "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE INDEX IF NOT EXISTS idx_rls_model "
                      "ON " rls-table " (model_id)")
                 ;; Requested reviewer on semantic_model for pending-review queries
                 "ALTER TABLE semantic_model ADD COLUMN IF NOT EXISTS requested_reviewer TEXT NULL"
                 (str "CREATE INDEX IF NOT EXISTS idx_semantic_model_reviewer "
                      "ON semantic_model (requested_reviewer) WHERE status = 'in_review'")]]
          (jdbc/execute! db/ds [ddl]))
        (reset! governance-ready? true)))))

;; ---------------------------------------------------------------------------
;; 1. Approval Workflow
;; ---------------------------------------------------------------------------
;; Lifecycle: draft → in_review → published
;;
;; submit-for-review!  : draft → in_review
;; review-model!       : in_review → approved/rejected/needs_changes
;;                        approved → published
;;                        rejected/needs_changes → draft (for revision)
;; list-reviews        : audit trail

(defn submit-for-review!
  "Transition a model from draft to in_review. Requires draft status.
   Optionally assigns a requested_reviewer so the reviewer can query
   their pending queue via list-pending-reviews."
  [model-id submitted-by & {:keys [requested-reviewer]}]
  (ensure-governance-tables!)
  (sem/ensure-semantic-tables!)
  (let [current (sem/get-semantic-model model-id)
        by      (or submitted-by "system")]
    (when-not current
      (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
    (when-not (= "draft" (:status current))
      (throw (ex-info (str "Model must be in draft status to submit for review (current: " (:status current) ")")
                      {:model_id model-id :status 400 :current_status (:status current)})))
    ;; Validate model before allowing review submission
    (let [validation (sem/validate-semantic-model (:model current))]
      (when-not (:valid validation)
        (throw (ex-info "Model validation failed — fix errors before submitting for review"
                        {:status 422 :errors (:errors validation)}))))
    (let [result (jdbc/execute-one!
                  (db-opts db/ds)
                  [(str "UPDATE semantic_model"
                        " SET status = 'in_review', updated_at_utc = now()"
                        ", requested_reviewer = ?"
                        " WHERE model_id = ? RETURNING *")
                   requested-reviewer model-id])]
      ;; Record lifecycle transition
      (jdbc/execute-one!
       (db-opts db/ds)
       [(str "INSERT INTO semantic_model_version"
             " (model_id, version, model_json, change_summary, created_by)"
             " VALUES (?, ?, ?, ?, ?)")
        model-id (:version current) (:model_json current)
        (str "Submitted for review"
             (when requested-reviewer (str " (reviewer: " requested-reviewer ")")))
        by])
      result)))

(defn review-model!
  "Submit a review decision for a model version.
   decision: 'approved' | 'rejected' | 'needs_changes'
   On approved: status → published
   On rejected/needs_changes: status → draft (so author can revise)"
  [model-id {:keys [reviewer decision comment]}]
  (ensure-governance-tables!)
  (sem/ensure-semantic-tables!)
  (when-not (contains? #{"approved" "rejected" "needs_changes"} decision)
    (throw (ex-info "decision must be 'approved', 'rejected', or 'needs_changes'"
                    {:status 400 :error "bad_request"})))
  (when (string/blank? reviewer)
    (throw (ex-info "reviewer is required" {:status 400 :error "bad_request"})))
  (let [current (sem/get-semantic-model model-id)]
    (when-not current
      (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
    (when-not (= "in_review" (:status current))
      (throw (ex-info (str "Model must be in_review to accept reviews (current: " (:status current) ")")
                      {:model_id model-id :status 400 :current_status (:status current)})))
    ;; Record the review
    (let [review-row (jdbc/execute-one!
                      (db-opts db/ds)
                      [(str "INSERT INTO " review-table
                            " (model_id, version, reviewer, decision, comment)"
                            " VALUES (?, ?, ?, ?, ?) RETURNING *")
                       model-id (:version current) reviewer decision comment])
          ;; Transition status based on decision
          new-status (if (= "approved" decision) "published" "draft")
          summary    (case decision
                       "approved"      (str "Approved by " reviewer)
                       "rejected"      (str "Rejected by " reviewer ": " (or comment "no comment"))
                       "needs_changes" (str "Changes requested by " reviewer ": " (or comment "no comment")))]
      ;; Clear requested_reviewer and transition status
      (jdbc/execute-one!
       (db-opts db/ds)
       [(str "UPDATE semantic_model"
             " SET status = ?, requested_reviewer = NULL, updated_at_utc = now()"
             " WHERE model_id = ?")
        new-status model-id])
      ;; Record transition in version history
      (jdbc/execute-one!
       (db-opts db/ds)
       [(str "INSERT INTO semantic_model_version"
             " (model_id, version, model_json, change_summary, created_by)"
             " VALUES (?, ?, ?, ?, ?)")
        model-id (:version current) (:model_json current) summary reviewer])
      (assoc review-row :new_status new-status))))

(defn list-reviews
  "List reviews for a model, ordered by most recent first."
  [model-id]
  (ensure-governance-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "SELECT * FROM " review-table
         " WHERE model_id = ? ORDER BY created_at_utc DESC")
    model-id]))

(defn list-pending-reviews
  "List all models currently in_review, optionally filtered to a specific reviewer.
   Returns model metadata (not the full model_json) for each pending model."
  [& {:keys [reviewer]}]
  (ensure-governance-tables!)
  (sem/ensure-semantic-tables!)
  (let [sql    (str "SELECT model_id, conn_id, schema_name, name, version, status, "
                    "requested_reviewer, created_by, created_at_utc, updated_at_utc "
                    "FROM semantic_model WHERE status = 'in_review'"
                    (when reviewer " AND requested_reviewer = ?")
                    " ORDER BY updated_at_utc DESC")
        params (if reviewer [sql reviewer] [sql])]
    (jdbc/execute! (db-opts db/ds) params)))

;; ---------------------------------------------------------------------------
;; 2. Model Promotion (cross-workspace)
;; ---------------------------------------------------------------------------
;; Copies a published model from one connection/schema scope to another.
;; The promoted copy starts as draft in the target scope.

(defn promote-model!
  "Copy a published model to a target workspace/connection/schema scope.
   The promoted copy starts as draft. Returns the new model row.
   When target-workspace-key is provided, the copy is scoped to that workspace."
  [model-id {:keys [target-conn-id target-schema target-name target-workspace-key promoted-by]}]
  (sem/ensure-semantic-tables!)
  (let [source (sem/get-semantic-model model-id)]
    (when-not source
      (throw (ex-info "Source model not found" {:model_id model-id :status 404})))
    (when-not (= "published" (:status source))
      (throw (ex-info "Only published models can be promoted"
                      {:model_id model-id :status 400 :current_status (:status source)})))
    (let [target-conn  (or target-conn-id (:conn_id source))
          target-sch   (or target-schema (:schema_name source))
          target-nm    (or target-name (str (:name source) "_promoted"))
          target-ws    (or target-workspace-key (:workspace_key source))
          model-json   (:model source)
          by           (or promoted-by "system")
          ;; Persist as a new draft model in target scope
          new-model    (sem/persist-semantic-model!
                        (cond-> (assoc model-json
                                       :conn_id target-conn
                                       :schema  target-sch
                                       :name    target-nm)
                          target-ws (assoc :workspace_key target-ws))
                        by)]
      ;; Record promotion in source version history
      (jdbc/execute-one!
       (db-opts db/ds)
       [(str "INSERT INTO semantic_model_version"
             " (model_id, version, model_json, change_summary, created_by)"
             " VALUES (?, ?, ?, ?, ?)")
        model-id (:version source) (:model_json source)
        (str "Promoted to " target-sch "." target-nm
             (when target-ws (str " [workspace: " target-ws "]"))
             " (conn " target-conn ")")
        by])
      {:source_model_id      model-id
       :target_model_id      (:model_id new-model)
       :target_conn_id       target-conn
       :target_schema        target-sch
       :target_name          target-nm
       :target_workspace_key target-ws
       :status               "draft"})))

;; ---------------------------------------------------------------------------
;; 3. Row-Level Security (RLS)
;; ---------------------------------------------------------------------------
;; Policies attach to dimension entities. At query time, the ISL pipeline
;; checks the user's session field against the policy's allowed_values.
;; If the user's session value is in allowed_values, a WHERE filter is injected.

(defn add-rls-policy!
  "Attach an RLS policy to a model entity's dimension column.
   user_field: session key to match (e.g. 'workspace_key', 'region', 'role')
   allowed_values: JSON array of permitted values for that user_field, or nil = unrestricted."
  [{:keys [model-id entity column-name user-field allowed-values created-by]}]
  (ensure-governance-tables!)
  (sem/ensure-semantic-tables!)
  ;; Validate entity + column exist in model
  (let [model-row (sem/get-semantic-model model-id)]
    (when-not model-row
      (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
    (let [model-json (:model model-row)
          entities   (or (:entities model-json) {})
          entity-obj (or (get entities (keyword entity))
                         (get entities entity))]
      (when-not entity-obj
        (throw (ex-info (str "Entity '" entity "' not found in model")
                        {:status 400 :error "bad_request"})))
      (let [col-names (set (map :name (:columns entity-obj)))]
        (when-not (contains? col-names column-name)
          (throw (ex-info (str "Column '" column-name "' not found on entity '" entity "'")
                          {:status 400 :error "bad_request"}))))))
  (let [values-json (when (seq allowed-values)
                      (if (string? allowed-values) allowed-values
                          (json/generate-string allowed-values)))]
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "INSERT INTO " rls-table
           " (model_id, entity, column_name, allowed_values_json, user_field, created_by)"
           " VALUES (?, ?, ?, ?, ?, ?) RETURNING *")
      model-id entity column-name values-json user-field (or created-by "system")])))

(defn list-rls-policies
  "List all RLS policies for a model."
  [model-id]
  (ensure-governance-tables!)
  (->> (jdbc/execute!
        (db-opts db/ds)
        [(str "SELECT * FROM " rls-table " WHERE model_id = ? ORDER BY entity, column_name")
         model-id])
       (mapv (fn [row]
               (cond-> row
                 (:allowed_values_json row)
                 (assoc :allowed_values (parse-json-safe (:allowed_values_json row))))))))

(defn get-rls-policy
  "Fetch a single RLS policy by ID."
  [policy-id]
  (ensure-governance-tables!)
  (when-let [row (jdbc/execute-one!
                  (db-opts db/ds)
                  [(str "SELECT * FROM " rls-table " WHERE policy_id = ?") policy-id])]
    (cond-> row
      (:allowed_values_json row)
      (assoc :allowed_values (parse-json-safe (:allowed_values_json row))))))

(defn delete-rls-policy!
  "Delete an RLS policy by ID."
  [policy-id]
  (ensure-governance-tables!)
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "DELETE FROM " rls-table " WHERE policy_id = ? RETURNING policy_id")
    policy-id]))

(defn apply-rls-filters
  "Given a semantic model, its RLS policies, and a user session map,
   return a vector of ISL-format filter maps to inject before compilation.

   Session map example: {:region 'EMEA' :role 'analyst' :workspace_key 'ws-1'}

   For each policy, if the user's session field value is NOT in allowed_values,
   the query is blocked. If it IS in allowed_values, a filter is injected to
   restrict results to that dimension value."
  [model-json policies session]
  (let [entities  (or (:entities model-json) {})
        filters   (atom [])
        blocked   (atom [])]
    (doseq [{:keys [entity column_name user_field allowed_values]} policies]
      (let [entity-obj (or (get entities (keyword entity))
                           (get entities entity))
            table      (or (:table entity-obj) entity)
            user-val   (get session (keyword user_field)
                            (get session user_field))]
        (cond
          ;; No allowed_values = unrestricted (policy just documents the field)
          (nil? allowed_values) nil

          ;; User has no value for this session field — block
          (nil? user-val)
          (swap! blocked conj
                 {:entity entity :column column_name :user_field user_field
                  :reason "Session missing required field"})

          ;; User's value is in allowed list — inject filter
          (contains? (set (map str allowed_values)) (str user-val))
          (swap! filters conj
                 {:column (str table "." column_name)
                  :op     "="
                  :value  (str user-val)})

          ;; User's value is NOT in allowed list — block
          :else
          (swap! blocked conj
                 {:entity entity :column column_name :user_field user_field
                  :user_value user-val
                  :reason "User value not in allowed values"}))))
    (if (seq @blocked)
      {:blocked true :reasons @blocked}
      {:blocked false :filters @filters})))

;; ---------------------------------------------------------------------------
;; 4. Impact Analysis
;; ---------------------------------------------------------------------------
;; Three-tier detection:
;;   Tier 1 — FK match: saved_report.semantic_model_id = model_id (definitive)
;;   Tier 2 — ISL structural: parse ISL JSON, match :table and join :table fields
;;            against model entity physical tables + logical names
;;   Tier 3 — Measure match: ISL aggregate/column refs matching model measure names
;;
;; Reports matched by Tier 1 are tagged :match "explicit".
;; Reports matched by Tier 2/3 are tagged :match "inferred".

(defn- extract-isl-table-refs
  "Parse an ISL document and return a set of table names referenced.
   Checks :table (primary), :join entries' :table, and qualified column prefixes."
  [isl]
  (when (map? isl)
    (let [primary   (or (:table isl) (get isl "table"))
          joins     (or (:join isl) (get isl "join") [])
          join-tbls (keep #(or (:table %) (get % "table")) joins)
          ;; Extract entity prefixes from qualified column refs (e.g. "drivers.region")
          all-cols  (concat
                     (or (:columns isl) (get isl "columns") [])
                     (map #(or (:column %) (get % "column"))
                          (or (:aggregates isl) (get isl "aggregates") []))
                     (map #(or (:column %) (get % "column"))
                          (or (:filters isl) (get isl "filters") []))
                     (or (:group_by isl) (get isl "group_by") []))
          prefixes  (->> all-cols
                         (keep #(when (and (string? %) (string/includes? % "."))
                                  (first (string/split % #"\.")))))]
      (disj (set (concat (when primary [primary]) join-tbls prefixes)) nil))))

(defn- extract-isl-measure-refs
  "Extract measure names referenced in ISL aggregate :column fields."
  [isl]
  (when (map? isl)
    (let [aggs (or (:aggregates isl) (get isl "aggregates") [])]
      (->> aggs
           (keep #(or (:column %) (get % "column")))
           set))))

(defn model-impact-analysis
  "Find saved reports that depend on a semantic model.
   Uses three-tier matching: explicit FK, ISL structure, measure names.
   Returns {:model_tables [...] :impacted_reports [...]}."
  [model-id]
  (sem/ensure-semantic-tables!)
  (let [model-row (sem/get-semantic-model model-id)]
    (when-not model-row
      (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
    (let [model-json    (:model model-row)
          conn-id       (:conn_id model-row)
          ;; Build reference sets from model
          entity-tables (->> (or (:entities model-json) {})
                             vals
                             (map :table)
                             (remove nil?)
                             set)
          entity-names  (set (map name (keys (or (:entities model-json) {}))))
          all-table-refs (into entity-tables entity-names)
          measure-names (set (concat
                              (map :name (or (:calculated_measures model-json) []))
                              (map :name (or (:restricted_measures model-json) []))))
          ;; Fetch all saved reports for this connection
          reports       (db/list-saved-reports conn-id)
          impacted      (atom [])
          seen-ids      (atom #{})]
      ;; Tier 1: explicit FK match
      (doseq [report reports]
        (when (= (:semantic_model_id report) model-id)
          (swap! seen-ids conj (:report_id report))
          (swap! impacted conj
                 {:report_id   (:report_id report)
                  :report_name (:name report)
                  :description (:description report)
                  :created_by  (:created_by report)
                  :perspective (:perspective_name report)
                  :match       "explicit"})))
      ;; Tier 2+3: structural ISL matching for remaining reports
      (doseq [report reports]
        (when-not (contains? @seen-ids (:report_id report))
          (let [isl-parsed (try (parse-json-safe (:isl_json report))
                                (catch Exception _ nil))
                table-refs (when isl-parsed (extract-isl-table-refs isl-parsed))
                meas-refs  (when isl-parsed (extract-isl-measure-refs isl-parsed))
                ;; Tier 2: table name overlap
                table-hit  (when table-refs
                             (seq (cset/intersection table-refs all-table-refs)))
                ;; Tier 3: measure name overlap
                meas-hit   (when meas-refs
                             (seq (cset/intersection meas-refs measure-names)))]
            (when (or table-hit meas-hit)
              (swap! impacted conj
                     {:report_id       (:report_id report)
                      :report_name     (:name report)
                      :description     (:description report)
                      :created_by      (:created_by report)
                      :match           "inferred"
                      :matched_tables  (when table-hit (vec table-hit))
                      :matched_measures (when meas-hit (vec meas-hit))})))))
      {:model_id         model-id
       :model_name       (:name model-row)
       :model_tables     (vec entity-tables)
       :entity_names     (vec entity-names)
       :measure_names    (vec measure-names)
       :impacted_count   (count @impacted)
       :impacted_reports @impacted})))
