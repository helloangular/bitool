(ns bitool.semantic.model
  "Semantic Layer — DDL, CRUD, auto-generation, and validation."
  (:require [bitool.db :as db]
            [bitool.modeling.automation :as modeling]
            [bitool.semantic.expression :as expr]
            [bitool.semantic.association :as assoc-resolver]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- db-opts [ds]
  (jdbc/with-options ds {:builder-fn rs/as-unqualified-lower-maps}))

(defn- non-blank-str [v]
  (let [s (some-> v str string/trim)]
    (when (seq s) s)))

(defn- parse-json-safe [v]
  (cond
    (nil? v) nil
    (map? v) v
    (vector? v) v
    (string? v) (try (json/parse-string v true) (catch Exception _ nil))
    :else nil))

;; ---------------------------------------------------------------------------
;; DDL
;; ---------------------------------------------------------------------------

(def ^:private semantic-ready? (atom false))
(def ^:private model-table "semantic_model")
(def ^:private version-table "semantic_model_version")

(defn ensure-semantic-tables!
  []
  (when-not @semantic-ready?
    (locking semantic-ready?
      (when-not @semantic-ready?
        (doseq [ddl
                [(str "CREATE TABLE IF NOT EXISTS " model-table " ("
                      "model_id BIGSERIAL PRIMARY KEY, "
                      "conn_id INTEGER NOT NULL REFERENCES connection(id), "
                      "schema_name TEXT NOT NULL DEFAULT 'public', "
                      "name TEXT NOT NULL, "
                      "version INTEGER NOT NULL DEFAULT 1, "
                      "status TEXT NOT NULL DEFAULT 'draft', "
                      "model_json TEXT NOT NULL, "
                      "created_by TEXT NOT NULL DEFAULT 'system', "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_model_uniq "
                      "ON " model-table " (conn_id, schema_name, name)")
                 (str "CREATE TABLE IF NOT EXISTS " version-table " ("
                      "version_id BIGSERIAL PRIMARY KEY, "
                      "model_id BIGINT NOT NULL REFERENCES " model-table "(model_id), "
                      "version INTEGER NOT NULL, "
                      "model_json TEXT NOT NULL, "
                      "change_summary TEXT, "
                      "created_by TEXT NOT NULL DEFAULT 'system', "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_version_uniq "
                      "ON " version-table " (model_id, version)")
                 ;; Phase 4: workspace scope for promotion
                 (str "ALTER TABLE " model-table
                      " ADD COLUMN IF NOT EXISTS workspace_key VARCHAR(128) NULL")
                 (str "CREATE INDEX IF NOT EXISTS idx_semantic_model_workspace "
                      "ON " model-table " (workspace_key)")]]
          (jdbc/execute! db/ds [ddl]))
        (reset! semantic-ready? true)))))

;; ---------------------------------------------------------------------------
;; CRUD
;; ---------------------------------------------------------------------------

(defn persist-semantic-model!
  "Insert a new semantic model. Returns the created row.
   Also records v1 in version history so the initial snapshot is recoverable."
  [model created-by]
  (ensure-semantic-tables!)
  (let [json-text     (json/generate-string (dissoc model :conn_id :schema :name :workspace_key))
        by            (or created-by "system")
        workspace-key (:workspace_key model)
        row           (jdbc/execute-one!
                       (db-opts db/ds)
                       [(str "INSERT INTO " model-table
                             " (conn_id, schema_name, name, version, status, model_json, created_by, workspace_key)"
                             " VALUES (?, ?, ?, 1, 'draft', ?, ?, ?)"
                             " RETURNING *")
                        (:conn_id model)
                        (or (:schema model) "public")
                        (:name model)
                        json-text
                        by
                        workspace-key])]
    ;; Record v1 in version history
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "INSERT INTO " version-table
           " (model_id, version, model_json, change_summary, created_by)"
           " VALUES (?, 1, ?, 'Initial auto-generation', ?)")
      (:model_id row) json-text by])
    row))

(defn get-semantic-model
  "Fetch a semantic model by id, parsing model_json."
  [model-id]
  (ensure-semantic-tables!)
  (when-let [row (jdbc/execute-one!
                  (db-opts db/ds)
                  [(str "SELECT * FROM " model-table " WHERE model_id = ?") model-id])]
    (assoc row :model (parse-json-safe (:model_json row)))))

(defn list-semantic-models
  "List semantic models for a connection, optionally filtered by status."
  [conn-id & {:keys [schema status]}]
  (ensure-semantic-tables!)
  (let [params (cond-> [conn-id]
                 schema (conj schema)
                 status (conj status))
        sql    (str "SELECT model_id, conn_id, schema_name, name, version, status, "
                    "model_json, created_by, created_at_utc, updated_at_utc "
                    "FROM " model-table
                    " WHERE conn_id = ?"
                    (when schema " AND schema_name = ?")
                    (when status " AND status = ?")
                    " ORDER BY updated_at_utc DESC")
        rows   (jdbc/execute! (db-opts db/ds) (into [sql] params))]
    (mapv (fn [row]
            (let [parsed (parse-json-safe (:model_json row))]
              (-> (dissoc row :model_json)
                  (assoc :confidence_score (:confidence parsed)))))
          rows)))

(defn update-semantic-model!
  "Update model_json, bump version, record version history."
  [model-id model-json change-summary updated-by]
  (ensure-semantic-tables!)
  (let [current   (get-semantic-model model-id)
        _         (when-not current
                    (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
        new-ver   (inc (long (:version current)))
        json-text (if (string? model-json) model-json (json/generate-string model-json))]
    ;; Insert version history
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "INSERT INTO " version-table
           " (model_id, version, model_json, change_summary, created_by)"
           " VALUES (?, ?, ?, ?, ?)")
      model-id new-ver json-text change-summary (or updated-by "system")])
    ;; Update main row
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "UPDATE " model-table
           " SET model_json = ?, version = ?, updated_at_utc = now()"
           " WHERE model_id = ?"
           " RETURNING *")
      json-text new-ver model-id])))

(defn publish-semantic-model!
  "Transition a model from draft to published. Records transition in version history."
  [model-id published-by]
  (ensure-semantic-tables!)
  (let [current (get-semantic-model model-id)
        by      (or published-by "system")]
    (when-not current
      (throw (ex-info "Semantic model not found" {:model_id model-id :status 404})))
    (when (= "published" (:status current))
      (throw (ex-info "Model is already published" {:model_id model-id :status 400})))
    (let [result (jdbc/execute-one!
                  (db-opts db/ds)
                  [(str "UPDATE " model-table
                        " SET status = 'published', updated_at_utc = now()"
                        " WHERE model_id = ?"
                        " RETURNING *")
                   model-id])]
      ;; Record lifecycle transition in version history
      (jdbc/execute-one!
       (db-opts db/ds)
       [(str "INSERT INTO " version-table
             " (model_id, version, model_json, change_summary, created_by)"
             " VALUES (?, ?, ?, 'Published', ?)")
        model-id (:version current) (:model_json current) by])
      result)))

(defn delete-semantic-model!
  "Archive (soft-delete) a semantic model. Records transition in version history."
  [model-id]
  (ensure-semantic-tables!)
  (let [current (get-semantic-model model-id)
        result  (jdbc/execute-one!
                 (db-opts db/ds)
                 [(str "UPDATE " model-table
                       " SET status = 'archived', updated_at_utc = now()"
                       " WHERE model_id = ?"
                       " RETURNING model_id, status")
                  model-id])]
    ;; Record lifecycle transition in version history
    (when current
      (jdbc/execute-one!
       (db-opts db/ds)
       [(str "INSERT INTO " version-table
             " (model_id, version, model_json, change_summary, created_by)"
             " VALUES (?, ?, ?, 'Archived', 'system')")
        model-id (:version current) (or (:model_json current) "{}")]))
    result))

(defn list-model-versions
  "Return version history for a model."
  [model-id]
  (ensure-semantic-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "SELECT version_id, model_id, version, change_summary, created_by, created_at_utc"
         " FROM " version-table
         " WHERE model_id = ?"
         " ORDER BY version DESC")
    model-id]))

(defn get-model-version
  "Return a specific version snapshot."
  [model-id version]
  (ensure-semantic-tables!)
  (when-let [row (jdbc/execute-one!
                  (db-opts db/ds)
                  [(str "SELECT * FROM " version-table
                        " WHERE model_id = ? AND version = ?")
                   model-id version])]
    (assoc row :model (parse-json-safe (:model_json row)))))

;; ---------------------------------------------------------------------------
;; Assembly — build semantic model from existing proposals + context + joins
;; ---------------------------------------------------------------------------

(defn- extract-columns-from-proposal
  "Extract column list with roles from a proposal's proposal_json."
  [proposal-json]
  (mapv (fn [col]
          (cond-> {:name (:target_column col)
                   :type (or (:type col) "STRING")
                   :role (or (:role col) "attribute")
                   :nullable (not= false (:nullable col))}
            (:confidence col) (assoc :confidence (:confidence col))
            (:source_columns col) (assoc :source_columns (:source_columns col))
            (:source_paths col) (assoc :source_paths (:source_paths col))
            (:expression col) (assoc :expression (:expression col))))
        (or (:columns proposal-json) [])))

(defn- build-entity-from-silver
  "Build a semantic entity from a Silver proposal."
  [proposal]
  (let [pjson   (parse-json-safe (:proposal_json proposal))
        columns (extract-columns-from-proposal pjson)
        grain   (:grain pjson)]
    {:kind        (or (:entity_kind pjson) "entity")
     :table       (or (:target_table pjson) (:target_model proposal))
     :grain       grain
     :columns     columns
     :confidence  (or (:confidence_score proposal) 0.8)
     :source      {:layer "silver"
                   :proposal_id (:proposal_id proposal)
                   :graph_id (:source_graph_id proposal)
                   :node_id (:source_node_id proposal)}}))

(defn- merge-gold-into-entity
  "Enrich a Silver-derived entity with Gold proposal measures and time dimensions."
  [entity gold-proposal]
  (let [gpjson     (parse-json-safe (:proposal_json gold-proposal))
        gold-cols  (extract-columns-from-proposal gpjson)
        measure-cols (filterv #(= "measure" (:role %)) gold-cols)
        time-cols    (filterv #(= "time_dimension" (:role %)) gold-cols)
        group-by     (:group_by gpjson)]
    (cond-> entity
      (seq measure-cols) (update :columns into measure-cols)
      (seq time-cols) (update :columns into time-cols)
      group-by (assoc :grain (let [g (:grain entity)]
                               (cond-> (if (map? g) g {:value g})
                                 true (assoc :group_by group-by))))
      true (assoc :gold_source {:proposal_id (:proposal_id gold-proposal)
                                :target_table (:target_table gpjson)}))))

(defn- enrich-entity-with-context
  "Merge schema_context descriptions into entity columns."
  [entity context-entries]
  (let [table-name   (some-> (:table entity) (string/split #"\.") last)
        table-ctx    (first (filter #(and (= table-name (:table_name %))
                                          (nil? (:column_name %)))
                                    context-entries))
        col-ctx-map  (into {}
                           (keep (fn [ctx]
                                   (when (and (= table-name (:table_name ctx))
                                              (:column_name ctx))
                                     [(:column_name ctx)
                                      {:description (:description ctx)
                                       :sample_values (parse-json-safe (:sample_values_json ctx))}])))
                           context-entries)
        enriched-cols (mapv (fn [col]
                              (if-let [ctx (get col-ctx-map (:name col))]
                                (cond-> col
                                  (:description ctx) (assoc :description (:description ctx))
                                  (:sample_values ctx) (assoc :sample_values (:sample_values ctx)))
                                col))
                            (:columns entity))]
    (cond-> (assoc entity :columns enriched-cols)
      (:description table-ctx) (assoc :description (:description table-ctx)))))

(defn- build-entities-from-proposals
  "Build the entities map from Silver + Gold proposals, enriched with schema context."
  [silver-proposals gold-proposals context-entries]
  (let [silver-entities (into {}
                              (map (fn [sp]
                                     [(:target_model sp) (build-entity-from-silver sp)]))
                              silver-proposals)
        ;; Match Gold proposals to their Silver counterparts by target_model prefix
        with-gold (reduce (fn [entities gp]
                            (let [gpjson (parse-json-safe (:proposal_json gp))
                                  ;; Gold models are typically "gold_<silver_model>"
                                  silver-key (some (fn [k]
                                                     (when (string/includes?
                                                            (or (:target_model gp) "")
                                                            (string/replace (str k) #"^silver_" ""))
                                                       k))
                                                   (keys entities))]
                              (if silver-key
                                (update entities silver-key merge-gold-into-entity gp)
                                ;; No matching Silver — create standalone Gold entity
                                (assoc entities (:target_model gp)
                                       {:kind "mart"
                                        :table (or (:target_table gpjson) (:target_model gp))
                                        :columns (extract-columns-from-proposal gpjson)
                                        :grain (:grain gpjson)
                                        :confidence (or (:confidence_score gp) 0.8)
                                        :gold_source {:proposal_id (:proposal_id gp)}}))))
                          silver-entities
                          gold-proposals)]
    ;; Enrich with context
    (into {}
          (map (fn [[k entity]]
                 [k (enrich-entity-with-context entity context-entries)]))
          with-gold)))

(defn- build-relationships-from-joins
  "Map discover-joins output to relationship objects.
   Returns [] gracefully when FK introspection is unavailable (Snowflake/Databricks/BigQuery)."
  [joins entities]
  (let [entity-tables (into {}
                            (map (fn [[k entity]]
                                   [(some-> (:table entity) (string/split #"\.") last) k]))
                            entities)]
    (->> (or joins [])
         (keep (fn [{:keys [from_table from_column to_table to_column]}]
                 (let [from-entity (get entity-tables from_table)
                       to-entity   (get entity-tables to_table)]
                   (when (and from-entity to-entity (not= from-entity to-entity))
                     {:from from-entity
                      :from_column from_column
                      :to to-entity
                      :to_column to_column
                      :type "many_to_one"
                      :join "LEFT"}))))
         vec)))

(defn- avg-confidence
  "Average confidence score across proposals."
  [proposals]
  (let [scores (->> proposals
                    (keep :confidence_score)
                    (map double))]
    (if (seq scores)
      (/ (reduce + 0.0 scores) (count scores))
      0.8)))

(defn- build-lineage
  "Extract lineage summary from proposals."
  [silver-proposals gold-proposals]
  (let [silver-tables (mapv (fn [sp]
                              (or (get-in (parse-json-safe (:proposal_json sp)) [:target_table])
                                  (:target_model sp)))
                            silver-proposals)
        gold-tables   (mapv (fn [gp]
                              (or (get-in (parse-json-safe (:proposal_json gp)) [:target_table])
                                  (:target_model gp)))
                            gold-proposals)]
    (cond-> {}
      (seq silver-tables) (assoc :silver silver-tables)
      (seq gold-tables) (assoc :gold gold-tables))))

(defn propose-semantic-model!
  "Auto-generate a Semantic Model from existing Silver/Gold proposals,
   schema context, and discovered joins for a given connection + schema.

   Accepts :graph-ids to scope which proposals to include. If omitted,
   fetches all proposals for the connection (via all graph-ids).

   On warehouses without FK introspection (Snowflake, Databricks, BigQuery),
   relationships will be empty — users add them manually."
  [{:keys [conn-id schema graph-ids created-by]
    :or {schema "public" created-by "system"}}]
  (let [;; Fetch proposals — iterate graph-ids
        silver-proposals (vec (mapcat (fn [gid]
                                        (modeling/list-silver-proposals {:graph-id gid}))
                                      graph-ids))
        gold-proposals   (vec (mapcat (fn [gid]
                                        (modeling/list-gold-proposals {:graph-id gid}))
                                       graph-ids))
        _                (when (and (empty? silver-proposals) (empty? gold-proposals))
                           (throw (ex-info "No Silver or Gold proposals found for the given graphs"
                                           {:conn_id conn-id
                                            :graph_ids graph-ids
                                            :status 404})))
        ;; Filter proposals whose three-part target_table (db.schema.table) has a schema
        ;; segment that contradicts the requested schema. Two-part names (layer.table)
        ;; are always allowed — the first segment is the output layer, not the source schema.
        schema-match?    (fn [proposal]
                           (let [pjson  (parse-json-safe (:proposal_json proposal))
                                 tbl    (or (:target_table pjson) "")
                                 parts  (string/split tbl #"\." -1)]
                             (or (<= (count parts) 2)
                                 ;; Three-part: db.schema.table — check schema segment
                                 (= schema (nth parts 1)))))
        silver-proposals (let [filtered (filterv schema-match? silver-proposals)
                               dropped  (- (count silver-proposals) (count filtered))]
                           (when (pos? dropped)
                             (log/warn "Dropped Silver proposals with mismatched schema"
                                       {:schema schema :dropped dropped}))
                           filtered)
        gold-proposals   (let [filtered (filterv schema-match? gold-proposals)
                               dropped  (- (count gold-proposals) (count filtered))]
                           (when (pos? dropped)
                             (log/warn "Dropped Gold proposals with mismatched schema"
                                       {:schema schema :dropped dropped}))
                           filtered)
        _                (when (and (empty? silver-proposals) (empty? gold-proposals))
                           (throw (ex-info "No proposals match the requested schema after filtering"
                                           {:conn_id conn-id :schema schema
                                            :graph_ids graph-ids :status 404})))
        ;; Fetch context + joins
        context-entries  (db/get-schema-context conn-id :schema schema)
        joins            (try
                           (db/discover-joins conn-id :schema schema)
                           (catch Exception e
                             (log/warn "discover-joins failed, proceeding without relationships"
                                       {:conn_id conn-id :error (.getMessage e)})
                             []))
        ;; Assemble
        entities         (build-entities-from-proposals silver-proposals gold-proposals context-entries)
        relationships    (build-relationships-from-joins joins entities)
        all-proposals    (concat silver-proposals gold-proposals)
        confidence       (avg-confidence all-proposals)
        lineage          (build-lineage silver-proposals gold-proposals)
        model-name       (str schema "_model")
        model            {:conn_id conn-id
                          :schema schema
                          :name model-name
                          :entities entities
                          :relationships relationships
                          :calculated_measures []
                          :restricted_measures []
                          :hierarchies []
                          :confidence confidence
                          :source "auto"
                          :lineage lineage}]
    (let [row (persist-semantic-model! model created-by)]
      (assoc row :model (parse-json-safe (:model_json row))))))

;; ---------------------------------------------------------------------------
;; Validation — Phase 2: calculated measures, restricted measures, hierarchies
;; ---------------------------------------------------------------------------

(defn validate-semantic-model
  "Validate a complete semantic model. Checks:
   1. Each entity's calculated measures (expression syntax, column refs, cycles)
   2. Each entity's restricted measures (base measure, filter column, relationships)
   3. Each entity's hierarchies (levels, column refs)

   Returns {:valid true :entity-sort-orders {entity-name [sorted-measures]}}
   or {:valid false :errors [...]}."
  [model-json]
  (let [errors      (atom [])
        entities    (or (:entities model-json) {})
        calc-m      (or (:calculated_measures model-json) [])
        restr-m     (or (:restricted_measures model-json) [])
        hierarchies (or (:hierarchies model-json) [])
        rels        (or (:relationships model-json) [])
        sort-orders (atom {})]

    ;; Group calculated measures by entity
    (let [calc-by-entity (group-by :entity calc-m)]
      (doseq [[entity-name entity-measures] calc-by-entity]
        (let [entity (get entities (keyword entity-name) (get entities entity-name))]
          (if-not entity
            (swap! errors conj (str "Calculated measure references unknown entity: " entity-name))
            (let [result (expr/validate-calculated-measures
                          entity-measures
                          (:columns entity))]
              (if (:valid result)
                (swap! sort-orders assoc entity-name (:sorted result))
                (doseq [e (:errors result)]
                  (swap! errors conj e))))))))

    ;; Validate restricted measures
    (doseq [rm restr-m]
      (let [entity-name (or (:entity rm) (first (keys entities)))
            entity      (get entities (keyword entity-name) (get entities entity-name))]
        (if-not entity
          (swap! errors conj (str "Restricted measure references unknown entity: " entity-name))
          (let [result (expr/validate-restricted-measure
                        rm entity-name (:columns entity) rels)]
            (when-not (:valid result)
              (doseq [e (:errors result)]
                (swap! errors conj (str "Restricted measure '" (:name rm) "': " e))))))))

    ;; Validate hierarchies
    (doseq [h hierarchies]
      (let [entity-name (:entity h)
            entity      (get entities (keyword entity-name) (get entities entity-name))]
        (if-not entity
          (swap! errors conj (str "Hierarchy references unknown entity: " entity-name))
          (let [result (expr/validate-hierarchy h (:columns entity))]
            (when-not (:valid result)
              (doseq [e (:errors result)]
                (swap! errors conj (str "Hierarchy '" (:name h) "': " e))))))))

    ;; Validate relationship uniqueness (Phase 3 — lazy association support)
    (when (seq rels)
      (let [rel-result (assoc-resolver/validate-relationship-uniqueness rels)]
        (when-not (:valid rel-result)
          (doseq [e (:errors rel-result)]
            (swap! errors conj e)))))

    (if (empty? @errors)
      {:valid true :entity-sort-orders @sort-orders}
      {:valid false :errors @errors})))

(defn build-isl-measures-for-entity
  "Build the ISL-visible measure list for a semantic model entity.
   Returns a vector of {:name :type :description} maps for prompt generation."
  [model-json entity-name]
  (let [calc-m  (->> (or (:calculated_measures model-json) [])
                     (filter #(= entity-name (:entity %))))
        restr-m (->> (or (:restricted_measures model-json) [])
                     (filter #(= entity-name (:entity %))))]
    (into
     (mapv (fn [m]
             {:name (:name m)
              :type "calculated"
              :aggregation (expr/classify-aggregation (:expression m) (:aggregation m))
              :description (or (:description m)
                               (str (:expression m) " (" (expr/classify-aggregation (:expression m) (:aggregation m)) ")"))})
           calc-m)
     (mapv (fn [m]
             {:name (:name m)
              :type "restricted"
              :description (or (:description m)
                               (str (:base_measure m) " WHERE " (:filter_column m)
                                    " IN " (pr-str (:filter_values m))))})
           restr-m))))

(defn build-isl-hierarchies-for-entity
  "Build the ISL-visible hierarchy list for a semantic model entity.
   Returns a vector of {:name :levels} maps."
  [model-json entity-name]
  (->> (or (:hierarchies model-json) [])
       (filter #(= entity-name (:entity %)))
       (mapv (fn [h]
               {:name (:name h)
                :levels (mapv :column (:levels h))}))))
