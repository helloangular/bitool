(ns bitool.semantic.perspective
  "Perspectives — audience-scoped views of a Semantic Model.

   A perspective is a named subset of a published semantic model that controls
   which entities, columns, measures, and hierarchies are visible to a particular
   audience (e.g., 'finance', 'ops', 'executive').

   The perspective itself is lightweight metadata stored alongside the model.
   At query time, `apply-perspective` filters the model to only the allowed
   elements before the ISL pipeline sees it."
  (:require [bitool.db :as db]
            [bitool.semantic.expression :as expr]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; DDL
;; ---------------------------------------------------------------------------

(def ^:private perspective-ready? (atom false))
(def ^:private perspective-table "semantic_perspective")

(defn- db-opts [ds]
  (jdbc/with-options ds {:builder-fn rs/as-unqualified-lower-maps}))

(defn ensure-perspective-tables!
  []
  (when-not @perspective-ready?
    (locking perspective-ready?
      (when-not @perspective-ready?
        (doseq [ddl
                [(str "CREATE TABLE IF NOT EXISTS " perspective-table " ("
                      "perspective_id BIGSERIAL PRIMARY KEY, "
                      "model_id BIGINT NOT NULL, "
                      "name TEXT NOT NULL, "
                      "description TEXT, "
                      "audience TEXT NOT NULL DEFAULT 'default', "
                      "spec_json TEXT NOT NULL, "
                      "created_by TEXT NOT NULL DEFAULT 'system', "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE UNIQUE INDEX IF NOT EXISTS idx_perspective_uniq "
                      "ON " perspective-table " (model_id, name)")]]
          (jdbc/execute! db/ds [ddl]))
        (reset! perspective-ready? true)))))

;; ---------------------------------------------------------------------------
;; Spec shape
;; ---------------------------------------------------------------------------
;;
;; A perspective spec is:
;; {:entities     ["trips" "drivers"]                ;; allowed entity names (nil = all)
;;  :columns      {"trips" ["miles" "fuel_cost" "region"]
;;                 "drivers" ["driver_name" "region"]}   ;; per-entity column allowlists (nil = all)
;;  :measures     ["cost_per_mile" "total_trips"]    ;; allowed measure names (nil = all)
;;  :hierarchies  ["geography"]                      ;; allowed hierarchy names (nil = all)
;; }
;;
;; nil/missing keys mean "no restriction" (allow all).

;; ---------------------------------------------------------------------------
;; CRUD
;; ---------------------------------------------------------------------------

(defn create-perspective!
  "Create a new perspective for a model. Returns the created row."
  [{:keys [model-id name description audience spec created-by]}]
  (ensure-perspective-tables!)
  (let [spec-json (if (string? spec) spec (json/generate-string spec))
        by        (or created-by "system")]
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "INSERT INTO " perspective-table
           " (model_id, name, description, audience, spec_json, created_by)"
           " VALUES (?, ?, ?, ?, ?, ?)"
           " RETURNING *")
      model-id name description (or audience "default") spec-json by])))

(defn list-perspectives
  "List perspectives for a model."
  [model-id]
  (ensure-perspective-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "SELECT * FROM " perspective-table
         " WHERE model_id = ? ORDER BY name")
    model-id]))

(defn get-perspective
  "Fetch a single perspective by ID."
  [perspective-id]
  (ensure-perspective-tables!)
  (when-let [row (jdbc/execute-one!
                  (db-opts db/ds)
                  [(str "SELECT * FROM " perspective-table
                        " WHERE perspective_id = ?")
                   perspective-id])]
    (let [spec (try (json/parse-string (:spec_json row) true)
                    (catch Exception _ {}))]
      (assoc row :spec spec))))

(defn get-perspective-by-name
  "Fetch a perspective by model_id + name."
  [model-id perspective-name]
  (ensure-perspective-tables!)
  (when-let [row (jdbc/execute-one!
                  (db-opts db/ds)
                  [(str "SELECT * FROM " perspective-table
                        " WHERE model_id = ? AND name = ?")
                   model-id perspective-name])]
    (let [spec (try (json/parse-string (:spec_json row) true)
                    (catch Exception _ {}))]
      (assoc row :spec spec))))

(defn update-perspective!
  "Update a perspective's spec, description, or audience."
  [perspective-id {:keys [description audience spec updated-by]}]
  (ensure-perspective-tables!)
  (let [spec-json (when spec (if (string? spec) spec (json/generate-string spec)))]
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "UPDATE " perspective-table " SET "
           (string/join ", "
             (cond-> ["updated_at_utc = now()"]
               description (conj "description = ?")
               audience    (conj "audience = ?")
               spec-json   (conj "spec_json = ?")))
           " WHERE perspective_id = ? RETURNING *")
      ;; params in same order as SET clauses
      ;; Using a flat approach
      (cond-> []
        description (conj description)
        audience    (conj audience)
        spec-json   (conj spec-json)
        true        (conj perspective-id))])))

(defn delete-perspective!
  "Delete a perspective by ID."
  [perspective-id]
  (ensure-perspective-tables!)
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "DELETE FROM " perspective-table
         " WHERE perspective_id = ? RETURNING perspective_id")
    perspective-id]))

;; ---------------------------------------------------------------------------
;; Perspective application — pure functions (no DB)
;; ---------------------------------------------------------------------------

(defn validate-perspective-spec
  "Validate a perspective spec against a semantic model.
   Returns {:valid true} or {:valid false :errors [...]}"
  [spec model-json]
  (let [errors      (atom [])
        entity-keys (set (map name (keys (or (:entities model-json) {}))))
        all-measures (set (concat
                           (map :name (or (:calculated_measures model-json) []))
                           (map :name (or (:restricted_measures model-json) []))))
        all-hiers   (set (map :name (or (:hierarchies model-json) [])))]

    ;; Validate entities
    (when-let [allowed-entities (:entities spec)]
      (doseq [e allowed-entities]
        (when-not (contains? entity-keys (name e))
          (swap! errors conj (str "Perspective references unknown entity: " e)))))

    ;; Validate columns per entity
    (when-let [col-map (:columns spec)]
      (doseq [[entity-name cols] col-map]
        (let [ename (name entity-name)
              entity (get (:entities model-json) (keyword ename)
                          (get (:entities model-json) ename))]
          (if-not entity
            (swap! errors conj (str "Column filter references unknown entity: " ename))
            (let [entity-col-names (set (map :name (:columns entity)))]
              (doseq [c cols]
                (when-not (contains? entity-col-names c)
                  (swap! errors conj
                         (str "Column '" c "' not found on entity '" ename "'")))))))))

    ;; Validate measures
    (when-let [allowed-measures (:measures spec)]
      (doseq [m allowed-measures]
        (when-not (contains? all-measures m)
          (swap! errors conj (str "Perspective references unknown measure: " m)))))

    ;; Validate hierarchies
    (when-let [allowed-hiers (:hierarchies spec)]
      (doseq [h allowed-hiers]
        (when-not (contains? all-hiers h)
          (swap! errors conj (str "Perspective references unknown hierarchy: " h)))))

    (if (empty? @errors)
      {:valid true}
      {:valid false :errors @errors})))

(defn validate-filtered-consistency
  "Check that a filtered model is internally consistent:
   - Retained calculated measures only reference columns still present on their entity
   - Retained restricted measures' filter_column is still present on their entity
   - Retained hierarchy levels still exist as columns on their entity
   Returns {:valid true} or {:valid false :errors [...]}"
  [filtered-model]
  (let [errors   (atom [])
        entities (or (:entities filtered-model) {})]

    ;; Check calculated measures
    (doseq [m (or (:calculated_measures filtered-model) [])]
      (let [entity-name (:entity m)
            entity      (get entities (keyword entity-name)
                             (get entities entity-name))
            col-names   (when entity (set (map :name (:columns entity))))
            ;; Extract column refs from expression (bare identifiers)
            expr-refs   (when (:expression m)
                          (try (expr/extract-column-refs (:expression m))
                               (catch Exception _ #{})))]
        (when (and entity col-names expr-refs)
          (doseq [ref expr-refs]
            (when-not (contains? col-names ref)
              (swap! errors conj
                     (str "Calculated measure '" (:name m) "' references column '"
                          ref "' which is hidden by the perspective on entity '"
                          entity-name "'")))))))

    ;; Check restricted measures
    (doseq [m (or (:restricted_measures filtered-model) [])]
      (let [entity-name (:entity m)
            entity      (get entities (keyword entity-name)
                             (get entities entity-name))
            col-names   (when entity (set (map :name (:columns entity))))
            filter-col  (:filter_column m)]
        (when (and entity col-names filter-col
                   (not (string/includes? (str filter-col) "."))
                   (not (contains? col-names filter-col)))
          (swap! errors conj
                 (str "Restricted measure '" (:name m) "' filter_column '"
                      filter-col "' is hidden by the perspective on entity '"
                      entity-name "'")))))

    ;; Check hierarchy levels
    (doseq [h (or (:hierarchies filtered-model) [])]
      (let [entity-name (:entity h)
            entity      (get entities (keyword entity-name)
                             (get entities entity-name))
            col-names   (when entity (set (map :name (:columns entity))))]
        (when (and entity col-names)
          (doseq [level (:levels h)]
            (let [col (or (:column level) level)]
              (when (and (string? col) (not (contains? col-names col)))
                (swap! errors conj
                       (str "Hierarchy '" (:name h) "' level '" col
                            "' is hidden by the perspective on entity '"
                            entity-name "'"))))))))

    (if (empty? @errors)
      {:valid true}
      {:valid false :errors @errors})))

(defn apply-perspective
  "Filter a semantic model through a perspective spec.
   Returns a new model-json with only the allowed entities, columns, measures,
   and hierarchies visible. nil spec = no filtering (full model)."
  [model-json perspective-spec]
  (if (or (nil? perspective-spec) (empty? perspective-spec))
    model-json
    (let [allowed-entities  (when-let [es (:entities perspective-spec)]
                              (set (map name es)))
          allowed-columns   (:columns perspective-spec)
          allowed-measures  (when-let [ms (:measures perspective-spec)]
                              (set ms))
          allowed-hiers     (when-let [hs (:hierarchies perspective-spec)]
                              (set hs))

          ;; Filter entities
          filtered-entities (if allowed-entities
                              (into {}
                                (filter (fn [[k _]] (contains? allowed-entities (name k))))
                                (or (:entities model-json) {}))
                              (:entities model-json))

          ;; Filter columns within allowed entities
          filtered-entities (if allowed-columns
                              (into {}
                                (map (fn [[k entity]]
                                       (let [col-filter (get allowed-columns (keyword (name k))
                                                             (get allowed-columns (name k)))]
                                         (if col-filter
                                           [k (update entity :columns
                                                      (fn [cols]
                                                        (filterv #(contains? (set col-filter) (:name %))
                                                                 cols)))]
                                           [k entity]))))
                                filtered-entities)
                              filtered-entities)

          ;; Filter calculated measures
          filtered-calc (cond->> (or (:calculated_measures model-json) [])
                          allowed-entities (filter #(contains? allowed-entities (:entity %)))
                          allowed-measures (filter #(contains? allowed-measures (:name %))))

          ;; Filter restricted measures
          filtered-restr (cond->> (or (:restricted_measures model-json) [])
                           allowed-entities (filter #(contains? allowed-entities (:entity %)))
                           allowed-measures (filter #(contains? allowed-measures (:name %))))

          ;; Filter hierarchies
          filtered-hiers (cond->> (or (:hierarchies model-json) [])
                           allowed-entities (filter #(contains? allowed-entities (:entity %)))
                           allowed-hiers    (filter #(contains? allowed-hiers (:name %))))

          ;; Filter relationships to only those between allowed entities
          filtered-rels (if allowed-entities
                          (filterv (fn [r]
                                     (and (contains? allowed-entities (:from r))
                                          (contains? allowed-entities (:to r))))
                                   (or (:relationships model-json) []))
                          (:relationships model-json))]

      (let [result (assoc model-json
                         :entities             filtered-entities
                         :calculated_measures  (vec filtered-calc)
                         :restricted_measures  (vec filtered-restr)
                         :hierarchies          (vec filtered-hiers)
                         :relationships        (or filtered-rels []))]
        ;; Post-filter consistency check: ensure retained measures/hierarchies
        ;; can still resolve their column dependencies after column filtering
        (let [{:keys [valid errors]} (validate-filtered-consistency result)]
          (when-not valid
            (throw (ex-info (str "Perspective produces an invalid model: "
                                 (string/join "; " errors))
                            {:errors errors
                             :perspective-spec perspective-spec}))))
        result))))
