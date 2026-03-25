(ns bitool.compiler.dialect.postgresql
  (:require [clojure.string :as string]))

(defn- quote-ident
  [x]
  (str "\"" (-> (or x "")
                str
                (string/replace "\"" "\"\""))
       "\""))

(defn- qualified-ident
  [x]
  (->> (string/split (str x) #"\.")
       (remove string/blank?)
       (map quote-ident)
       (string/join ".")))

(defn- quote-mixed-case-refs
  "Quote unquoted alias.column references where the column contains uppercase
   letters. PostgreSQL lowercases unquoted identifiers, breaking camelCase names
   like data_items_createdAtTime. Already-quoted refs (alias.\"col\") are left alone."
  [expression]
  (if (string? expression)
    (string/replace expression
                    #"(\b[A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b(?!\")"
                    (fn [[_ alias col]]
                      (if (re-find #"[A-Z]" col)
                        (str alias "." (quote-ident col))
                        (str alias "." col))))
    expression))

(defn- group-by-list
  [sql-ir]
  (let [group-cols (set (:group_by sql-ir))]
    (->> (:select sql-ir)
         (filter (fn [{:keys [target_column]}]
                   (contains? group-cols target_column)))
         (map :expression)
         vec)))

(defn- select-list
  [sql-ir]
  (string/join ", "
               (map (fn [{:keys [target_column expression]}]
                      (str (quote-mixed-case-refs expression) " AS " (quote-ident target_column)))
                    (:select sql-ir))))

(defn- jsonb-array-expansion-sql
  [sql-ir]
  (when-let [{:keys [kind alias json_column path]} (:source_expansion sql-ir)]
    (when (= kind "jsonb_array")
      (let [source-alias (get-in sql-ir [:sources 0 :alias] "bronze")
            json-expr    (str "(" source-alias "." (quote-ident (or json_column "payload_json")) "::jsonb)")
            path-expr    (if (seq path)
                           (str json-expr " #> '{" (string/join "," path) "}'")
                           json-expr)]
        (str " CROSS JOIN LATERAL jsonb_array_elements(" path-expr ") AS " (or alias "item") "(value)")))))

(defn- source-select-list
  [target-columns]
  (string/join ", "
               (map #(str "s." (quote-ident %)) target-columns)))

(defn- policy-business-keys
  [sql-ir]
  (vec (or (get-in sql-ir [:processing_policy :business_keys])
           (get-in sql-ir [:materialization :keys])
           [])))

(defn- policy-requires-ranking?
  [sql-ir]
  (let [ordering-strategy (get-in sql-ir [:processing_policy :ordering_strategy])
        business-keys     (policy-business-keys sql-ir)]
    (and (seq business-keys)
         (contains? #{"latest_event_time_wins" "latest_sequence_wins" "event_time_then_sequence"} ordering-strategy))))

(defn- interval-sql
  [{:keys [value unit]}]
  (when (and (some? value) (seq (str unit)))
    (str "INTERVAL '" value " " unit "'")))

(defn- policy-filter-clause
  [sql-ir]
  (let [event-time-column (get-in sql-ir [:processing_policy :event_time_column])
        reprocess-window  (get-in sql-ir [:processing_policy :reprocess_window])]
    (when-let [interval-expr (and event-time-column (interval-sql reprocess-window))]
      (str "s." (quote-ident event-time-column) " >= NOW() - " interval-expr))))

(defn- ranking-order-clauses
  [sql-ir]
  (let [event-time-column (get-in sql-ir [:processing_policy :event_time_column])
        sequence-column   (get-in sql-ir [:processing_policy :sequence_column])
        ordering-strategy (get-in sql-ir [:processing_policy :ordering_strategy])]
    (vec
     (concat
      (case ordering-strategy
        "latest_event_time_wins" [(str "s." (quote-ident event-time-column) " DESC NULLS LAST")]
        "latest_sequence_wins"   [(str "s." (quote-ident sequence-column) " DESC NULLS LAST")]
        "event_time_then_sequence" [(str "s." (quote-ident event-time-column) " DESC NULLS LAST")
                                    (str "s." (quote-ident sequence-column) " DESC NULLS LAST")]
        [])
      ["md5(row_to_json(s)::text) DESC"]))))

(defn- compile-base-select-sql
  [sql-ir]
  (let [source-relation (get-in sql-ir [:sources 0 :relation])
        source-alias    (get-in sql-ir [:sources 0 :alias] "bronze")
        where-sql       (some->> (:where sql-ir) (string/join " AND ") not-empty)
        expansion-sql   (jsonb-array-expansion-sql sql-ir)]
    (str "SELECT "
         (select-list sql-ir)
         " FROM "
         (qualified-ident source-relation)
         " " source-alias
         expansion-sql
         (when where-sql
           (str " WHERE " where-sql))
         (when-let [group-exprs (seq (group-by-list sql-ir))]
           (str " GROUP BY " (string/join ", " group-exprs))))))

(defn- postgres-type
  [logical-type]
  (case (-> (or logical-type "STRING") name string/upper-case)
    "STRING" "TEXT"
    "VARCHAR" "TEXT"
    "TEXT" "TEXT"
    "BOOLEAN" "BOOLEAN"
    "BOOL" "BOOLEAN"
    "DATE" "DATE"
    "TIMESTAMP" "TIMESTAMP"
    "TIMESTAMPTZ" "TIMESTAMPTZ"
    "INT" "INTEGER"
    "INTEGER" "INTEGER"
    "BIGINT" "BIGINT"
    "LONG" "BIGINT"
    "DOUBLE" "DOUBLE PRECISION"
    "FLOAT" "DOUBLE PRECISION"
    "DECIMAL" "NUMERIC"
    "NUMERIC" "NUMERIC"
    "JSON" "JSONB"
    "JSONB" "JSONB"
    "TEXT"))

(defn compile-select-sql
  [sql-ir]
  (let [base-select-sql (compile-base-select-sql sql-ir)
        target-columns  (mapv :target_column (:select sql-ir))
        filter-clause   (policy-filter-clause sql-ir)
        ranking?        (policy-requires-ranking? sql-ir)
        business-keys   (policy-business-keys sql-ir)
        rank-order-sql  (string/join ", " (ranking-order-clauses sql-ir))
        selected-cols   (string/join ", " (map #(str "s." (quote-ident %)) target-columns))]
    (cond
      ranking?
      (str "WITH source_rows AS (" base-select-sql "), "
           "filtered_rows AS (SELECT s.* FROM source_rows s"
           (when filter-clause
             (str " WHERE " filter-clause))
           "), "
           "ranked_rows AS (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY "
           (string/join ", " (map #(str "s." (quote-ident %)) business-keys))
           " ORDER BY " rank-order-sql ") AS " (quote-ident "__bitool_row_num")
           " FROM filtered_rows s) "
           "SELECT " selected-cols
           " FROM ranked_rows s WHERE s." (quote-ident "__bitool_row_num") " = 1")

      filter-clause
      (str "WITH source_rows AS (" base-select-sql ") "
           "SELECT " selected-cols
           " FROM source_rows s WHERE " filter-clause)

      :else
      base-select-sql)))

(defn compile-create-table-sql
  [sql-ir]
  (let [target-table (qualified-ident (get-in sql-ir [:materialization :target]))
        column-defs  (string/join ", "
                                  (map (fn [{:keys [target_column type]}]
                                         (str (quote-ident target_column) " " (postgres-type type)))
                                       (:select sql-ir)))]
    (str "CREATE TABLE IF NOT EXISTS " target-table " (" column-defs ")")))

(defn compile-materialization-sql
  [sql-ir select-sql]
  (let [{:keys [mode target keys]} (:materialization sql-ir)
        target-table          (qualified-ident target)
        target-columns        (mapv :target_column (:select sql-ir))
        quoted-target-columns (mapv quote-ident target-columns)
        insert-head           (str "INSERT INTO " target-table " ("
                                   (string/join ", " quoted-target-columns)
                                   ") ")
        insert-select         (str "SELECT " (source-select-list target-columns) " FROM source_rows s")]
    (case mode
      "merge"
      (do
        (when-not (seq keys)
          (throw (ex-info "PostgreSQL merge materialization requires one or more merge keys"
                          {:mode mode
                           :sql_ir sql-ir})))
        (let [join-on (string/join " AND "
                                   (map #(str "t." (quote-ident %) " = s." (quote-ident %)) keys))]
          (str "WITH source_rows AS (" select-sql "), "
               "deleted AS (DELETE FROM " target-table " t USING source_rows s WHERE " join-on " RETURNING 1) "
               insert-head
               insert-select)))

      "table_replace"
      (str "WITH deleted AS (DELETE FROM " target-table " RETURNING 1), "
           "source_rows AS (" select-sql ") "
           insert-head
           insert-select)

      "append"
      (str insert-head select-sql)

      (throw (ex-info "Unsupported materialization mode for PostgreSQL compilation"
                      {:mode mode
                       :sql_ir sql-ir})))))
