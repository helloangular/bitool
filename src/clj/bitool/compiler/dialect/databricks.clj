(ns bitool.compiler.dialect.databricks
  (:require [clojure.string :as string]))

(def ^:private databricks-fragment-pattern
  #"(?is)^[A-Za-z0-9_.'\"`()\[\]{}=<>!%+\-*/,:\s|&]+$")

(def ^:private databricks-disallowed-keyword-pattern
  #"(?is)\b(SELECT|FROM|JOIN|QUALIFY|UNION|WITH|MERGE|INSERT|UPDATE|DELETE|COPY|OPTIMIZE|VACUUM|REORG|ALTER|DROP|CREATE|CALL)\b")

(def ^:private databricks-comment-pattern
  #"(?s)(--|/\*|\*/|;)")

(def ^:private databricks-function-call-pattern
  #"(?is)\b[A-Za-z_][A-Za-z0-9_]*\(")

(def ^:private databricks-qualified-ref-pattern
  #"(?is)(?:^|[^A-Za-z0-9_`])(?:s|t)\s*\.\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)")

(def ^:private databricks-simple-literal-pattern
  #"(?is)^('([^']|'')*'|-?\d+(?:\.\d+)?|NULL|TRUE|FALSE)$")

(defn- quote-ident
  [x]
  (str "`" (-> (or x "")
               str
               (string/replace "`" "``"))
       "`"))

(defn- qualified-ident
  [x]
  (->> (string/split (str x) #"\.")
       (remove string/blank?)
       (map quote-ident)
       (string/join ".")))

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
                      (str expression " AS " (quote-ident target_column)))
                    (:select sql-ir))))

(defn- join-on-sql
  [keys]
  (string/join " AND "
               (map #(str "t." (quote-ident %) " = s." (quote-ident %)) keys)))

(defn- normalize-merge-keys
  [keys]
  (->> (or keys [])
       (map #(some-> % str string/trim))
       (remove string/blank?)
       vec))

(defn- ensure-merge-keys!
  [sql-ir]
  (let [keys (normalize-merge-keys (get-in sql-ir [:materialization :keys]))]
    (when (empty? keys)
      (throw (ex-info "Databricks materialization requires at least one merge key"
                      {:sql_ir sql-ir})))
    keys))

(defn- safe-databricks-fragment?
  [value {:keys [require-ref? allow-or? allow-literal-only?]
          :or {require-ref? false
               allow-or? true
               allow-literal-only? false}}]
  (let [fragment (some-> value str string/trim)
        has-ref? (boolean (and fragment (re-find databricks-qualified-ref-pattern fragment)))
        literal-only? (boolean (and fragment (re-matches databricks-simple-literal-pattern fragment)))]
    (boolean
     (and (seq fragment)
          (re-matches databricks-fragment-pattern fragment)
          (not (re-find databricks-comment-pattern fragment))
          (not (re-find databricks-disallowed-keyword-pattern fragment))
          (not (re-find databricks-function-call-pattern fragment))
          (or allow-or?
              (not (re-find #"(?i)\bOR\b" fragment)))
          (or (not require-ref?) has-ref?)
          (or has-ref?
              allow-literal-only?
              literal-only?)))))

(defn- ensure-condition-safe!
  [label condition]
  (when (some? condition)
    (when-not (safe-databricks-fragment? condition {:require-ref? true
                                                    :allow-or? false})
      (throw (ex-info (str "Unsafe Databricks " label " condition")
                      {:label label
                       :condition condition})))))

(defn- ensure-assignment-safe!
  [label {:keys [target_column expression] :as assignment}]
  (when (string/blank? (some-> target_column str string/trim))
    (throw (ex-info (str "Databricks " label " assignment target_column is required")
                    {:label label
                     :assignment assignment})))
  (when-not (safe-databricks-fragment? expression {:allow-literal-only? true})
    (throw (ex-info (str "Unsafe Databricks " label " assignment expression")
                    {:label label
                     :assignment assignment})))
  assignment)

(defn- default-update-assignments
  [target-columns]
  (mapv (fn [target-column]
          {:target_column target-column
           :expression (str "s." (quote-ident target-column))})
        target-columns))

(defn- assignment-list-sql
  [assignments]
  (string/join ", "
               (map (fn [{:keys [target_column expression]}]
                      (str "t." (quote-ident target_column) " = " expression))
                    assignments)))

(defn- when-clause-sql
  [prefix condition body]
  (str prefix
       (when (seq (str condition))
         (str " AND " condition))
       " THEN "
       body))

(defn compile-select-sql
  [sql-ir]
  (let [source-relation (get-in sql-ir [:sources 0 :relation])
        source-alias    (get-in sql-ir [:sources 0 :alias] "bronze")
        where-sql       (some->> (:where sql-ir) (string/join " AND ") not-empty)]
    (str "SELECT "
         (select-list sql-ir)
         " FROM "
         (qualified-ident source-relation)
         " " source-alias
         (when where-sql
           (str " WHERE " where-sql))
         (when-let [group-exprs (seq (group-by-list sql-ir))]
           (str " GROUP BY " (string/join ", " group-exprs))))))

(defn compile-materialization-sql
  [sql-ir select-sql]
  (let [{:keys [mode target keys schema_evolution update_on_matched insert_on_not_matched
                matched_condition not_matched_condition update_assignments when_not_matched_by_source]} (:materialization sql-ir)
        merge-keys                (when (#{"merge" "update" "delete"} mode)
                                    (ensure-merge-keys! sql-ir))
        target-table              (qualified-ident target)
        target-columns            (mapv :target_column (:select sql-ir))
        quoted-target-columns     (mapv quote-ident target-columns)
        default-assignments       (default-update-assignments target-columns)
        update-assignments        (mapv #(ensure-assignment-safe! "matched" %)
                                        (vec (or update_assignments default-assignments)))
        when-not-matched-by-source (when (map? when_not_matched_by_source)
                                     (let [assignments (vec (:assignments when_not_matched_by_source))
                                           action      (:action when_not_matched_by_source)]
                                       (ensure-condition-safe! "WHEN NOT MATCHED BY SOURCE"
                                                               (:condition when_not_matched_by_source))
                                       (when (= "update" action)
                                         (when (empty? assignments)
                                           (throw (ex-info "Databricks WHEN NOT MATCHED BY SOURCE UPDATE requires assignments"
                                                           {:sql_ir sql-ir})))
                                         (doseq [assignment assignments]
                                           (ensure-assignment-safe! "not matched by source" assignment)))
                                       (assoc when_not_matched_by_source
                                              :assignments assignments)))
        _                        (ensure-condition-safe! "matched" matched_condition)
        _                        (ensure-condition-safe! "not matched" not_matched_condition)
        join-on                   (when merge-keys (join-on-sql merge-keys))]
    (case mode
      "merge"
      (let [clauses (cond-> []
                      (not= false update_on_matched)
                      (conj (when-clause-sql "WHEN MATCHED"
                                             matched_condition
                                             (str "UPDATE SET " (assignment-list-sql update-assignments))))
                      (not= false insert_on_not_matched)
                      (conj (when-clause-sql "WHEN NOT MATCHED"
                                             not_matched_condition
                                             (str "INSERT ("
                                                  (string/join ", " quoted-target-columns)
                                                  ") VALUES ("
                                                  (string/join ", " (map #(str "s." (quote-ident %)) target-columns))
                                                  ")")))
                      (map? when_not_matched_by_source)
                      (conj (case (:action when_not_matched_by_source)
                              "delete" (when-clause-sql "WHEN NOT MATCHED BY SOURCE"
                                                        (:condition when_not_matched_by_source)
                                                        "DELETE")
                              "update" (when-clause-sql "WHEN NOT MATCHED BY SOURCE"
                                                        (:condition when_not_matched_by_source)
                                                        (str "UPDATE SET "
                                                             (assignment-list-sql
                                                              (vec (:assignments when_not_matched_by_source)))))
                              (throw (ex-info "Unsupported Databricks merge action for WHEN NOT MATCHED BY SOURCE"
                                              {:action (:action when_not_matched_by_source)
                                               :sql_ir sql-ir})))))]
        (when (empty? clauses)
          (throw (ex-info "Databricks merge materialization requires at least one action clause"
                          {:mode mode
                           :sql_ir sql-ir})))
        (str (if schema_evolution "MERGE WITH SCHEMA EVOLUTION INTO " "MERGE INTO ")
             target-table " t "
             "USING (" select-sql ") s "
             "ON " join-on " "
             (string/join " " clauses)))

      "update"
      (str "MERGE INTO " target-table " t "
           "USING (" select-sql ") s "
           "ON " join-on " "
           (when-clause-sql "WHEN MATCHED"
                            matched_condition
                            (str "UPDATE SET " (assignment-list-sql update-assignments))))

      "delete"
      (str "DELETE FROM " target-table " t WHERE EXISTS (SELECT 1 FROM (" select-sql ") s WHERE "
           join-on
           (when (seq (str matched_condition))
             (str " AND " matched_condition))
           ")")

      "table_replace"
      (str "CREATE OR REPLACE TABLE " target-table " AS " select-sql)

      "append"
      (str "INSERT INTO " target-table " ("
           (string/join ", " quoted-target-columns)
           ") " select-sql)

      (throw (ex-info "Unsupported materialization mode for Databricks compilation"
                      {:mode mode
                       :sql_ir sql-ir})))))
