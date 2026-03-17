(ns bitool.compiler.ir
  (:require [clojure.string :as string]))

(defn- valid-alias?
  [value]
  (boolean
   (and (string? value)
        (re-matches #"[A-Za-z_][A-Za-z0-9_]*" value))))

(defn- safe-graph-filter?
  [value]
  (let [sql (some-> value str string/trim)]
    (or (string/blank? sql)
        (and (not (re-find #"(?:--|/\*|\*/|;)" sql))
             (not (re-find #"(?i)\b(select|insert|update|delete|drop|alter|merge|union|join|from|into|copy|put)\b" sql))
             (re-matches #"[A-Za-z0-9_.'\"()=<>!%+\-*/,\s]+" sql)))))

(defn proposal-columns
  [proposal-json]
  (vec (or (:columns proposal-json) [])))

(defn build-sql-ir
  [proposal-json source-table]
  (let [source-alias (or (:source_alias proposal-json)
                         (:source_layer proposal-json)
                         "bronze")
        graph-filter (some-> (:graph_filter_sql proposal-json) str string/trim not-empty)]
    (when-not (valid-alias? source-alias)
      (throw (ex-info "Proposal source alias must be a valid SQL identifier"
                      {:status 400
                       :field :source_alias
                       :value source-alias})))
    (when (and graph-filter (not (safe-graph-filter? graph-filter)))
      (throw (ex-info "Proposal graph filter contains unsupported SQL"
                      {:status 400
                       :field :graph_filter_sql})))
    {:sources [{:alias source-alias
              :relation source-table}]
     :select (mapv (fn [{:keys [target_column expression type nullable source_paths source_columns]}]
                     {:target_column target_column
                      :expression expression
                      :type type
                      :nullable nullable
                      :source_paths source_paths
                      :source_columns source_columns})
                   (proposal-columns proposal-json))
     :where (some-> graph-filter vector)
     :group_by (vec (or (:group_by proposal-json) []))
     :materialization {:mode (get-in proposal-json [:materialization :mode])
                       :target (:target_table proposal-json)
                       :keys (vec (or (get-in proposal-json [:materialization :keys]) []))}
     :target_warehouse (or (:target_warehouse proposal-json)
                           (:target_kind proposal-json)
                           "databricks")}))
