(ns bitool.compiler.ir)

(defn proposal-columns
  [proposal-json]
  (vec (or (:columns proposal-json) [])))

(defn build-sql-ir
  [proposal-json source-table]
  {:sources [{:alias (or (:source_alias proposal-json)
                         (:source_layer proposal-json)
                         "bronze")
              :relation source-table}]
   :select (mapv (fn [{:keys [target_column expression type nullable source_paths source_columns]}]
                   {:target_column target_column
                    :expression expression
                    :type type
                    :nullable nullable
                    :source_paths source_paths
                    :source_columns source_columns})
                 (proposal-columns proposal-json))
   :where (some-> (:graph_filter_sql proposal-json) str not-empty vector)
   :group_by (vec (or (:group_by proposal-json) []))
   :materialization {:mode (get-in proposal-json [:materialization :mode])
                     :target (:target_table proposal-json)
                     :keys (vec (or (get-in proposal-json [:materialization :keys]) []))}
   :target_warehouse (or (:target_warehouse proposal-json)
                         (:target_kind proposal-json)
                         "databricks")})
