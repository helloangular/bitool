(ns bitool.compiler.core
  (:require [bitool.compiler.dialect.databricks :as databricks]
            [bitool.compiler.dialect.snowflake :as snowflake]
            [bitool.compiler.ir :as ir]
            [clojure.string :as string]))

(defn normalize-warehouse
  [warehouse]
  (-> (or warehouse "databricks")
      name
      string/lower-case))

(defn compile-model
  [{:keys [target-warehouse proposal-json source-table]}]
  (let [warehouse  (normalize-warehouse target-warehouse)
        sql-ir     (ir/build-sql-ir (assoc proposal-json :target_warehouse warehouse) source-table)
        compile-fn (case warehouse
                     "snowflake" {:select snowflake/compile-select-sql
                                  :materialize snowflake/compile-materialization-sql}
                     {:select databricks/compile-select-sql
                      :materialize databricks/compile-materialization-sql})
        select-sql ((:select compile-fn) sql-ir)]
    {:sql_ir sql-ir
     :select_sql select-sql
     :compiled_sql ((:materialize compile-fn) sql-ir select-sql)
     :target_warehouse warehouse}))
