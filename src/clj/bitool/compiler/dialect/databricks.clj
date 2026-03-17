(ns bitool.compiler.dialect.databricks
  (:require [clojure.string :as string]))

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
                      (str expression " AS " target_column))
                    (:select sql-ir))))

(defn compile-select-sql
  [sql-ir]
  (let [source-relation (get-in sql-ir [:sources 0 :relation])
        source-alias    (get-in sql-ir [:sources 0 :alias] "bronze")
        where-sql       (some->> (:where sql-ir) (string/join " AND ") not-empty)]
    (str "SELECT "
         (select-list sql-ir)
         " FROM "
         source-relation
         " " source-alias
         (when where-sql
           (str " WHERE " where-sql))
         (when-let [group-exprs (seq (group-by-list sql-ir))]
           (str " GROUP BY " (string/join ", " group-exprs))))))

(defn compile-materialization-sql
  [sql-ir select-sql]
  (let [{:keys [mode target keys]} (:materialization sql-ir)
        target-columns            (mapv :target_column (:select sql-ir))]
    (case mode
      "merge"
      (let [update-set (string/join ", "
                                    (map #(str "t." % " = s." %) target-columns))
            join-on    (string/join " AND "
                                    (map #(str "t." % " = s." %) keys))]
        (str "MERGE INTO " target " t "
             "USING (" select-sql ") s "
             "ON " join-on " "
             "WHEN MATCHED THEN UPDATE SET " update-set " "
             "WHEN NOT MATCHED THEN INSERT ("
             (string/join ", " target-columns)
             ") VALUES ("
             (string/join ", " (map #(str "s." %) target-columns))
             ")"))

      "table_replace"
      (str "CREATE OR REPLACE TABLE " target " AS " select-sql)

      "append"
      (str "INSERT INTO " target " ("
           (string/join ", " target-columns)
           ") " select-sql)

      (throw (ex-info "Unsupported materialization mode for Databricks compilation"
                      {:mode mode
                       :sql_ir sql-ir})))))
