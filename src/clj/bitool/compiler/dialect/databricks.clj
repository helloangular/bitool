(ns bitool.compiler.dialect.databricks
  (:require [clojure.string :as string]))

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
  (let [{:keys [mode target keys]} (:materialization sql-ir)
        target-table              (qualified-ident target)
        target-columns            (mapv :target_column (:select sql-ir))
        quoted-target-columns     (mapv quote-ident target-columns)]
    (case mode
      "merge"
      (let [update-set (string/join ", "
                                    (map #(str "t." (quote-ident %) " = s." (quote-ident %)) target-columns))
            join-on    (string/join " AND "
                                    (map #(str "t." (quote-ident %) " = s." (quote-ident %)) keys))]
        (str "MERGE INTO " target-table " t "
             "USING (" select-sql ") s "
             "ON " join-on " "
             "WHEN MATCHED THEN UPDATE SET " update-set " "
             "WHEN NOT MATCHED THEN INSERT ("
             (string/join ", " quoted-target-columns)
             ") VALUES ("
             (string/join ", " (map #(str "s." (quote-ident %)) target-columns))
             ")"))

      "table_replace"
      (str "CREATE OR REPLACE TABLE " target-table " AS " select-sql)

      "append"
      (str "INSERT INTO " target-table " ("
           (string/join ", " quoted-target-columns)
           ") " select-sql)

      (throw (ex-info "Unsupported materialization mode for Databricks compilation"
                      {:mode mode
                       :sql_ir sql-ir})))))
