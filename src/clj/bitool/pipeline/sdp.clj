(ns bitool.pipeline.sdp
  "Stage 7: PipelineSpec -> Databricks SDP SQL artifacts.
   Optional export — not the primary Bitool runtime in MVP."
  (:require [clojure.string :as string]))

(defn- col-select
  "Build SELECT column list. If columns is empty, select all with * from Bronze."
  [columns]
  (if (seq columns)
    (->> columns
         (map (fn [{:keys [source target]}]
                (let [src (str "data_items_" source)
                      tgt (or target source)]
                  (if (= src tgt)
                    (str "  " src)
                    (str "  " src " AS " tgt)))))
         (string/join ",\n"))
    ;; No explicit columns — select payload_json fields with wildcard
    "  *"))

(defn generate-silver-sdp
  "Generate SDP SQL for a Silver entity."
  [{:keys [target-model source-bronze columns business-keys entity-kind]}
   {:keys [catalog bronze-schema silver-schema]}]
  (let [fq-source (str catalog "." bronze-schema "."
                       (or source-bronze "unknown_source"))
        fq-target (str catalog "." silver-schema "." target-model)
        has-cols? (seq columns)
        select-cols (col-select columns)
        where-clause (when (seq business-keys)
                       (str "\nWHERE "
                            (string/join " AND "
                                         (map #(str "data_items_" % " IS NOT NULL")
                                              business-keys))))]
    (str "-- Silver: " target-model " (" entity-kind ")\n"
         "CREATE OR REFRESH STREAMING TABLE " fq-target " AS\n"
         "SELECT\n"
         select-cols
         (when has-cols? ",\n  ingested_at_utc,\n  run_id")
         "\n"
         "FROM STREAM(" fq-source ")"
         (or where-clause "")
         ";\n")))

(defn- grain-to-trunc [grain]
  (case (some-> grain string/lower-case)
    "day"    "DAY"
    "week"   "WEEK"
    "month"  "MONTH"
    "hour"   "HOUR"
    "minute" "MINUTE"
    "DAY"))

(defn- generate-gold-from-measures
  "Generate Gold SQL from measures and depends-on Silver tables."
  [{:keys [target-model grain depends-on measures dimensions]}
   {:keys [catalog silver-schema gold-schema]}]
  (let [fq-target (str catalog "." gold-schema "." target-model)
        trunc     (grain-to-trunc grain)
        ;; Use the first dependency as the main FROM table
        main-source (when (seq depends-on)
                      (str catalog "." silver-schema "." (first depends-on)))
        date-col  (str "DATE_TRUNC('" trunc "', ingested_at_utc)")
        ;; Build measure expressions
        measure-exprs (mapv (fn [m]
                              (case (string/lower-case (str m))
                                "count"          "COUNT(*) AS record_count"
                                "distinct_count" "COUNT(DISTINCT data_items_id) AS distinct_count"
                                "sum"            "SUM(data_items_amount) AS total_amount"
                                "avg"            "AVG(data_items_amount) AS avg_amount"
                                (str "COUNT(*) AS " (string/replace (str m) #"[^a-zA-Z0-9_]" "_"))))
                            (or measures ["count"]))
        select-parts (concat [(str "  " date-col " AS " (string/lower-case trunc) "_date")]
                             (map #(str "  " %) measure-exprs))]
    (str "SELECT\n"
         (string/join ",\n" select-parts) "\n"
         "FROM " (or main-source (str catalog "." silver-schema ".unknown_source")) "\n"
         "GROUP BY " date-col)))

(defn generate-gold-sdp
  "Generate SDP SQL for a Gold model."
  [{:keys [target-model grain sql-template depends-on measures] :as gold-model}
   {:keys [catalog gold-schema] :as ctx}]
  (let [fq-target (str catalog "." gold-schema "." target-model)]
    (cond
      ;; Explicit SQL template
      sql-template
      (str "-- Gold: " target-model " (" grain " grain)\n"
           "CREATE OR REFRESH MATERIALIZED VIEW " fq-target " AS\n"
           sql-template ";\n")

      ;; Has measures — generate from measures
      (seq measures)
      (let [inner-sql (generate-gold-from-measures gold-model ctx)]
        (str "-- Gold: " target-model " (" grain " grain)\n"
             "CREATE OR REFRESH MATERIALIZED VIEW " fq-target " AS\n"
             inner-sql ";\n"))

      ;; Fallback
      :else
      (str "-- Gold: " target-model " (" grain " grain)\n"
           "CREATE OR REFRESH MATERIALIZED VIEW " fq-target " AS\n"
           "SELECT\n"
           "  DATE_TRUNC('DAY', ingested_at_utc) AS day_date,\n"
           "  COUNT(*) AS record_count\n"
           "FROM " catalog "." (or (:silver-schema ctx) "silver") "."
           (first (or depends-on ["unknown"])) "\n"
           "GROUP BY DATE_TRUNC('DAY', ingested_at_utc);\n"))))

(defn generate-pipeline-sdp
  "Generate all SDP SQL artifacts for a PipelineSpec."
  [{:keys [silver-proposals gold-models catalog
           bronze-schema silver-schema gold-schema] :as spec}]
  (let [ctx {:catalog       catalog
             :bronze-schema bronze-schema
             :silver-schema silver-schema
             :gold-schema   gold-schema}
        silver-sql (mapv #(generate-silver-sdp % ctx) silver-proposals)
        gold-sql   (mapv #(generate-gold-sdp % ctx) gold-models)]
    {:silver silver-sql
     :gold   gold-sql
     :combined (string/join "\n\n" (concat silver-sql gold-sql))}))
