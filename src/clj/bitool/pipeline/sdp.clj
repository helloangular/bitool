(ns bitool.pipeline.sdp
  "Stage 7: PipelineSpec -> Databricks SDP SQL artifacts.
   Optional export — not the primary Bitool runtime in MVP."
  (:require [clojure.string :as string]))

(defn- col-select [columns]
  (->> columns
       (map (fn [{:keys [source target type]}]
              (let [src (str "data_items_" source)
                    tgt target]
                (if (= src tgt)
                  (str "  " src)
                  (str "  " src " AS " tgt)))))
       (string/join ",\n")))

(defn generate-silver-sdp
  "Generate SDP SQL for a Silver entity."
  [{:keys [target-model source-bronze columns business-keys entity-kind]}
   {:keys [catalog bronze-schema silver-schema]}]
  (let [fq-source (str catalog "." bronze-schema "." source-bronze)
        fq-target (str catalog "." silver-schema "." target-model)
        select-cols (col-select columns)
        where-clause (when (seq business-keys)
                       (str "\nWHERE "
                            (string/join " AND "
                                         (map #(str "data_items_" % " IS NOT NULL")
                                              business-keys))))]
    (str "-- Silver: " target-model " (" entity-kind ")\n"
         "CREATE OR REFRESH STREAMING TABLE " fq-target " AS\n"
         "SELECT\n"
         select-cols ",\n"
         "  ingested_at_utc,\n"
         "  run_id\n"
         "FROM STREAM(" fq-source ")"
         (or where-clause "")
         ";\n")))

(defn generate-gold-sdp
  "Generate SDP SQL for a Gold model."
  [{:keys [target-model grain sql-template depends-on]}
   {:keys [catalog gold-schema]}]
  (let [fq-target (str catalog "." gold-schema "." target-model)]
    (if sql-template
      (str "-- Gold: " target-model " (" grain " grain)\n"
           "CREATE OR REFRESH MATERIALIZED VIEW " fq-target " AS\n"
           sql-template ";\n")
      (str "-- Gold: " target-model " (" grain " grain)\n"
           "-- TODO: SQL template not yet generated. Depends on: "
           (string/join ", " depends-on) "\n"
           "-- Measures: " (string/join ", " (or (:measures (meta target-model)) [])) "\n"))))

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
