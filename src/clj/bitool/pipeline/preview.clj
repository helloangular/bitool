(ns bitool.pipeline.preview
  "Stage 5: PipelineSpec -> human-readable preview for the user.
   Pure function, no side effects."
  (:require [clojure.string :as string]))

(defn- format-columns [columns]
  (when (seq columns)
    (str (count columns) " columns: "
         (string/join ", " (map :target (take 6 columns)))
         (when (> (count columns) 6) (str " + " (- (count columns) 6) " more")))))

(defn- format-endpoint [ep-config]
  {:endpoint  (:endpoint_name ep-config)
   :method    (:http_method ep-config)
   :load-type (:load_type ep-config)
   :watermark (:watermark_column ep-config)
   :key       (first (:primary_key_fields ep-config))
   :table     (:bronze_table_name ep-config)
   :fields    (count (or (:inferred_fields ep-config) []))})

(defn generate-preview
  "Generate a structured preview from a PipelineSpec.
   Returns a map suitable for rendering in the UI."
  [{:keys [pipeline-name target-platform catalog
           bronze-schema silver-schema gold-schema
           bronze-nodes silver-proposals gold-models
           assumptions ops] :as spec}]
  (let [api-node     (first (filter #(= "api-connection" (:node-type %)) bronze-nodes))
        target-node  (first (filter #(= "target" (:node-type %)) bronze-nodes))
        endpoints    (get-in api-node [:config :endpoint_configs] [])
        connection   (get-in target-node [:config :connection_binding :connection_id])]
    {:pipeline-name pipeline-name
     :target        {:platform   target-platform
                     :catalog    catalog
                     :connection connection}

     :bronze
     {:schema    (str catalog "." bronze-schema)
      :endpoints (mapv format-endpoint endpoints)}

     :silver
     {:schema   (str catalog "." silver-schema)
      :entities (mapv (fn [s]
                        {:table        (:target-model s)
                         :entity-kind  (:entity-kind s)
                         :source       (:source-endpoint s)
                         :columns      (format-columns (:columns s))
                         :business-keys (:business-keys s)
                         :dedup-strategy (get-in s [:processing-policy :ordering-strategy])})
                      silver-proposals)}

     :gold
     {:schema (str catalog "." gold-schema)
      :models (mapv (fn [g]
                      {:table      (:target-model g)
                       :grain      (:grain g)
                       :measures   (:measures g)
                       :dimensions (:dimensions g)
                       :depends-on (:depends-on g)})
                    gold-models)}

     :schedule   (:schedule ops)
     :retries    (:retries ops)
     :assumptions assumptions
     :coverage   (:coverage spec)}))

(defn preview-text
  "Generate a plain-text preview for the chat panel."
  [preview]
  (let [lines (transient [])]
    (conj! lines (str "Pipeline: " (:pipeline-name preview)))
    (conj! lines (str "Target: " (get-in preview [:target :platform])
                      " (" (get-in preview [:target :catalog]) " catalog)"
                      (when-let [c (get-in preview [:target :connection])]
                        (str ", connection #" c))))
    (conj! lines "")

    ;; Bronze
    (conj! lines "Bronze Layer:")
    (doseq [ep (get-in preview [:bronze :endpoints])]
      (conj! lines (str "  - " (:endpoint ep) " -> " (get-in preview [:bronze :schema]) "." (:table ep)))
      (conj! lines (str "    " (:fields ep) " fields, watermark: " (:watermark ep) ", key: " (:key ep))))
    (conj! lines "")

    ;; Silver
    (when (seq (get-in preview [:silver :entities]))
      (conj! lines "Silver Layer:")
      (doseq [s (get-in preview [:silver :entities])]
        (conj! lines (str "  - " (:table s) " (" (:entity-kind s) ", " (:columns s) ")"
                          " from " (:source s)))
        (conj! lines (str "    Dedup: " (:dedup-strategy s) ", keys: " (string/join ", " (:business-keys s)))))
      (conj! lines ""))

    ;; Gold
    (when (seq (get-in preview [:gold :models]))
      (conj! lines "Gold Layer:")
      (doseq [g (get-in preview [:gold :models])]
        (conj! lines (str "  - " (:table g) " (" (:grain g) " grain)"))
        (conj! lines (str "    Measures: " (string/join ", " (:measures g))))
        (when (seq (:depends-on g))
          (conj! lines (str "    Depends on: " (string/join ", " (:depends-on g))))))
      (conj! lines ""))

    ;; Ops
    (when (:schedule preview)
      (conj! lines (str "Schedule: " (:schedule preview))))
    (when (:retries preview)
      (conj! lines (str "Retries: " (:retries preview))))
    (conj! lines "")

    ;; Coverage
    (when-let [cov (:coverage preview)]
      (conj! lines "Coverage:")
      (conj! lines (str "  Status: " (:status cov)))
      (when (seq (:required-entities cov))
        (conj! lines (str "  Required: " (string/join ", " (:required-entities cov)))))
      (when (seq (:available-entities cov))
        (conj! lines (str "  Available: " (string/join ", " (:available-entities cov)))))
      (when (seq (:missing-entities cov))
        (conj! lines (str "  Missing: " (string/join ", " (:missing-entities cov)))))
      (doseq [r (:recommendations cov)]
        (conj! lines (str "  Recommendation: " (:message r))))
      (conj! lines ""))

    ;; Assumptions
    (conj! lines "Assumptions:")
    (doseq [a (:assumptions preview)]
      (conj! lines (str "  -> " a)))

    (string/join "\n" (persistent! lines))))
