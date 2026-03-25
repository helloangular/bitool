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

(defn- non-blank-str
  [value]
  (let [value (some-> value str string/trim)]
    (when (seq value) value)))

(defn- parse-nonnegative-long
  [value]
  (try
    (let [parsed (cond
                   (nil? value) nil
                   (number? value) (long value)
                   :else (Long/parseLong (str value)))]
      (when (and (some? parsed) (not (neg? parsed)))
        parsed))
    (catch Exception _
      nil)))

(defn- normalize-duration-window
  [window]
  (when (map? window)
    (let [value (parse-nonnegative-long (:value window))
          unit  (some-> (:unit window) non-blank-str string/lower-case)]
      (cond-> {}
        (some? value) (assoc :value value)
        unit (assoc :unit unit)))))

(defn- normalize-processing-policy
  [proposal-json]
  (when (map? (:processing_policy proposal-json))
    (let [policy (:processing_policy proposal-json)
          business-keys (->> (or (:business_keys policy) [])
                             (keep non-blank-str)
                             vec)
          event-time-column (some-> (:event_time_column policy) non-blank-str)
          sequence-column   (some-> (:sequence_column policy) non-blank-str)
          ordering-strategy (some-> (:ordering_strategy policy) non-blank-str string/lower-case)
          late-data-mode    (some-> (:late_data_mode policy) non-blank-str string/lower-case)
          too-late-behavior (some-> (:too_late_behavior policy) non-blank-str string/lower-case)
          late-data-tolerance (normalize-duration-window (:late_data_tolerance policy))
          reprocess-window    (normalize-duration-window (:reprocess_window policy))]
      (cond-> {}
        (seq business-keys) (assoc :business_keys business-keys)
        event-time-column (assoc :event_time_column event-time-column)
        sequence-column (assoc :sequence_column sequence-column)
        ordering-strategy (assoc :ordering_strategy ordering-strategy)
        late-data-mode (assoc :late_data_mode late-data-mode)
        too-late-behavior (assoc :too_late_behavior too-late-behavior)
        (seq late-data-tolerance) (assoc :late_data_tolerance late-data-tolerance)
        (seq reprocess-window) (assoc :reprocess_window reprocess-window)))))

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
     :source_expansion (:source_expansion proposal-json)
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
     :processing_policy (normalize-processing-policy proposal-json)
     :materialization {:mode (get-in proposal-json [:materialization :mode])
                       :target (:target_table proposal-json)
                       :keys (vec (or (get-in proposal-json [:materialization :keys]) []))}
     :target_warehouse (or (:target_warehouse proposal-json)
                           (:target_kind proposal-json)
                           "databricks")}))
