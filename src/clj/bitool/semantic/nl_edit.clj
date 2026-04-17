(ns bitool.semantic.nl-edit
  "Natural-language model editing for the Semantic Layer.

   Parses structured NL commands to modify semantic models:
   - add/remove entities, columns, measures, hierarchies, relationships
   - rename entities, columns, measures
   - set/update descriptions, grain, entity kind

   The approach is pattern-based (not LLM-dependent) for deterministic,
   auditable changes. Each parsed command returns a delta that can be
   previewed before applying."
  (:require [bitool.semantic.model :as model]
            [bitool.semantic.expression :as expr]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Command parsing
;; ---------------------------------------------------------------------------

(def ^:private command-patterns
  "Ordered list of [regex action extractor-fn]. First match wins."
  [;; Add calculated measure
   [#"(?i)add\s+(?:a\s+)?(?:calculated\s+)?measure\s+['\"]?(\w+)['\"]?\s+(?:as|=|:)\s+['\"]?(.+?)['\"]?\s+(?:on|to|for)\s+['\"]?(\w+)['\"]?"
    :add-calculated-measure
    (fn [m] {:name (nth m 1) :expression (nth m 2) :entity (nth m 3)})]

   ;; Add restricted measure
   [#"(?i)add\s+(?:a\s+)?restricted\s+measure\s+['\"]?(\w+)['\"]?\s+(?:as|=|:)\s+['\"]?(.+?)['\"]?\s+where\s+['\"]?(\w+)['\"]?\s+(?:in|=)\s+['\"]?(.+?)['\"]?\s+(?:on|to|for)\s+['\"]?(\w+)['\"]?"
    :add-restricted-measure
    (fn [m] {:name (nth m 1) :base_measure (nth m 2)
             :filter_column (nth m 3)
             :filter_values (mapv string/trim (string/split (nth m 4) #","))
             :entity (nth m 5)})]

   ;; Remove measure
   [#"(?i)(?:remove|delete|drop)\s+measure\s+['\"]?(\w+)['\"]?"
    :remove-measure
    (fn [m] {:name (nth m 1)})]

   ;; Add hierarchy
   [#"(?i)add\s+(?:a\s+)?hierarchy\s+['\"]?(\w+)['\"]?\s+(?:on|to|for)\s+['\"]?(\w+)['\"]?\s*(?::|as|with\s+levels?)\s+(.+)"
    :add-hierarchy
    (fn [m] {:name (nth m 1) :entity (nth m 2)
             :levels (mapv (fn [l] {:column (string/trim l)})
                           (string/split (nth m 3) #"[,>→]"))})]

   ;; Remove hierarchy
   [#"(?i)(?:remove|delete|drop)\s+hierarchy\s+['\"]?(\w+)['\"]?"
    :remove-hierarchy
    (fn [m] {:name (nth m 1)})]

   ;; Add relationship
   [#"(?i)add\s+(?:a\s+)?(?:relationship|association|join)\s+(?:from\s+)?['\"]?(\w+)['\"]?\.(['\"]?\w+['\"]?)\s*(?:->|→|to)\s*['\"]?(\w+)['\"]?\.(['\"]?\w+['\"]?)"
    :add-relationship
    (fn [m] {:from (nth m 1) :from_column (nth m 2)
             :to (nth m 3) :to_column (nth m 4)
             :type "many_to_one" :join "LEFT"})]

   ;; Remove relationship
   [#"(?i)(?:remove|delete|drop)\s+(?:relationship|association|join)\s+(?:from\s+)?['\"]?(\w+)['\"]?\s*(?:->|→|to)\s*['\"]?(\w+)['\"]?"
    :remove-relationship
    (fn [m] {:from (nth m 1) :to (nth m 2)})]

   ;; Set description on entity
   [#"(?i)(?:set|update|change)\s+description\s+(?:of|on|for)\s+['\"]?(\w+)['\"]?\s+(?:to|as|:)\s+['\"]?(.+?)['\"]?$"
    :set-entity-description
    (fn [m] {:entity (nth m 1) :description (nth m 2)})]

   ;; Rename entity
   [#"(?i)rename\s+entity\s+['\"]?(\w+)['\"]?\s+(?:to|as)\s+['\"]?(\w+)['\"]?"
    :rename-entity
    (fn [m] {:old_name (nth m 1) :new_name (nth m 2)})]

   ;; Rename measure
   [#"(?i)rename\s+measure\s+['\"]?(\w+)['\"]?\s+(?:to|as)\s+['\"]?(\w+)['\"]?"
    :rename-measure
    (fn [m] {:old_name (nth m 1) :new_name (nth m 2)})]

   ;; Mark relationship as preferred
   [#"(?i)(?:prefer|mark\s+as\s+preferred|set\s+preferred)\s+(?:relationship|association|join)\s+(?:from\s+)?['\"]?(\w+)['\"]?\s*(?:->|→|to)\s*['\"]?(\w+)['\"]?"
    :prefer-relationship
    (fn [m] {:from (nth m 1) :to (nth m 2)})]])

(defn parse-command
  "Parse a natural-language model editing command.
   Returns {:action keyword :params map} or nil if unrecognized."
  [text]
  (let [trimmed (string/trim (str text))]
    (some (fn [[pattern action extractor]]
            (when-let [m (re-matches pattern trimmed)]
              {:action action
               :params (extractor m)
               :raw text}))
          command-patterns)))

;; ---------------------------------------------------------------------------
;; Command application — pure model transformations
;; ---------------------------------------------------------------------------

(defmulti apply-command
  "Apply a parsed command to a model-json. Returns {:model updated-model :summary string}
   or throws ExceptionInfo on error."
  (fn [model-json command] (:action command)))

(defmethod apply-command :add-calculated-measure
  [model-json {:keys [params]}]
  (let [{:keys [name expression entity]} params
        measure {:name name :expression expression :entity entity
                 :aggregation (if (expr/classify-aggregation expression nil)
                                (clojure.core/name (expr/classify-aggregation expression nil))
                                "row")}]
    {:model   (update model-json :calculated_measures (fnil conj []) measure)
     :summary (str "Added calculated measure '" name "' = " expression " on " entity)}))

(defmethod apply-command :add-restricted-measure
  [model-json {:keys [params]}]
  (let [{:keys [name base_measure filter_column filter_values entity]} params
        measure {:name name :base_measure base_measure
                 :filter_column filter_column :filter_values filter_values
                 :entity entity}]
    {:model   (update model-json :restricted_measures (fnil conj []) measure)
     :summary (str "Added restricted measure '" name "' = " base_measure
                   " WHERE " filter_column " IN " (pr-str filter_values) " on " entity)}))

(defmethod apply-command :remove-measure
  [model-json {:keys [params]}]
  (let [{:keys [name]} params]
    {:model   (-> model-json
                  (update :calculated_measures
                          (fn [ms] (vec (remove #(= name (:name %)) (or ms [])))))
                  (update :restricted_measures
                          (fn [ms] (vec (remove #(= name (:name %)) (or ms []))))))
     :summary (str "Removed measure '" name "'")}))

(defmethod apply-command :add-hierarchy
  [model-json {:keys [params]}]
  (let [{:keys [name entity levels]} params]
    {:model   (update model-json :hierarchies (fnil conj []) {:name name :entity entity :levels levels})
     :summary (str "Added hierarchy '" name "' on " entity ": "
                   (string/join " → " (map :column levels)))}))

(defmethod apply-command :remove-hierarchy
  [model-json {:keys [params]}]
  (let [{:keys [name]} params]
    {:model   (update model-json :hierarchies
                      (fn [hs] (vec (remove #(= name (:name %)) (or hs [])))))
     :summary (str "Removed hierarchy '" name "'")}))

(defmethod apply-command :add-relationship
  [model-json {:keys [params]}]
  {:model   (update model-json :relationships (fnil conj []) params)
   :summary (str "Added relationship " (:from params) "." (:from_column params)
                 " → " (:to params) "." (:to_column params))})

(defmethod apply-command :remove-relationship
  [model-json {:keys [params]}]
  (let [{:keys [from to]} params]
    {:model   (update model-json :relationships
                      (fn [rels]
                        (vec (remove #(and (= from (:from %)) (= to (:to %)))
                                     (or rels [])))))
     :summary (str "Removed relationship " from " → " to)}))

(defmethod apply-command :set-entity-description
  [model-json {:keys [params]}]
  (let [{:keys [entity description]} params
        ek (keyword entity)]
    (if (get-in model-json [:entities ek])
      {:model   (assoc-in model-json [:entities ek :description] description)
       :summary (str "Set description of '" entity "' to: " description)}
      ;; Try string key
      (if (get-in model-json [:entities entity])
        {:model   (assoc-in model-json [:entities entity :description] description)
         :summary (str "Set description of '" entity "' to: " description)}
        (throw (ex-info (str "Entity not found: " entity)
                        {:entity entity :action :set-entity-description}))))))

(defmethod apply-command :rename-entity
  [model-json {:keys [params]}]
  (let [{:keys [old_name new_name]} params
        old-key (or (when (get-in model-json [:entities (keyword old_name)]) (keyword old_name))
                    (when (get-in model-json [:entities old_name]) old_name))]
    (when-not old-key
      (throw (ex-info (str "Entity not found: " old_name)
                      {:entity old_name :action :rename-entity})))
    (let [entity (get-in model-json [:entities old-key])]
      {:model   (-> model-json
                    (update :entities dissoc old-key)
                    (assoc-in [:entities (keyword new_name)] entity)
                    ;; Update references in measures, hierarchies, relationships
                    (update :calculated_measures
                            (fn [ms] (mapv #(if (= old_name (:entity %))
                                              (assoc % :entity new_name) %) (or ms []))))
                    (update :restricted_measures
                            (fn [ms] (mapv #(if (= old_name (:entity %))
                                              (assoc % :entity new_name) %) (or ms []))))
                    (update :hierarchies
                            (fn [hs] (mapv #(if (= old_name (:entity %))
                                              (assoc % :entity new_name) %) (or hs []))))
                    (update :relationships
                            (fn [rels] (mapv (fn [r]
                                              (cond-> r
                                                (= old_name (:from r)) (assoc :from new_name)
                                                (= old_name (:to r))   (assoc :to new_name)))
                                             (or rels [])))))
       :summary (str "Renamed entity '" old_name "' to '" new_name "'")})))

(defmethod apply-command :rename-measure
  [model-json {:keys [params]}]
  (let [{:keys [old_name new_name]} params]
    {:model   (-> model-json
                  (update :calculated_measures
                          (fn [ms] (mapv #(if (= old_name (:name %))
                                            (assoc % :name new_name) %) (or ms []))))
                  (update :restricted_measures
                          (fn [ms] (mapv #(if (= old_name (:name %))
                                            (assoc % :name new_name) %) (or ms [])))))
     :summary (str "Renamed measure '" old_name "' to '" new_name "'")}))

(defmethod apply-command :prefer-relationship
  [model-json {:keys [params]}]
  (let [{:keys [from to]} params]
    {:model   (update model-json :relationships
                      (fn [rels]
                        (mapv (fn [r]
                                (if (and (= from (:from r)) (= to (:to r)))
                                  (assoc r :preferred true)
                                  r))
                              (or rels []))))
     :summary (str "Marked relationship " from " → " to " as preferred")}))

(defmethod apply-command :default
  [_ command]
  (throw (ex-info (str "Unknown command action: " (:action command))
                  {:action (:action command)})))

;; ---------------------------------------------------------------------------
;; High-level API
;; ---------------------------------------------------------------------------

(defn execute-nl-edit
  "Parse and execute a natural-language model edit command.
   Returns {:model updated-model :summary string :action keyword}
   or {:error string} if the command is unrecognized."
  [model-json command-text]
  (if-let [command (parse-command command-text)]
    (let [result (apply-command model-json command)]
      (assoc result :action (:action command)))
    {:error (str "Could not parse command: " command-text)
     :hint "Try commands like: add measure cost_per_mile as fuel_cost / NULLIF(miles, 0) on trips"}))

(defn execute-and-validate
  "Parse, execute, and validate a natural-language model edit.
   Returns the result of execute-nl-edit with an additional :validation key.
   If validation fails, the model is NOT persisted (caller decides)."
  [model-json command-text]
  (let [result (execute-nl-edit model-json command-text)]
    (if (:error result)
      result
      (let [validation (model/validate-semantic-model (:model result))]
        (assoc result :validation validation)))))
