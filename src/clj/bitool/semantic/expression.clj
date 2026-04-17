(ns bitool.semantic.expression
  "Expression parser, validator, and dependency resolver for calculated measures.

   Expression language (subset of SQL scalar expressions):
   - Arithmetic: +, -, *, /
   - Functions: NULLIF, COALESCE, ABS, ROUND, CAST
   - Aggregates: SUM, COUNT, AVG, MIN, MAX (only in post-aggregate measures)
   - References: bare column names resolving to entity columns or other measures
   - No subqueries, no window functions, no cross-entity column refs"
  (:require [clojure.string :as string]
            [clojure.set :as cset]))

;; ---------------------------------------------------------------------------
;; Tokenizer
;; ---------------------------------------------------------------------------

(def ^:private allowed-functions
  #{"NULLIF" "COALESCE" "ABS" "ROUND" "CAST"})

(def ^:private allowed-aggregates
  #{"SUM" "COUNT" "AVG" "MIN" "MAX"})

(def ^:private allowed-ops
  #{:plus :minus :star :slash})

(defn- tokenize
  "Simple tokenizer for SQL scalar expressions.
   Returns a vector of tokens: {:type :ident/:number/:string/:op/:lparen/:rparen/:comma/:fn/:agg, :value ...}"
  [expr-str]
  (let [s      (string/trim (or expr-str ""))
        tokens (atom [])
        i      (atom 0)
        len    (count s)]
    (while (< @i len)
      (let [ch (.charAt s @i)]
        (cond
          ;; Whitespace — skip
          (Character/isWhitespace ch)
          (swap! i inc)

          ;; Number literal (int or decimal)
          (or (Character/isDigit ch)
              (and (= ch \.) (< (inc @i) len) (Character/isDigit (.charAt s (inc @i)))))
          (let [start @i]
            (while (and (< @i len)
                        (let [c (.charAt s @i)]
                          (or (Character/isDigit c) (= c \.))))
              (swap! i inc))
            (swap! tokens conj {:type :number :value (subs s start @i)}))

          ;; String literal
          (= ch \')
          (let [start @i]
            (swap! i inc)
            (while (and (< @i len) (not= (.charAt s @i) \'))
              (swap! i inc))
            (when (< @i len) (swap! i inc)) ; consume closing quote
            (swap! tokens conj {:type :string :value (subs s start @i)}))

          ;; Identifier or function/aggregate
          (or (Character/isLetter ch) (= ch \_))
          (let [start @i]
            (while (and (< @i len)
                        (let [c (.charAt s @i)]
                          (or (Character/isLetterOrDigit c) (= c \_) (= c \.))))
              (swap! i inc))
            (let [word (subs s start @i)
                  upper (string/upper-case word)]
              (cond
                (contains? allowed-aggregates upper)
                (swap! tokens conj {:type :agg :value upper})

                (contains? allowed-functions upper)
                (swap! tokens conj {:type :fn :value upper})

                (= upper "AS")
                (swap! tokens conj {:type :keyword :value "AS"})

                :else
                (swap! tokens conj {:type :ident :value word}))))

          ;; Operators
          (= ch \+) (do (swap! tokens conj {:type :op :value :plus})   (swap! i inc))
          (= ch \-) (do (swap! tokens conj {:type :op :value :minus})  (swap! i inc))
          (= ch \*) (do (swap! tokens conj {:type :op :value :star})   (swap! i inc))
          (= ch \/) (do (swap! tokens conj {:type :op :value :slash})  (swap! i inc))

          ;; Parens / comma
          (= ch \() (do (swap! tokens conj {:type :lparen}) (swap! i inc))
          (= ch \)) (do (swap! tokens conj {:type :rparen}) (swap! i inc))
          (= ch \,) (do (swap! tokens conj {:type :comma})  (swap! i inc))

          ;; Unknown char — skip
          :else (swap! i inc))))
    @tokens))

;; ---------------------------------------------------------------------------
;; Reference extraction
;; ---------------------------------------------------------------------------

(defn extract-column-refs
  "Extract all bare column name references from a calculated measure expression.
   Returns a set of lowercase column/measure names."
  [expr-str]
  (let [tokens (tokenize expr-str)]
    (->> tokens
         (filter #(= :ident (:type %)))
         (map #(string/lower-case (:value %)))
         set)))

(defn extract-aggregate-refs
  "Extract aggregate function names used in the expression."
  [expr-str]
  (let [tokens (tokenize expr-str)]
    (->> tokens
         (filter #(= :agg (:type %)))
         (map :value)
         set)))

(defn classify-aggregation
  "Determine whether a measure expression is row-level or post-aggregate.
   - If expression contains aggregate functions (SUM, COUNT, etc.) → post
   - Otherwise → row (default)"
  [expr-str explicit-aggregation]
  (cond
    ;; Explicit override
    (= "post" explicit-aggregation) "post"
    (= "row" explicit-aggregation) "row"
    ;; Auto-detect
    (seq (extract-aggregate-refs expr-str)) "post"
    :else "row"))

;; ---------------------------------------------------------------------------
;; Validation
;; ---------------------------------------------------------------------------

(defn validate-expression
  "Validate a calculated measure expression against available columns.
   Returns {:valid true} or {:valid false :errors [...]}.

   Parameters:
   - expr-str: the expression string
   - entity-columns: set of column names available on the entity
   - measure-names: set of other calculated measure names on the same entity
   - aggregation: \"row\" or \"post\""
  [expr-str entity-columns measure-names aggregation]
  (let [errors   (atom [])
        tokens   (tokenize expr-str)]
    ;; Check for empty expression
    (when (empty? tokens)
      (swap! errors conj "Expression is empty"))

    ;; Check balanced parens
    (let [depth (reduce (fn [d tok]
                          (case (:type tok)
                            :lparen (inc d)
                            :rparen (dec d)
                            d))
                        0 tokens)]
      (when (not= 0 depth)
        (swap! errors conj "Unbalanced parentheses")))

    ;; Check column refs resolve
    (let [col-refs      (extract-column-refs expr-str)
          available     (cset/union (set (map string/lower-case entity-columns))
                                   (set (map string/lower-case measure-names)))
          unresolved    (cset/difference col-refs available)]
      (when (seq unresolved)
        (swap! errors conj (str "Unknown column references: " (string/join ", " (sort unresolved))))))

    ;; Row-level measures must not use aggregate functions
    (when (= "row" aggregation)
      (let [aggs (extract-aggregate-refs expr-str)]
        (when (seq aggs)
          (swap! errors conj (str "Row-level measure cannot use aggregate functions: "
                                  (string/join ", " aggs)
                                  ". Set aggregation to \"post\" or remove aggregates.")))))

    ;; Check for disallowed constructs
    (let [lower (string/lower-case (or expr-str ""))]
      (when (re-find #"\bselect\b" lower)
        (swap! errors conj "Subqueries are not allowed in calculated measure expressions"))
      (when (re-find #"\bover\s*\(" lower)
        (swap! errors conj "Window functions (OVER) are not allowed in calculated measure expressions")))

    (if (empty? @errors)
      {:valid true}
      {:valid false :errors @errors})))

;; ---------------------------------------------------------------------------
;; Dependency graph — topological sort with cycle detection
;; ---------------------------------------------------------------------------

(defn build-dependency-graph
  "Build a dependency graph for calculated measures.
   Returns {measure-name #{dependent-measure-names}}.
   Only includes dependencies on OTHER calculated measures, not base columns."
  [calculated-measures]
  (let [measure-names (set (map :name calculated-measures))]
    (into {}
          (map (fn [m]
                 (let [refs (extract-column-refs (:expression m))
                       deps (cset/intersection refs measure-names)]
                   [(:name m) deps])))
          calculated-measures)))

(defn topological-sort
  "Topologically sort calculated measures by dependency order.
   Returns {:sorted [measure-names-in-order]} or {:cycle [cycle-path]}.

   Uses Kahn's algorithm for deterministic ordering."
  [dep-graph]
  (let [;; Compute in-degree for each node
        all-nodes  (set (keys dep-graph))
        in-degree  (atom (into {} (map (fn [n] [n 0]) all-nodes)))
        ;; Build adjacency: if A depends on B, then B → A (B must come first)
        adj        (atom (into {} (map (fn [n] [n #{}]) all-nodes)))]
    ;; For each measure M that depends on dep D: D → M edge
    (doseq [[m deps] dep-graph
            d deps]
      (when (contains? all-nodes d)
        (swap! adj update d (fnil conj #{}) m)
        (swap! in-degree update m inc)))

    (let [queue  (atom (into (sorted-set) (filter #(zero? (get @in-degree %)) all-nodes)))
          result (atom [])]
      (while (seq @queue)
        (let [n (first @queue)]
          (swap! queue disj n)
          (swap! result conj n)
          (doseq [neighbor (get @adj n)]
            (swap! in-degree update neighbor dec)
            (when (zero? (get @in-degree neighbor))
              (swap! queue conj neighbor)))))

      (if (= (count @result) (count all-nodes))
        {:sorted @result}
        ;; Cycle detected — find a cycle for error reporting
        (let [remaining (cset/difference all-nodes (set @result))
              ;; Walk from any remaining node to find cycle
              start     (first remaining)
              visited   (atom #{})
              path      (atom [start])]
          (loop [curr start]
            (swap! visited conj curr)
            (let [next-dep (first (cset/intersection (get dep-graph curr #{}) remaining))]
              (if (or (nil? next-dep) (contains? @visited next-dep))
                (do
                  (when next-dep (swap! path conj next-dep))
                  {:cycle @path})
                (do
                  (swap! path conj next-dep)
                  (recur next-dep))))))))))

;; ---------------------------------------------------------------------------
;; Full validation of all calculated measures on a model
;; ---------------------------------------------------------------------------

(defn validate-calculated-measures
  "Validate all calculated measures for a semantic model entity.
   Checks:
   1. Each expression is syntactically valid
   2. All column refs resolve to entity columns or other measures
   3. No dependency cycles
   4. Row-level measures don't use aggregates

   Returns {:valid true :sorted [ordered-names]}
   or {:valid false :errors [...]}."
  [calculated-measures entity-columns]
  (let [errors        (atom [])
        measure-names (set (map :name calculated-measures))
        col-names     (set (map :name entity-columns))]
    ;; Validate each expression
    (doseq [m calculated-measures]
      (let [aggregation (classify-aggregation (:expression m) (:aggregation m))
            result (validate-expression
                    (:expression m)
                    col-names
                    (disj measure-names (:name m)) ;; can reference other measures, not self
                    aggregation)]
        (when-not (:valid result)
          (doseq [e (:errors result)]
            (swap! errors conj (str "Measure '" (:name m) "': " e))))))

    ;; Check for duplicate names
    (let [dupes (->> calculated-measures
                     (map :name)
                     frequencies
                     (filter (fn [[_ cnt]] (> cnt 1)))
                     (map first))]
      (when (seq dupes)
        (swap! errors conj (str "Duplicate measure names: " (string/join ", " dupes)))))

    ;; Check names don't shadow entity columns
    (let [shadows (cset/intersection measure-names col-names)]
      (when (seq shadows)
        (swap! errors conj (str "Calculated measures shadow entity columns: "
                                (string/join ", " (sort shadows))))))

    ;; Topological sort for dependency ordering
    (let [dep-graph (build-dependency-graph calculated-measures)
          topo      (topological-sort dep-graph)]
      (if (:cycle topo)
        (do
          (swap! errors conj (str "Circular dependency detected: "
                                  (string/join " → " (:cycle topo))))
          {:valid false :errors @errors})
        (if (seq @errors)
          {:valid false :errors @errors}
          {:valid true :sorted (:sorted topo)})))))

;; ---------------------------------------------------------------------------
;; Restricted measures
;; ---------------------------------------------------------------------------

(defn validate-restricted-measure
  "Validate a restricted measure definition.
   A restricted measure is: base_measure + filter predicate.
   Cross-entity filters require :via_relationship.

   Returns {:valid true} or {:valid false :errors [...]}"
  [restricted-measure entity-name entity-columns relationships]
  (let [errors (atom [])]
    ;; Check base_measure exists
    (when (string/blank? (:base_measure restricted-measure))
      (swap! errors conj "base_measure is required"))

    ;; Check filter column exists
    (let [filter-col (:filter_column restricted-measure)
          filter-entity (:entity restricted-measure)
          col-names (set (map (comp string/lower-case :name) entity-columns))]
      (when (string/blank? filter-col)
        (swap! errors conj "filter_column is required"))

      ;; Verify filter_column resolves to a real column on the (local) entity
      (when (and (not (string/blank? filter-col))
                 (or (nil? filter-entity) (= filter-entity entity-name))
                 (not (contains? col-names (string/lower-case filter-col))))
        (swap! errors conj
               (str "filter_column '" filter-col "' is not a column on entity '" entity-name "'")))

      ;; Verify base_measure inner column resolves (e.g. SUM(revenue) → revenue must exist)
      (let [base (:base_measure restricted-measure)]
        (when (and (not (string/blank? base))
                   (re-find #"(?i)(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\w+)\s*\)" base))
          (let [[_ _ inner-col] (re-find #"(?i)(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\w+)\s*\)" base)]
            (when (and inner-col
                       (not (contains? col-names (string/lower-case inner-col))))
              (swap! errors conj
                     (str "base_measure references column '" inner-col
                          "' which is not found on entity '" entity-name "'"))))))

      ;; Cross-entity filter requires via_relationship
      (when (and filter-entity (not= filter-entity entity-name))
        (let [via (:via_relationship restricted-measure)]
          (when (string/blank? via)
            (swap! errors conj
                   (str "Cross-entity filter on '" filter-entity "' requires :via_relationship")))
          (when (and via (not-any? #(or (and (= (:from %) entity-name) (= (:to %) filter-entity))
                                        (and (= (:to %) entity-name) (= (:from %) filter-entity)))
                                   relationships))
            (swap! errors conj
                   (str "No relationship found between '" entity-name "' and '" filter-entity "'"))))))

    ;; Check filter values
    (when (empty? (:filter_values restricted-measure))
      (swap! errors conj "filter_values must be a non-empty list"))

    (if (empty? @errors)
      {:valid true}
      {:valid false :errors @errors})))

;; ---------------------------------------------------------------------------
;; Hierarchies
;; ---------------------------------------------------------------------------

(defn validate-hierarchy
  "Validate a hierarchy definition on a dimension entity.
   A hierarchy is an ordered list of column levels for drill-down.

   Returns {:valid true} or {:valid false :errors [...]}"
  [hierarchy entity-columns]
  (let [errors    (atom [])
        levels    (:levels hierarchy)
        col-names (set (map :name entity-columns))]
    (when (string/blank? (:name hierarchy))
      (swap! errors conj "Hierarchy name is required"))

    (when (or (nil? levels) (< (count levels) 2))
      (swap! errors conj "Hierarchy must have at least 2 levels"))

    (doseq [level (or levels [])]
      (let [col (:column level)]
        (when-not (contains? col-names col)
          (swap! errors conj (str "Hierarchy level column '" col "' not found on entity")))))

    ;; Check for duplicate levels
    (let [level-cols (map :column (or levels []))
          dupes (->> level-cols frequencies (filter (fn [[_ c]] (> c 1))) (map first))]
      (when (seq dupes)
        (swap! errors conj (str "Duplicate hierarchy levels: " (string/join ", " dupes)))))

    (if (empty? @errors)
      {:valid true}
      {:valid false :errors @errors})))

;; ---------------------------------------------------------------------------
;; Query-time expansion
;; ---------------------------------------------------------------------------

(defn expand-row-level-measure
  "Expand a row-level calculated measure into a SQL column alias expression.
   E.g., {:name \"cost_per_mile\" :expression \"fuel_cost / NULLIF(miles, 0)\"}
   → \"(fuel_cost / NULLIF(miles, 0)) AS cost_per_mile\""
  [measure]
  (str "(" (:expression measure) ") AS " (:name measure)))

(defn expand-post-aggregate-measure
  "Expand a post-aggregate calculated measure into a SQL expression.
   The expression already contains aggregate functions.
   E.g., {:name \"avg_cost_per_mile\" :expression \"SUM(fuel_cost) / SUM(miles)\"}
   → \"(SUM(fuel_cost) / SUM(miles)) AS avg_cost_per_mile\""
  [measure]
  (str "(" (:expression measure) ") AS " (:name measure)))

(defn expand-restricted-measure
  "Expand a restricted measure into a filtered aggregate SQL expression.
   E.g., {:name \"emea_revenue\" :base_measure \"SUM(revenue)\"
          :filter_column \"region\" :filter_values [\"EMEA\"]}
   → \"SUM(CASE WHEN region IN ('EMEA') THEN revenue END) AS emea_revenue\""
  [restricted-measure]
  (let [{:keys [name base_measure filter_column filter_values]} restricted-measure
        ;; Extract the aggregate function and inner column from base_measure
        ;; Handles patterns like "SUM(revenue)" or "COUNT(order_id)"
        [_ agg-fn inner-col] (re-find #"(?i)(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\w+)\s*\)"
                                       (or base_measure ""))
        quoted-vals (string/join ", " (map #(str "'" (string/replace (str %) "'" "''") "'")
                                          (or filter_values [])))]
    (if (and agg-fn inner-col)
      (str (string/upper-case agg-fn)
           "(CASE WHEN " filter_column " IN (" quoted-vals ") THEN " inner-col " END)"
           " AS " name)
      ;; Fallback — wrap the whole base_measure with a CASE
      (str "CASE WHEN " filter_column " IN (" quoted-vals ") THEN " base_measure " END"
           " AS " name))))

(defn resolve-measures-for-query
  "Given a list of requested measure names and the model's calculated/restricted measures,
   return ordered SQL expressions ready for SELECT injection.

   Parameters:
   - requested-names: set of measure names the query references
   - calculated-measures: vector of calculated measure defs from model
   - restricted-measures: vector of restricted measure defs from model
   - sorted-order: topological order from validate-calculated-measures

   Returns {:select-exprs [sql-strings] :group-by-additions [col-names]}"
  [requested-names calculated-measures restricted-measures sorted-order]
  (let [calc-by-name   (into {} (map (fn [m] [(:name m) m]) calculated-measures))
        restr-by-name  (into {} (map (fn [m] [(:name m) m]) restricted-measures))
        ;; Resolve calculated measures in dependency order
        calc-exprs     (->> (or sorted-order [])
                            (filter #(contains? requested-names %))
                            (mapv (fn [mname]
                                    (let [m   (get calc-by-name mname)
                                          agg (classify-aggregation (:expression m) (:aggregation m))]
                                      (if (= "post" agg)
                                        (expand-post-aggregate-measure m)
                                        (expand-row-level-measure m))))))
        ;; Resolve restricted measures
        restr-exprs    (->> restricted-measures
                            (filter #(contains? requested-names (:name %)))
                            (mapv expand-restricted-measure))]
    {:select-exprs (into calc-exprs restr-exprs)
     :group-by-additions []}))
