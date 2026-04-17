(ns bitool.semantic-expression-test
  (:require [clojure.test :refer :all]
            [bitool.semantic.expression :as expr]))

;; ---------------------------------------------------------------------------
;; Tokenizer & extraction
;; ---------------------------------------------------------------------------

(deftest extract-column-refs-basic
  (is (= #{"fuel_cost" "miles"} (expr/extract-column-refs "fuel_cost / NULLIF(miles, 0)")))
  (is (= #{"a" "b" "c"} (expr/extract-column-refs "a + b * c")))
  (is (= #{} (expr/extract-column-refs "42 + 3.14")))
  (is (= #{} (expr/extract-column-refs "")))
  (is (= #{} (expr/extract-column-refs nil))))

(deftest extract-column-refs-ignores-functions-and-aggregates
  ;; SUM, COUNT etc. should NOT appear as column refs
  (is (= #{"revenue"} (expr/extract-column-refs "SUM(revenue)")))
  (is (= #{"x"} (expr/extract-column-refs "ABS(x)")))
  (is (= #{"a" "b"} (expr/extract-column-refs "COALESCE(a, b)"))))

(deftest extract-aggregate-refs-basic
  (is (= #{"SUM"} (expr/extract-aggregate-refs "SUM(revenue)")))
  (is (= #{"SUM" "COUNT"} (expr/extract-aggregate-refs "SUM(x) / COUNT(y)")))
  (is (= #{} (expr/extract-aggregate-refs "fuel_cost / miles")))
  (is (= #{} (expr/extract-aggregate-refs ""))))

;; ---------------------------------------------------------------------------
;; Classification
;; ---------------------------------------------------------------------------

(deftest classify-aggregation-explicit-overrides
  (is (= "post" (expr/classify-aggregation "a + b" "post")))
  (is (= "row" (expr/classify-aggregation "SUM(x)" "row"))))

(deftest classify-aggregation-auto-detect
  (is (= "post" (expr/classify-aggregation "SUM(fuel_cost) / SUM(miles)" nil)))
  (is (= "row" (expr/classify-aggregation "fuel_cost / NULLIF(miles, 0)" nil))))

;; ---------------------------------------------------------------------------
;; Expression validation
;; ---------------------------------------------------------------------------

(deftest validate-expression-valid-row-level
  (let [result (expr/validate-expression
                "fuel_cost / NULLIF(miles, 0)"
                #{"fuel_cost" "miles"}
                #{}
                "row")]
    (is (:valid result))))

(deftest validate-expression-valid-post-aggregate
  (let [result (expr/validate-expression
                "SUM(fuel_cost) / SUM(miles)"
                #{"fuel_cost" "miles"}
                #{}
                "post")]
    (is (:valid result))))

(deftest validate-expression-valid-with-measure-refs
  (let [result (expr/validate-expression
                "cost_per_mile * 100"
                #{"fuel_cost" "miles"}
                #{"cost_per_mile"}
                "row")]
    (is (:valid result))))

(deftest validate-expression-empty
  (let [result (expr/validate-expression "" #{} #{} "row")]
    (is (not (:valid result)))
    (is (some #(.contains % "empty") (:errors result)))))

(deftest validate-expression-unknown-column
  (let [result (expr/validate-expression
                "unknown_col + 1"
                #{"fuel_cost" "miles"}
                #{}
                "row")]
    (is (not (:valid result)))
    (is (some #(.contains % "Unknown column") (:errors result)))))

(deftest validate-expression-unbalanced-parens
  (let [result (expr/validate-expression
                "((a + b)"
                #{"a" "b"}
                #{}
                "row")]
    (is (not (:valid result)))
    (is (some #(.contains % "Unbalanced parentheses") (:errors result)))))

(deftest validate-expression-row-level-no-aggregates
  (let [result (expr/validate-expression
                "SUM(fuel_cost) / miles"
                #{"fuel_cost" "miles"}
                #{}
                "row")]
    (is (not (:valid result)))
    (is (some #(.contains % "Row-level measure cannot use aggregate") (:errors result)))))

(deftest validate-expression-subquery-rejected
  (let [result (expr/validate-expression
                "price * (select max(discount) from discounts)"
                #{"price"}
                #{}
                "row")]
    (is (not (:valid result)))
    (is (some #(.contains % "Subqueries") (:errors result)))))

(deftest validate-expression-window-function-rejected
  (let [result (expr/validate-expression
                "revenue - LAG(revenue) over (order by month)"
                #{"revenue" "month"}
                #{}
                "row")]
    (is (not (:valid result)))
    (is (some #(.contains % "Window functions") (:errors result)))))

;; ---------------------------------------------------------------------------
;; Dependency graph & topological sort
;; ---------------------------------------------------------------------------

(deftest build-dependency-graph-no-deps
  (let [measures [{:name "cost_per_mile" :expression "fuel_cost / miles"}
                  {:name "total_dist" :expression "SUM(distance)"}]
        graph (expr/build-dependency-graph measures)]
    (is (= #{}  (get graph "cost_per_mile")))
    (is (= #{}  (get graph "total_dist")))))

(deftest build-dependency-graph-with-deps
  (let [measures [{:name "cost_per_mile" :expression "fuel_cost / NULLIF(miles, 0)"}
                  {:name "cost_index" :expression "cost_per_mile * 100"}]
        graph (expr/build-dependency-graph measures)]
    (is (= #{}              (get graph "cost_per_mile")))
    (is (= #{"cost_per_mile"} (get graph "cost_index")))))

(deftest topological-sort-linear-chain
  (let [graph {"a" #{}
               "b" #{"a"}
               "c" #{"b"}}
        result (expr/topological-sort graph)]
    (is (:sorted result))
    ;; a before b before c
    (let [order (:sorted result)]
      (is (< (.indexOf order "a") (.indexOf order "b")))
      (is (< (.indexOf order "b") (.indexOf order "c"))))))

(deftest topological-sort-independent-nodes
  (let [graph {"x" #{}
               "y" #{}
               "z" #{}}
        result (expr/topological-sort graph)]
    (is (:sorted result))
    (is (= 3 (count (:sorted result))))))

(deftest topological-sort-detects-cycle
  (let [graph {"a" #{"b"}
               "b" #{"c"}
               "c" #{"a"}}
        result (expr/topological-sort graph)]
    (is (:cycle result))
    (is (nil? (:sorted result)))))

(deftest topological-sort-detects-self-reference
  (let [graph {"a" #{"a"}}
        result (expr/topological-sort graph)]
    (is (:cycle result))))

;; ---------------------------------------------------------------------------
;; validate-calculated-measures (integration)
;; ---------------------------------------------------------------------------

(deftest validate-calculated-measures-valid
  (let [measures [{:name "cost_per_mile"
                   :expression "fuel_cost / NULLIF(miles, 0)"
                   :aggregation "row"}
                  {:name "cost_index"
                   :expression "cost_per_mile * 100"
                   :aggregation "row"}]
        columns [{:name "fuel_cost"} {:name "miles"}]
        result (expr/validate-calculated-measures measures columns)]
    (is (:valid result))
    (is (vector? (:sorted result)))
    ;; cost_per_mile must come before cost_index
    (let [order (:sorted result)]
      (is (< (.indexOf order "cost_per_mile") (.indexOf order "cost_index"))))))

(deftest validate-calculated-measures-cycle-error
  (let [measures [{:name "a" :expression "b + 1"}
                  {:name "b" :expression "a + 1"}]
        columns []
        result (expr/validate-calculated-measures measures columns)]
    (is (not (:valid result)))
    (is (some #(.contains % "Circular dependency") (:errors result)))))

(deftest validate-calculated-measures-duplicate-names
  (let [measures [{:name "m1" :expression "fuel_cost + 1"}
                  {:name "m1" :expression "fuel_cost + 2"}]
        columns [{:name "fuel_cost"}]
        result (expr/validate-calculated-measures measures columns)]
    (is (not (:valid result)))
    (is (some #(.contains % "Duplicate measure names") (:errors result)))))

(deftest validate-calculated-measures-shadows-column
  (let [measures [{:name "fuel_cost" :expression "fuel_cost * 1.1"}]
        columns [{:name "fuel_cost"}]
        result (expr/validate-calculated-measures measures columns)]
    (is (not (:valid result)))
    (is (some #(.contains % "shadow entity columns") (:errors result)))))

;; ---------------------------------------------------------------------------
;; Restricted measures
;; ---------------------------------------------------------------------------

(deftest validate-restricted-measure-valid
  (let [rm {:base_measure "SUM(revenue)"
            :filter_column "region"
            :filter_values ["EMEA"]}
        cols [{:name "revenue"} {:name "region"}]
        result (expr/validate-restricted-measure rm "orders" cols [])]
    (is (:valid result))))

(deftest validate-restricted-measure-missing-base
  (let [rm {:filter_column "region" :filter_values ["US"]}
        result (expr/validate-restricted-measure rm "orders" [{:name "region"}] [])]
    (is (not (:valid result)))
    (is (some #(.contains % "base_measure") (:errors result)))))

(deftest validate-restricted-measure-missing-filter-column
  (let [rm {:base_measure "SUM(revenue)" :filter_values ["US"]}
        result (expr/validate-restricted-measure rm "orders" [{:name "revenue"}] [])]
    (is (not (:valid result)))
    (is (some #(.contains % "filter_column") (:errors result)))))

(deftest validate-restricted-measure-missing-filter-values
  (let [rm {:base_measure "SUM(revenue)" :filter_column "region" :filter_values []}
        result (expr/validate-restricted-measure rm "orders" [{:name "revenue"} {:name "region"}] [])]
    (is (not (:valid result)))
    (is (some #(.contains % "filter_values") (:errors result)))))

(deftest validate-restricted-measure-filter-column-not-on-entity
  (let [rm {:base_measure "SUM(revenue)"
            :filter_column "nonexistent_col"
            :filter_values ["US"]}
        cols [{:name "revenue"} {:name "region"}]
        result (expr/validate-restricted-measure rm "orders" cols [])]
    (is (not (:valid result)))
    (is (some #(.contains % "not a column on entity") (:errors result)))))

(deftest validate-restricted-measure-base-measure-inner-col-not-on-entity
  (let [rm {:base_measure "SUM(nonexistent)"
            :filter_column "region"
            :filter_values ["US"]}
        cols [{:name "revenue"} {:name "region"}]
        result (expr/validate-restricted-measure rm "orders" cols [])]
    (is (not (:valid result)))
    (is (some #(.contains % "base_measure references column") (:errors result)))))

(deftest validate-restricted-measure-cross-entity-requires-via
  (let [rm {:base_measure "SUM(revenue)"
            :filter_column "country"
            :filter_values ["US"]
            :entity "customers"}
        result (expr/validate-restricted-measure rm "orders" [{:name "revenue"}] [])]
    (is (not (:valid result)))
    (is (some #(.contains % "via_relationship") (:errors result)))))

(deftest validate-restricted-measure-cross-entity-no-relationship
  (let [rm {:base_measure "SUM(revenue)"
            :filter_column "country"
            :filter_values ["US"]
            :entity "customers"
            :via_relationship "orders_to_customers"}
        ;; No relationship exists between orders and customers
        result (expr/validate-restricted-measure rm "orders" [{:name "revenue"}] [])]
    (is (not (:valid result)))
    (is (some #(.contains % "No relationship found") (:errors result)))))

(deftest validate-restricted-measure-cross-entity-valid-relationship
  (let [rm {:base_measure "SUM(revenue)"
            :filter_column "country"
            :filter_values ["US"]
            :entity "customers"
            :via_relationship "orders_to_customers"}
        rels [{:from "orders" :to "customers"}]
        result (expr/validate-restricted-measure rm "orders" [{:name "revenue"}] rels)]
    (is (:valid result))))

;; ---------------------------------------------------------------------------
;; Hierarchies
;; ---------------------------------------------------------------------------

(deftest validate-hierarchy-valid
  (let [h {:name "time_hierarchy"
           :levels [{:column "year"} {:column "quarter"} {:column "month"}]}
        cols [{:name "year"} {:name "quarter"} {:name "month"} {:name "day"}]
        result (expr/validate-hierarchy h cols)]
    (is (:valid result))))

(deftest validate-hierarchy-missing-name
  (let [h {:levels [{:column "year"} {:column "month"}]}
        cols [{:name "year"} {:name "month"}]
        result (expr/validate-hierarchy h cols)]
    (is (not (:valid result)))
    (is (some #(.contains % "name is required") (:errors result)))))

(deftest validate-hierarchy-too-few-levels
  (let [h {:name "flat" :levels [{:column "year"}]}
        cols [{:name "year"}]
        result (expr/validate-hierarchy h cols)]
    (is (not (:valid result)))
    (is (some #(.contains % "at least 2 levels") (:errors result)))))

(deftest validate-hierarchy-unknown-column
  (let [h {:name "geo" :levels [{:column "country"} {:column "city"}]}
        cols [{:name "country"}]
        result (expr/validate-hierarchy h cols)]
    (is (not (:valid result)))
    (is (some #(.contains % "'city' not found") (:errors result)))))

(deftest validate-hierarchy-duplicate-levels
  (let [h {:name "dup" :levels [{:column "year"} {:column "year"}]}
        cols [{:name "year"}]
        result (expr/validate-hierarchy h cols)]
    (is (not (:valid result)))
    (is (some #(.contains % "Duplicate hierarchy levels") (:errors result)))))

;; ---------------------------------------------------------------------------
;; Query-time expansion
;; ---------------------------------------------------------------------------

(deftest expand-row-level-measure-basic
  (let [m {:name "cost_per_mile" :expression "fuel_cost / NULLIF(miles, 0)"}]
    (is (= "(fuel_cost / NULLIF(miles, 0)) AS cost_per_mile"
           (expr/expand-row-level-measure m)))))

(deftest expand-post-aggregate-measure-basic
  (let [m {:name "avg_cpm" :expression "SUM(fuel_cost) / SUM(miles)"}]
    (is (= "(SUM(fuel_cost) / SUM(miles)) AS avg_cpm"
           (expr/expand-post-aggregate-measure m)))))

(deftest expand-restricted-measure-basic
  (let [m {:name "emea_revenue"
           :base_measure "SUM(revenue)"
           :filter_column "region"
           :filter_values ["EMEA"]}]
    (is (= "SUM(CASE WHEN region IN ('EMEA') THEN revenue END) AS emea_revenue"
           (expr/expand-restricted-measure m)))))

(deftest expand-restricted-measure-multiple-values
  (let [m {:name "na_revenue"
           :base_measure "SUM(revenue)"
           :filter_column "region"
           :filter_values ["US" "CA"]}]
    (is (= "SUM(CASE WHEN region IN ('US', 'CA') THEN revenue END) AS na_revenue"
           (expr/expand-restricted-measure m)))))

(deftest expand-restricted-measure-escapes-quotes
  (let [m {:name "test_m"
           :base_measure "COUNT(id)"
           :filter_column "status"
           :filter_values ["it's done"]}]
    (is (.contains (expr/expand-restricted-measure m) "it''s done"))))

;; ---------------------------------------------------------------------------
;; resolve-measures-for-query
;; ---------------------------------------------------------------------------

(deftest resolve-measures-for-query-mixed
  (let [calc [{:name "cost_per_mile"
               :expression "fuel_cost / NULLIF(miles, 0)"
               :aggregation "row"}
              {:name "cost_index"
               :expression "cost_per_mile * 100"
               :aggregation "row"}]
        restr [{:name "emea_revenue"
                :base_measure "SUM(revenue)"
                :filter_column "region"
                :filter_values ["EMEA"]}]
        sorted ["cost_per_mile" "cost_index"]
        requested #{"cost_index" "emea_revenue"}
        result (expr/resolve-measures-for-query requested calc restr sorted)]
    ;; cost_per_mile is not requested, so only cost_index should appear
    (is (= 2 (count (:select-exprs result))))
    ;; cost_index is a row-level measure
    (is (.contains (first (:select-exprs result)) "cost_per_mile * 100"))
    ;; emea_revenue is a restricted measure
    (is (.contains (second (:select-exprs result)) "emea_revenue"))))

(deftest resolve-measures-for-query-empty
  (let [result (expr/resolve-measures-for-query #{} [] [] [])]
    (is (= [] (:select-exprs result)))))
