(ns bitool.semantic-governance-test
  (:require [clojure.test :refer :all]
            [clojure.set :as cset]
            [bitool.semantic.governance :as gov]))

;; ---------------------------------------------------------------------------
;; Test fixture — full model (reuses the standard fleet model)
;; ---------------------------------------------------------------------------

(def fleet-model
  {:entities {"trips"   {:kind "fact"
                          :table "gold_trips"
                          :columns [{:name "trip_id"    :role "dimension"}
                                    {:name "driver_id"  :role "dimension"}
                                    {:name "miles"      :role "measure"}
                                    {:name "fuel_cost"  :role "measure"}
                                    {:name "region"     :role "attribute"}
                                    {:name "event_date" :role "time_dimension"}]}
             "drivers"  {:kind "dimension"
                          :table "silver_drivers"
                          :columns [{:name "driver_id"   :role "business_key"}
                                    {:name "driver_name" :role "attribute"}
                                    {:name "region"      :role "attribute"}
                                    {:name "hire_date"   :role "timestamp"}]}
             "vehicles" {:kind "dimension"
                          :table "silver_vehicles"
                          :columns [{:name "vehicle_id" :role "business_key"}
                                    {:name "make"       :role "attribute"}
                                    {:name "model_year" :role "attribute"}]}}
   :relationships [{:from "trips" :from_column "driver_id"
                     :to "drivers" :to_column "driver_id"
                     :type "many_to_one" :join "LEFT"}]
   :calculated_measures [{:name "cost_per_mile"
                           :expression "fuel_cost / NULLIF(miles, 0)"
                           :entity "trips"}]
   :restricted_measures [{:name "emea_fuel_cost"
                           :base_measure "SUM(fuel_cost)"
                           :filter_column "region"
                           :filter_values ["EMEA"]
                           :entity "trips"}]
   :hierarchies [{:name "geography"
                   :entity "drivers"
                   :levels [{:column "region"} {:column "driver_name"}]}]})

;; ---------------------------------------------------------------------------
;; apply-rls-filters (pure function — no DB needed)
;; ---------------------------------------------------------------------------

(deftest rls-no-policies-returns-no-filters
  (let [result (gov/apply-rls-filters fleet-model [] {:region "EMEA"})]
    (is (not (:blocked result)))
    (is (empty? (:filters result)))))

(deftest rls-matching-user-value-injects-filter
  (let [policies [{:entity "drivers"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values ["EMEA" "NA" "APAC"]}]
        result   (gov/apply-rls-filters fleet-model policies {:region "EMEA"})]
    (is (not (:blocked result)))
    (is (= 1 (count (:filters result))))
    (is (= "silver_drivers.region" (:column (first (:filters result)))))
    (is (= "=" (:op (first (:filters result)))))
    (is (= "EMEA" (:value (first (:filters result)))))))

(deftest rls-user-value-not-in-allowed-blocks
  (let [policies [{:entity "drivers"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values ["NA" "APAC"]}]
        result   (gov/apply-rls-filters fleet-model policies {:region "EMEA"})]
    (is (:blocked result))
    (is (= 1 (count (:reasons result))))
    (is (= "User value not in allowed values" (:reason (first (:reasons result)))))))

(deftest rls-missing-session-field-blocks
  (let [policies [{:entity "drivers"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values ["EMEA"]}]
        result   (gov/apply-rls-filters fleet-model policies {})]
    (is (:blocked result))
    (is (= "Session missing required field" (:reason (first (:reasons result)))))))

(deftest rls-nil-allowed-values-means-unrestricted
  ;; nil allowed_values = policy documents the field but doesn't restrict
  (let [policies [{:entity "drivers"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values nil}]
        result   (gov/apply-rls-filters fleet-model policies {:region "EMEA"})]
    (is (not (:blocked result)))
    (is (empty? (:filters result)))))

(deftest rls-multiple-policies-inject-multiple-filters
  (let [policies [{:entity "drivers"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values ["EMEA" "NA"]}
                  {:entity "trips"
                   :column_name "region"
                   :user_field "trip_region"
                   :allowed_values ["EMEA" "NA"]}]
        result   (gov/apply-rls-filters fleet-model policies
                                        {:region "EMEA" :trip_region "NA"})]
    (is (not (:blocked result)))
    (is (= 2 (count (:filters result))))
    ;; First filter on drivers
    (is (= "silver_drivers.region" (:column (first (:filters result)))))
    (is (= "EMEA" (:value (first (:filters result)))))
    ;; Second filter on trips
    (is (= "gold_trips.region" (:column (second (:filters result)))))
    (is (= "NA" (:value (second (:filters result)))))))

(deftest rls-one-policy-blocks-whole-request
  ;; If any policy blocks, the entire request is blocked
  (let [policies [{:entity "drivers"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values ["EMEA"]}
                  {:entity "trips"
                   :column_name "region"
                   :user_field "trip_region"
                   :allowed_values ["NA"]}]
        result   (gov/apply-rls-filters fleet-model policies
                                        {:region "EMEA" :trip_region "EMEA"})]
    ;; Second policy blocks (EMEA not in [NA])
    (is (:blocked result))))

(deftest rls-uses-physical-table-name-not-entity-name
  ;; Filters should use the entity's :table (physical name) for column qualification
  (let [policies [{:entity "trips"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values ["EMEA"]}]
        result   (gov/apply-rls-filters fleet-model policies {:region "EMEA"})]
    (is (= "gold_trips.region" (:column (first (:filters result)))))))

(deftest rls-keyword-entity-keys
  ;; Model might use keyword entity keys
  (let [model    (assoc fleet-model :entities
                        {:trips   (get (:entities fleet-model) "trips")
                         :drivers (get (:entities fleet-model) "drivers")})
        policies [{:entity "trips"
                   :column_name "region"
                   :user_field "region"
                   :allowed_values ["EMEA"]}]
        result   (gov/apply-rls-filters model policies {:region "EMEA"})]
    (is (not (:blocked result)))
    (is (= 1 (count (:filters result))))))

(deftest rls-string-coercion-for-allowed-values
  ;; allowed_values might contain integers; user_val might be string — compare as strings
  (let [model    (assoc-in fleet-model [:entities "trips" :columns]
                           [{:name "dept_id" :role "dimension"}])
        policies [{:entity "trips"
                   :column_name "dept_id"
                   :user_field "dept"
                   :allowed_values [1 2 3]}]
        result   (gov/apply-rls-filters model policies {:dept "2"})]
    (is (not (:blocked result)))
    (is (= 1 (count (:filters result))))))

;; ---------------------------------------------------------------------------
;; Approval workflow validation (pure logic tests)
;; ---------------------------------------------------------------------------
;; The actual submit-for-review! and review-model! require DB.
;; We test the decision→status mapping logic here via the review-model! contract:

(deftest review-decision-must-be-valid
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"decision must be"
                        (gov/review-model! 999 {:reviewer "alice"
                                                :decision "invalid_decision"}))))

(deftest review-requires-reviewer
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"reviewer is required"
                        (gov/review-model! 999 {:reviewer ""
                                                :decision "approved"}))))

;; ---------------------------------------------------------------------------
;; Impact analysis — structural ISL parsing
;; ---------------------------------------------------------------------------
;; Tests the extract-isl-table-refs and extract-isl-measure-refs helpers
;; that impact analysis uses instead of raw string matching.

(deftest entity-tables-extracted-correctly
  (let [entity-tables (->> (or (:entities fleet-model) {})
                           vals
                           (map :table)
                           (remove nil?)
                           set)
        entity-names  (set (map name (keys (or (:entities fleet-model) {}))))]
    (is (= #{"gold_trips" "silver_drivers" "silver_vehicles"} entity-tables))
    (is (= #{"trips" "drivers" "vehicles"} entity-names))))

(deftest extract-isl-table-refs-finds-primary-table
  (let [isl {:table "gold_trips" :columns ["miles" "fuel_cost"]}]
    (is (contains? (#'gov/extract-isl-table-refs isl) "gold_trips"))))

(deftest extract-isl-table-refs-finds-join-tables
  (let [isl {:table "gold_trips"
             :join [{:table "silver_drivers" :type "LEFT"
                     :on {"gold_trips.driver_id" "silver_drivers.driver_id"}}]
             :columns ["miles" "drivers.region"]}]
    (is (= #{"gold_trips" "silver_drivers" "drivers"}
           (#'gov/extract-isl-table-refs isl)))))

(deftest extract-isl-table-refs-finds-qualified-column-prefixes
  (let [isl {:table "gold_trips"
             :columns ["miles" "drivers.region" "vehicles.make"]
             :filters [{:column "drivers.hire_date" :op ">=" :value "2025-01-01"}]}]
    (is (= #{"gold_trips" "drivers" "vehicles"}
           (#'gov/extract-isl-table-refs isl)))))

(deftest extract-isl-table-refs-handles-nil-and-empty
  (is (nil? (#'gov/extract-isl-table-refs nil)))
  (is (= #{"t"} (#'gov/extract-isl-table-refs {:table "t"}))))

(deftest extract-isl-measure-refs-finds-aggregate-columns
  (let [isl {:table "gold_trips"
             :aggregates [{:column "cost_per_mile" :function "AVG"}
                          {:column "miles" :function "SUM"}]}]
    (is (= #{"cost_per_mile" "miles"}
           (#'gov/extract-isl-measure-refs isl)))))

(deftest extract-isl-measure-refs-handles-no-aggregates
  (is (= #{} (#'gov/extract-isl-measure-refs {:table "t" :columns ["a"]}))))

(deftest impact-analysis-isl-structural-match
  ;; Simulate what impact analysis does: parse ISL, match against model tables
  (let [all-table-refs #{"gold_trips" "silver_drivers" "trips" "drivers"}
        measure-names  #{"cost_per_mile" "emea_fuel_cost"}
        ;; ISL that references gold_trips (match)
        isl-hit   {:table "gold_trips" :columns ["miles"]
                   :aggregates [{:column "cost_per_mile" :function "AVG"}]}
        ;; ISL that references unrelated table (no match)
        isl-miss  {:table "raw_orders" :columns ["total"]}
        table-refs-hit  (#'gov/extract-isl-table-refs isl-hit)
        meas-refs-hit   (#'gov/extract-isl-measure-refs isl-hit)
        table-refs-miss (#'gov/extract-isl-table-refs isl-miss)
        meas-refs-miss  (#'gov/extract-isl-measure-refs isl-miss)]
    ;; Hit: table AND measure match
    (is (seq (cset/intersection table-refs-hit all-table-refs)))
    (is (seq (cset/intersection meas-refs-hit measure-names)))
    ;; Miss: neither table nor measure match
    (is (empty? (cset/intersection table-refs-miss all-table-refs)))
    (is (empty? (cset/intersection meas-refs-miss measure-names)))))
