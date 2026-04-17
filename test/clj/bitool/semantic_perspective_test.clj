(ns bitool.semantic-perspective-test
  (:require [clojure.test :refer :all]
            [bitool.semantic.perspective :as persp]))

;; ---------------------------------------------------------------------------
;; Test fixture — full model
;; ---------------------------------------------------------------------------

(def full-model
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
                           :entity "trips"}
                          {:name "total_trips"
                           :expression "COUNT(*)"
                           :entity "trips"}]
   :restricted_measures [{:name "emea_fuel_cost"
                           :base_measure "SUM(fuel_cost)"
                           :filter_column "region"
                           :filter_values ["EMEA"]
                           :entity "trips"}]
   :hierarchies [{:name "geography"
                   :entity "drivers"
                   :levels [{:column "region"} {:column "driver_name"}]}
                  {:name "time"
                   :entity "trips"
                   :levels [{:column "event_date"}]}]})

;; ---------------------------------------------------------------------------
;; validate-perspective-spec
;; ---------------------------------------------------------------------------

(deftest validate-spec-valid
  (let [spec {:entities ["trips" "drivers"]
              :measures ["cost_per_mile"]
              :hierarchies ["geography"]}]
    (is (:valid (persp/validate-perspective-spec spec full-model)))))

(deftest validate-spec-all-nil-valid
  ;; nil means "no restriction" — always valid
  (is (:valid (persp/validate-perspective-spec {} full-model))))

(deftest validate-spec-unknown-entity
  (let [spec {:entities ["trips" "nonexistent"]}
        result (persp/validate-perspective-spec spec full-model)]
    (is (not (:valid result)))
    (is (some #(clojure.string/includes? % "unknown entity") (:errors result)))))

(deftest validate-spec-unknown-column
  (let [spec {:columns {"trips" ["miles" "nonexistent_col"]}}
        result (persp/validate-perspective-spec spec full-model)]
    (is (not (:valid result)))
    (is (some #(clojure.string/includes? % "not found on entity") (:errors result)))))

(deftest validate-spec-unknown-measure
  (let [spec {:measures ["cost_per_mile" "bogus_measure"]}
        result (persp/validate-perspective-spec spec full-model)]
    (is (not (:valid result)))
    (is (some #(clojure.string/includes? % "unknown measure") (:errors result)))))

(deftest validate-spec-unknown-hierarchy
  (let [spec {:hierarchies ["geography" "missing_hier"]}
        result (persp/validate-perspective-spec spec full-model)]
    (is (not (:valid result)))
    (is (some #(clojure.string/includes? % "unknown hierarchy") (:errors result)))))

(deftest validate-spec-column-on-unknown-entity
  (let [spec {:columns {"nonexistent" ["col1"]}}
        result (persp/validate-perspective-spec spec full-model)]
    (is (not (:valid result)))
    (is (some #(clojure.string/includes? % "unknown entity") (:errors result)))))

;; ---------------------------------------------------------------------------
;; apply-perspective — entity filtering
;; ---------------------------------------------------------------------------

(deftest apply-filters-entities
  ;; Include trips + drivers — all measures/hierarchies stay valid
  (let [spec {:entities ["trips" "drivers"]}
        result (persp/apply-perspective full-model spec)]
    (is (= #{"trips" "drivers"} (set (map name (keys (:entities result))))))
    (is (not (contains? (set (map name (keys (:entities result)))) "vehicles")))))

(deftest apply-nil-spec-returns-full-model
  (is (= full-model (persp/apply-perspective full-model nil)))
  (is (= full-model (persp/apply-perspective full-model {}))))

(deftest apply-filters-columns-within-entity
  ;; Keep all columns that measures + hierarchies depend on
  (let [spec {:columns {"trips" ["miles" "fuel_cost" "region" "event_date"]}}
        result (persp/apply-perspective full-model spec)
        trip-cols (set (map :name (:columns (get (:entities result) "trips"))))]
    (is (= #{"miles" "fuel_cost" "region" "event_date"} trip-cols))
    ;; drivers columns should be unaffected (no column filter for drivers)
    (is (= 4 (count (:columns (get (:entities result) "drivers")))))))

(deftest apply-filters-calculated-measures
  (let [spec {:measures ["cost_per_mile"]}
        result (persp/apply-perspective full-model spec)]
    (is (= 1 (count (:calculated_measures result))))
    (is (= "cost_per_mile" (:name (first (:calculated_measures result)))))))

(deftest apply-filters-restricted-measures
  (let [spec {:measures ["emea_fuel_cost"]}
        result (persp/apply-perspective full-model spec)]
    (is (= 1 (count (:restricted_measures result))))
    (is (= "emea_fuel_cost" (:name (first (:restricted_measures result)))))
    ;; No calculated measures should remain (not in allowed list)
    (is (empty? (:calculated_measures result)))))

(deftest apply-filters-hierarchies
  (let [spec {:hierarchies ["geography"]}
        result (persp/apply-perspective full-model spec)]
    (is (= 1 (count (:hierarchies result))))
    (is (= "geography" (:name (first (:hierarchies result)))))))

(deftest apply-filters-relationships-to-allowed-entities
  ;; trips only — but also exclude measures/hierarchies that have deps outside trips
  (let [spec {:entities ["trips"]
              :hierarchies ["time"]}
        result (persp/apply-perspective full-model spec)]
    ;; trips→drivers relationship should be removed since drivers is excluded
    (is (empty? (:relationships result)))))

(deftest apply-keeps-relationships-between-allowed-entities
  (let [spec {:entities ["trips" "drivers"]}
        result (persp/apply-perspective full-model spec)]
    (is (= 1 (count (:relationships result))))))

;; ---------------------------------------------------------------------------
;; apply-perspective — combined filtering
;; ---------------------------------------------------------------------------

(deftest apply-combined-perspective
  ;; A consistent combined perspective: trips only, keep columns that
  ;; cost_per_mile depends on (fuel_cost, miles), no time hierarchy (event_date hidden)
  (let [spec {:entities    ["trips"]
              :columns     {"trips" ["miles" "fuel_cost" "region"]}
              :measures    ["cost_per_mile"]
              :hierarchies []}
        result (persp/apply-perspective full-model spec)]
    ;; Only trips entity
    (is (= 1 (count (:entities result))))
    ;; Only miles, fuel_cost, region columns
    (is (= #{"miles" "fuel_cost" "region"}
           (set (map :name (:columns (get (:entities result) "trips"))))))
    ;; Only cost_per_mile measure
    (is (= 1 (count (:calculated_measures result))))
    (is (= "cost_per_mile" (:name (first (:calculated_measures result)))))
    ;; No hierarchies
    (is (empty? (:hierarchies result)))
    ;; No restricted measures (not in allowed list)
    (is (empty? (:restricted_measures result)))
    ;; No relationships (drivers not allowed)
    (is (empty? (:relationships result)))))

(deftest apply-entity-filter-cascades-to-measures
  ;; If entity is excluded, its measures should also be excluded
  (let [spec {:entities ["drivers"]}
        result (persp/apply-perspective full-model spec)]
    ;; All measures are on "trips" entity, which is excluded
    (is (empty? (:calculated_measures result)))
    (is (empty? (:restricted_measures result)))))

;; ---------------------------------------------------------------------------
;; Post-filter consistency validation
;; ---------------------------------------------------------------------------

(deftest apply-throws-when-column-filter-hides-measure-dependency
  ;; Perspective keeps cost_per_mile but hides "miles" column — should throw
  (let [spec {:columns {"trips" ["fuel_cost" "region" "event_date"]}}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Perspective produces an invalid model"
                          (persp/apply-perspective full-model spec)))))

(deftest apply-throws-when-column-filter-hides-restricted-measure-filter-col
  ;; Perspective hides "region" column — emea_fuel_cost depends on it
  (let [spec {:columns {"trips" ["trip_id" "miles" "fuel_cost" "event_date"]}}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Perspective produces an invalid model"
                          (persp/apply-perspective full-model spec)))))

(deftest apply-throws-when-column-filter-hides-hierarchy-level
  ;; Perspective hides "event_date" — time hierarchy depends on it
  (let [spec {:columns {"trips" ["miles" "fuel_cost" "region"]}}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Perspective produces an invalid model"
                          (persp/apply-perspective full-model spec)))))

(deftest apply-valid-when-hidden-columns-dont-affect-retained-measures
  ;; Hide trip_id and driver_id — no measures/hierarchies depend on them
  (let [spec {:columns {"trips" ["miles" "fuel_cost" "region" "event_date"]}}
        result (persp/apply-perspective full-model spec)]
    (is (= #{"miles" "fuel_cost" "region" "event_date"}
           (set (map :name (:columns (get (:entities result) "trips"))))))))

;; ---------------------------------------------------------------------------
;; Keyword entity keys (models can use keyword or string keys)
;; ---------------------------------------------------------------------------

(deftest apply-perspective-with-keyword-entity-keys
  (let [model (assoc full-model :entities
                     {:trips   (get (:entities full-model) "trips")
                      :drivers (get (:entities full-model) "drivers")})
        spec {:entities ["trips" "drivers"]}
        result (persp/apply-perspective model spec)]
    (is (= 2 (count (:entities result))))))
