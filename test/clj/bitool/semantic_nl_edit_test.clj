(ns bitool.semantic-nl-edit-test
  (:require [clojure.test :refer :all]
            [bitool.semantic.nl-edit :as nl-edit]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Fixture model
;; ---------------------------------------------------------------------------

(def base-model
  {:entities {:trips   {:kind "fact"
                         :table "gold_trips"
                         :description "Trip data"
                         :columns [{:name "trip_id"   :role "dimension"}
                                   {:name "miles"     :role "measure"}
                                   {:name "fuel_cost" :role "measure"}
                                   {:name "region"    :role "attribute"}]}
              :drivers {:kind "dimension"
                         :table "silver_drivers"
                         :columns [{:name "driver_id"   :role "business_key"}
                                   {:name "driver_name" :role "attribute"}
                                   {:name "region"      :role "attribute"}]}}
   :relationships [{:from "trips" :from_column "driver_id"
                     :to "drivers" :to_column "driver_id"
                     :type "many_to_one" :join "LEFT"}]
   :calculated_measures [{:name "cost_per_mile"
                           :expression "fuel_cost / NULLIF(miles, 0)"
                           :entity "trips"
                           :aggregation "row"}]
   :restricted_measures [{:name "emea_cost"
                           :base_measure "SUM(fuel_cost)"
                           :filter_column "region"
                           :filter_values ["EMEA"]
                           :entity "trips"}]
   :hierarchies [{:name "geography"
                   :entity "drivers"
                   :levels [{:column "region"} {:column "driver_name"}]}]})

;; ---------------------------------------------------------------------------
;; parse-command
;; ---------------------------------------------------------------------------

(deftest parse-add-calculated-measure
  (let [cmd (nl-edit/parse-command "add measure total_fuel as SUM(fuel_cost) on trips")]
    (is (= :add-calculated-measure (:action cmd)))
    (is (= "total_fuel" (get-in cmd [:params :name])))
    (is (= "SUM(fuel_cost)" (get-in cmd [:params :expression])))
    (is (= "trips" (get-in cmd [:params :entity])))))

(deftest parse-add-calculated-measure-with-quotes
  (let [cmd (nl-edit/parse-command "add a calculated measure 'margin' = 'revenue - cost' on orders")]
    (is (= :add-calculated-measure (:action cmd)))
    (is (= "margin" (get-in cmd [:params :name])))))

(deftest parse-add-restricted-measure
  (let [cmd (nl-edit/parse-command "add restricted measure emea_revenue as SUM(revenue) where region = EMEA on trips")]
    (is (= :add-restricted-measure (:action cmd)))
    (is (= "emea_revenue" (get-in cmd [:params :name])))
    (is (= "SUM(revenue)" (get-in cmd [:params :base_measure])))
    (is (= "region" (get-in cmd [:params :filter_column])))
    (is (= ["EMEA"] (get-in cmd [:params :filter_values])))))

(deftest parse-remove-measure
  (let [cmd (nl-edit/parse-command "remove measure cost_per_mile")]
    (is (= :remove-measure (:action cmd)))
    (is (= "cost_per_mile" (get-in cmd [:params :name])))))

(deftest parse-add-hierarchy
  (let [cmd (nl-edit/parse-command "add hierarchy time_drill on trips: year, quarter, month")]
    (is (= :add-hierarchy (:action cmd)))
    (is (= "time_drill" (get-in cmd [:params :name])))
    (is (= "trips" (get-in cmd [:params :entity])))
    (is (= [{:column "year"} {:column "quarter"} {:column "month"}]
           (get-in cmd [:params :levels])))))

(deftest parse-add-hierarchy-arrow-syntax
  (let [cmd (nl-edit/parse-command "add hierarchy geo on drivers: region > driver_name")]
    (is (= :add-hierarchy (:action cmd)))
    (is (= [{:column "region"} {:column "driver_name"}]
           (get-in cmd [:params :levels])))))

(deftest parse-remove-hierarchy
  (let [cmd (nl-edit/parse-command "delete hierarchy geography")]
    (is (= :remove-hierarchy (:action cmd)))
    (is (= "geography" (get-in cmd [:params :name])))))

(deftest parse-add-relationship
  (let [cmd (nl-edit/parse-command "add relationship trips.vehicle_id -> vehicles.vehicle_id")]
    (is (= :add-relationship (:action cmd)))
    (is (= "trips" (get-in cmd [:params :from])))
    (is (= "vehicle_id" (get-in cmd [:params :from_column])))
    (is (= "vehicles" (get-in cmd [:params :to])))
    (is (= "vehicle_id" (get-in cmd [:params :to_column])))))

(deftest parse-remove-relationship
  (let [cmd (nl-edit/parse-command "remove relationship trips to drivers")]
    (is (= :remove-relationship (:action cmd)))
    (is (= "trips" (get-in cmd [:params :from])))
    (is (= "drivers" (get-in cmd [:params :to])))))

(deftest parse-set-description
  (let [cmd (nl-edit/parse-command "set description of trips to One row per completed trip")]
    (is (= :set-entity-description (:action cmd)))
    (is (= "trips" (get-in cmd [:params :entity])))
    (is (= "One row per completed trip" (get-in cmd [:params :description])))))

(deftest parse-rename-entity
  (let [cmd (nl-edit/parse-command "rename entity trips to journeys")]
    (is (= :rename-entity (:action cmd)))
    (is (= "trips" (get-in cmd [:params :old_name])))
    (is (= "journeys" (get-in cmd [:params :new_name])))))

(deftest parse-rename-measure
  (let [cmd (nl-edit/parse-command "rename measure cost_per_mile to fuel_efficiency")]
    (is (= :rename-measure (:action cmd)))
    (is (= "cost_per_mile" (get-in cmd [:params :old_name])))
    (is (= "fuel_efficiency" (get-in cmd [:params :new_name])))))

(deftest parse-prefer-relationship
  (let [cmd (nl-edit/parse-command "prefer relationship trips to drivers")]
    (is (= :prefer-relationship (:action cmd)))
    (is (= "trips" (get-in cmd [:params :from])))
    (is (= "drivers" (get-in cmd [:params :to])))))

(deftest parse-unrecognized-returns-nil
  (is (nil? (nl-edit/parse-command "make the model better")))
  (is (nil? (nl-edit/parse-command ""))))

;; ---------------------------------------------------------------------------
;; apply-command
;; ---------------------------------------------------------------------------

(deftest apply-add-calculated-measure
  (let [result (nl-edit/execute-nl-edit base-model
                 "add measure total_fuel as SUM(fuel_cost) on trips")]
    (is (nil? (:error result)))
    (is (= 2 (count (:calculated_measures (:model result)))))
    (is (string/includes? (:summary result) "total_fuel"))))

(deftest apply-remove-measure-calculated
  (let [result (nl-edit/execute-nl-edit base-model "remove measure cost_per_mile")]
    (is (nil? (:error result)))
    (is (empty? (:calculated_measures (:model result))))))

(deftest apply-remove-measure-restricted
  (let [result (nl-edit/execute-nl-edit base-model "remove measure emea_cost")]
    (is (nil? (:error result)))
    (is (empty? (:restricted_measures (:model result))))))

(deftest apply-add-hierarchy
  (let [result (nl-edit/execute-nl-edit base-model
                 "add hierarchy time_drill on trips: year, quarter, month")]
    (is (nil? (:error result)))
    (is (= 2 (count (:hierarchies (:model result)))))))

(deftest apply-remove-hierarchy
  (let [result (nl-edit/execute-nl-edit base-model "drop hierarchy geography")]
    (is (nil? (:error result)))
    (is (empty? (:hierarchies (:model result))))))

(deftest apply-add-relationship
  (let [result (nl-edit/execute-nl-edit base-model
                 "add relationship trips.vehicle_id -> vehicles.vehicle_id")]
    (is (nil? (:error result)))
    (is (= 2 (count (:relationships (:model result)))))))

(deftest apply-remove-relationship
  (let [result (nl-edit/execute-nl-edit base-model
                 "remove relationship trips to drivers")]
    (is (nil? (:error result)))
    (is (empty? (:relationships (:model result))))))

(deftest apply-set-entity-description
  (let [result (nl-edit/execute-nl-edit base-model
                 "set description of trips to One row per completed trip")]
    (is (nil? (:error result)))
    (is (= "One row per completed trip"
           (get-in (:model result) [:entities :trips :description])))))

(deftest apply-rename-entity-updates-all-refs
  (let [result (nl-edit/execute-nl-edit base-model
                 "rename entity trips to journeys")]
    (is (nil? (:error result)))
    ;; Entity key changed
    (is (some? (get-in (:model result) [:entities :journeys])))
    (is (nil? (get-in (:model result) [:entities :trips])))
    ;; Measure entity ref updated
    (is (= "journeys" (:entity (first (:calculated_measures (:model result))))))
    ;; Relationship from ref updated
    (is (= "journeys" (:from (first (:relationships (:model result))))))))

(deftest apply-rename-measure
  (let [result (nl-edit/execute-nl-edit base-model
                 "rename measure cost_per_mile to fuel_efficiency")]
    (is (nil? (:error result)))
    (is (= "fuel_efficiency" (:name (first (:calculated_measures (:model result))))))))

(deftest apply-prefer-relationship
  (let [result (nl-edit/execute-nl-edit base-model
                 "prefer relationship trips to drivers")]
    (is (nil? (:error result)))
    (is (true? (:preferred (first (:relationships (:model result))))))))

(deftest unrecognized-command-returns-error
  (let [result (nl-edit/execute-nl-edit base-model "make it better")]
    (is (some? (:error result)))
    (is (some? (:hint result)))))

(deftest set-description-unknown-entity-throws
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Entity not found"
                        (nl-edit/execute-nl-edit base-model
                          "set description of nonexistent to foo"))))

(deftest rename-unknown-entity-throws
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Entity not found"
                        (nl-edit/execute-nl-edit base-model
                          "rename entity bogus to something"))))
