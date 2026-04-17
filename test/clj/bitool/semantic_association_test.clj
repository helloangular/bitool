(ns bitool.semantic-association-test
  (:require [clojure.test :refer :all]
            [bitool.semantic.association :as assoc]))

;; ---------------------------------------------------------------------------
;; Test fixtures — reusable model fragments
;; ---------------------------------------------------------------------------

(def sample-relationships
  [{:from "trips" :from_column "driver_id"
    :to   "drivers" :to_column "driver_id"
    :type "many_to_one" :join "LEFT"}
   {:from "trips" :from_column "vehicle_id"
    :to   "vehicles" :to_column "vehicle_id"
    :type "many_to_one" :join "LEFT"}
   {:from "vehicles" :from_column "depot_id"
    :to   "depots" :to_column "depot_id"
    :type "many_to_one" :join "LEFT"}])

(def sample-model
  {:entities {"trips"    {:kind "fact"
                          :table "gold_trips"
                          :columns [{:name "trip_id"    :role "dimension"}
                                    {:name "driver_id"  :role "dimension"}
                                    {:name "vehicle_id" :role "dimension"}
                                    {:name "miles"      :role "measure"}
                                    {:name "fuel_cost"  :role "measure"}]}
             "drivers"   {:kind "dimension"
                          :table "silver_drivers"
                          :columns [{:name "driver_id"   :role "business_key"}
                                    {:name "driver_name" :role "attribute"}
                                    {:name "region"      :role "attribute"}]}
             "vehicles"  {:kind "dimension"
                          :table "silver_vehicles"
                          :columns [{:name "vehicle_id" :role "business_key"}
                                    {:name "make"       :role "attribute"}
                                    {:name "depot_id"   :role "dimension"}]}
             "depots"    {:kind "dimension"
                          :table "silver_depots"
                          :columns [{:name "depot_id"   :role "business_key"}
                                    {:name "depot_name" :role "attribute"}
                                    {:name "city"       :role "attribute"}]}}
   :relationships sample-relationships})

;; ---------------------------------------------------------------------------
;; build-relationship-graph
;; ---------------------------------------------------------------------------

(deftest build-graph-bidirectional
  (let [{:keys [adjacency edges]} (assoc/build-relationship-graph sample-relationships)]
    ;; trips connects to drivers and vehicles
    (is (= #{"drivers" "vehicles"} (get adjacency "trips")))
    ;; drivers connects back to trips
    (is (= #{"trips"} (get adjacency "drivers")))
    ;; vehicles connects to trips and depots
    (is (= #{"trips" "depots"} (get adjacency "vehicles")))
    ;; depots connects back to vehicles
    (is (= #{"vehicles"} (get adjacency "depots")))
    ;; Forward and reverse edges stored
    (is (= 1 (count (get edges ["trips" "drivers"]))))
    (is (= 1 (count (get edges ["drivers" "trips"]))))
    (is (= :forward (:direction (first (get edges ["trips" "drivers"])))))
    (is (= :reverse (:direction (first (get edges ["drivers" "trips"])))))))

(deftest build-graph-empty-relationships
  (let [{:keys [adjacency edges]} (assoc/build-relationship-graph [])]
    (is (empty? adjacency))
    (is (empty? edges))))

;; ---------------------------------------------------------------------------
;; extract-referenced-entities
;; ---------------------------------------------------------------------------

(deftest extract-refs-from-qualified-columns
  (let [isl {:table "trips"
             :columns ["trips.miles" "drivers.region"]}]
    (is (= #{"drivers"} (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-from-aggregates
  (let [isl {:table "trips"
             :aggregates [{:fn "SUM" :column "trips.miles"}
                          {:fn "COUNT" :column "vehicles.make"}]}]
    (is (= #{"vehicles"} (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-from-filters
  (let [isl {:table "trips"
             :columns ["miles"]
             :filters [{:column "drivers.region" :op "=" :value "EMEA"}]}]
    (is (= #{"drivers"} (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-from-group-by
  (let [isl {:table "trips"
             :columns ["miles"]
             :group_by ["drivers.region"]}]
    (is (= #{"drivers"} (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-from-order-by
  (let [isl {:table "trips"
             :columns ["miles"]
             :order_by [{:column "depots.city" :direction "ASC"}]}]
    (is (= #{"depots"} (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-ignores-base-entity
  (let [isl {:table "trips"
             :columns ["trips.miles" "trips.fuel_cost"]}]
    ;; trips is the base — should not appear in referenced set
    (is (empty? (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-ignores-unqualified
  (let [isl {:table "trips"
             :columns ["miles" "fuel_cost"]}]
    (is (empty? (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-ignores-unknown-entities
  (let [isl {:table "trips"
             :columns ["unknown_table.some_col"]}]
    (is (empty? (assoc/extract-referenced-entities isl sample-model)))))

(deftest extract-refs-multiple-entities
  (let [isl {:table "trips"
             :columns ["drivers.region" "vehicles.make" "depots.city"]}]
    (is (= #{"drivers" "vehicles" "depots"}
           (assoc/extract-referenced-entities isl sample-model)))))

;; ---------------------------------------------------------------------------
;; BFS shortest-path (tested via resolve-lazy-joins)
;; ---------------------------------------------------------------------------

(deftest resolve-direct-relationship
  (let [isl {:table "trips"
             :columns ["drivers.region"]}
        {:keys [join-specs joined-entities]} (assoc/resolve-lazy-joins isl sample-model)]
    (is (= 1 (count join-specs)))
    (is (= "trips" (:from (first join-specs))))
    (is (= "drivers" (:to (first join-specs))))
    (is (contains? joined-entities "drivers"))))

(deftest resolve-multi-hop-path
  ;; depots is reachable from trips only via vehicles
  (let [isl {:table "trips"
             :columns ["depots.city"]}
        {:keys [join-specs joined-entities]} (assoc/resolve-lazy-joins isl sample-model)]
    ;; Two joins: trips→vehicles, vehicles→depots
    (is (= 2 (count join-specs)))
    (is (contains? joined-entities "vehicles"))
    (is (contains? joined-entities "depots"))))

(deftest resolve-multiple-targets
  (let [isl {:table "trips"
             :columns ["drivers.region" "vehicles.make"]}
        {:keys [join-specs joined-entities]} (assoc/resolve-lazy-joins isl sample-model)]
    ;; Two direct joins
    (is (= 2 (count join-specs)))
    (is (contains? joined-entities "drivers"))
    (is (contains? joined-entities "vehicles"))))

(deftest resolve-deduplicates-intermediate-joins
  ;; Both vehicles.make and depots.city referenced — vehicles should only be joined once
  (let [isl {:table "trips"
             :columns ["vehicles.make" "depots.city"]}
        {:keys [join-specs]} (assoc/resolve-lazy-joins isl sample-model)]
    ;; trips→vehicles for vehicles.make, then vehicles→depots for depots.city
    ;; vehicles should NOT be joined twice
    (let [join-entities (map #(if (= :forward (:direction %)) (:to %) (:from %)) join-specs)]
      (is (= (count join-entities) (count (distinct join-entities)))))))

(deftest resolve-no-refs-returns-empty
  (let [isl {:table "trips"
             :columns ["miles" "fuel_cost"]}
        {:keys [join-specs joined-entities]} (assoc/resolve-lazy-joins isl sample-model)]
    (is (empty? join-specs))
    (is (= #{"trips"} joined-entities))))

(deftest resolve-unreachable-throws
  (let [model (assoc sample-model :relationships
                     ;; Remove all relationships to depots
                     [{:from "trips" :from_column "driver_id"
                       :to "drivers" :to_column "driver_id"
                       :type "many_to_one" :join "LEFT"}])
        isl {:table "trips"
             :columns ["depots.city"]}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Cannot resolve join path"
                          (assoc/resolve-lazy-joins isl model)))))

;; ---------------------------------------------------------------------------
;; Ambiguity detection
;; ---------------------------------------------------------------------------

(deftest resolve-ambiguous-throws-without-preferred
  (let [rels [{:from "trips" :from_column "driver_id"
               :to "drivers" :to_column "driver_id"
               :type "many_to_one" :join "LEFT"}
              {:from "trips" :from_column "backup_driver_id"
               :to "drivers" :to_column "driver_id"
               :type "many_to_one" :join "LEFT"}]
        model (assoc sample-model :relationships rels)
        isl {:table "trips"
             :columns ["drivers.region"]}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Ambiguous join paths"
                          (assoc/resolve-lazy-joins isl model)))))

(deftest resolve-ambiguous-prefers-preferred
  (let [rels [{:from "trips" :from_column "driver_id"
               :to "drivers" :to_column "driver_id"
               :type "many_to_one" :join "LEFT"
               :preferred true}
              {:from "trips" :from_column "backup_driver_id"
               :to "drivers" :to_column "driver_id"
               :type "many_to_one" :join "LEFT"}]
        model (assoc sample-model :relationships rels)
        isl {:table "trips"
             :columns ["drivers.region"]}
        {:keys [join-specs]} (assoc/resolve-lazy-joins isl model)]
    (is (= 1 (count join-specs)))
    (is (= "driver_id" (:from_column (first join-specs))))))

;; ---------------------------------------------------------------------------
;; Multi-route ambiguity (different paths through different intermediate entities)
;; ---------------------------------------------------------------------------

(deftest resolve-ambiguous-multi-route-throws
  ;; Two equal-length paths: trips→vehicles→depots AND trips→routes→depots
  (let [rels [{:from "trips" :from_column "vehicle_id"
               :to "vehicles" :to_column "vehicle_id"
               :type "many_to_one" :join "LEFT"}
              {:from "vehicles" :from_column "depot_id"
               :to "depots" :to_column "depot_id"
               :type "many_to_one" :join "LEFT"}
              {:from "trips" :from_column "route_id"
               :to "routes" :to_column "route_id"
               :type "many_to_one" :join "LEFT"}
              {:from "routes" :from_column "depot_id"
               :to "depots" :to_column "depot_id"
               :type "many_to_one" :join "LEFT"}]
        model {:entities {"trips"    {:columns [{:name "vehicle_id"} {:name "route_id"}]}
                          "vehicles" {:columns [{:name "vehicle_id"} {:name "depot_id"}]}
                          "routes"   {:columns [{:name "route_id"} {:name "depot_id"}]}
                          "depots"   {:columns [{:name "depot_id"} {:name "city"}]}}
               :relationships rels}
        isl {:table "trips" :columns ["depots.city"]}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Ambiguous join paths"
                          (assoc/resolve-lazy-joins isl model)))))

(deftest resolve-ambiguous-multi-route-prefers-fully-preferred-path
  ;; Same two paths, but one has all edges marked :preferred
  (let [rels [{:from "trips" :from_column "vehicle_id"
               :to "vehicles" :to_column "vehicle_id"
               :type "many_to_one" :join "LEFT"
               :preferred true}
              {:from "vehicles" :from_column "depot_id"
               :to "depots" :to_column "depot_id"
               :type "many_to_one" :join "LEFT"
               :preferred true}
              {:from "trips" :from_column "route_id"
               :to "routes" :to_column "route_id"
               :type "many_to_one" :join "LEFT"}
              {:from "routes" :from_column "depot_id"
               :to "depots" :to_column "depot_id"
               :type "many_to_one" :join "LEFT"}]
        model {:entities {"trips"    {:columns [{:name "vehicle_id"} {:name "route_id"}]}
                          "vehicles" {:columns [{:name "vehicle_id"} {:name "depot_id"}]}
                          "routes"   {:columns [{:name "route_id"} {:name "depot_id"}]}
                          "depots"   {:columns [{:name "depot_id"} {:name "city"}]}}
               :relationships rels}
        isl {:table "trips" :columns ["depots.city"]}
        {:keys [join-specs joined-entities]} (assoc/resolve-lazy-joins isl model)]
    ;; Should resolve via the preferred path: trips→vehicles→depots
    (is (= 2 (count join-specs)))
    (is (contains? joined-entities "vehicles"))
    (is (contains? joined-entities "depots"))
    ;; routes should NOT be joined
    (is (not (contains? joined-entities "routes")))))

;; ---------------------------------------------------------------------------
;; relationship->isl-join-spec
;; ---------------------------------------------------------------------------

(deftest relationship-to-isl-forward
  (let [rel {:from "trips" :from_column "driver_id"
             :to "drivers" :to_column "driver_id"
             :join "LEFT" :direction :forward}
        spec (assoc/relationship->isl-join-spec rel)]
    (is (= "drivers" (:table spec)))
    (is (= "LEFT" (:type spec)))
    (is (= {"trips.driver_id" "drivers.driver_id"} (:on spec)))))

(deftest relationship-to-isl-reverse
  (let [rel {:from "trips" :from_column "driver_id"
             :to "drivers" :to_column "driver_id"
             :join "LEFT" :direction :reverse}
        spec (assoc/relationship->isl-join-spec rel)]
    (is (= "trips" (:table spec)))
    (is (= "LEFT" (:type spec)))
    (is (= {"drivers.driver_id" "trips.driver_id"} (:on spec)))))

;; ---------------------------------------------------------------------------
;; inject-lazy-joins
;; ---------------------------------------------------------------------------

(deftest inject-adds-joins-to-isl
  (let [isl {:table "trips"
             :columns ["drivers.region" "miles"]}
        result (assoc/inject-lazy-joins isl sample-model)]
    (is (seq (:join result)))
    (is (= "drivers" (:table (first (:join result)))))))

(deftest inject-skips-when-explicit-joins-present
  (let [isl {:table "trips"
             :columns ["drivers.region"]
             :join [{:table "drivers" :type "LEFT"
                     :on {"trips.driver_id" "drivers.driver_id"}}]}
        result (assoc/inject-lazy-joins isl sample-model)]
    ;; Should return ISL unchanged — explicit joins take precedence
    (is (= isl result))))

(deftest inject-noop-when-no-cross-entity-refs
  (let [isl {:table "trips"
             :columns ["miles" "fuel_cost"]}
        result (assoc/inject-lazy-joins isl sample-model)]
    (is (nil? (:join result)))))

;; ---------------------------------------------------------------------------
;; validate-relationship-uniqueness
;; ---------------------------------------------------------------------------

(deftest validate-uniqueness-passes-for-distinct-pairs
  (is (:valid (assoc/validate-relationship-uniqueness sample-relationships))))

(deftest validate-uniqueness-fails-for-duplicate-without-preferred
  (let [rels [{:from "trips" :from_column "driver_id"
               :to "drivers" :to_column "driver_id"}
              {:from "trips" :from_column "backup_driver_id"
               :to "drivers" :to_column "driver_id"}]]
    (let [result (assoc/validate-relationship-uniqueness rels)]
      (is (not (:valid result)))
      (is (seq (:errors result))))))

(deftest validate-uniqueness-passes-with-preferred
  (let [rels [{:from "trips" :from_column "driver_id"
               :to "drivers" :to_column "driver_id"
               :preferred true}
              {:from "trips" :from_column "backup_driver_id"
               :to "drivers" :to_column "driver_id"}]]
    (is (:valid (assoc/validate-relationship-uniqueness rels)))))

(deftest validate-uniqueness-fails-with-two-preferred
  (let [rels [{:from "trips" :from_column "driver_id"
               :to "drivers" :to_column "driver_id"
               :preferred true}
              {:from "trips" :from_column "backup_driver_id"
               :to "drivers" :to_column "driver_id"
               :preferred true}]]
    (let [result (assoc/validate-relationship-uniqueness rels)]
      (is (not (:valid result))))))

(deftest validate-uniqueness-empty-rels
  (is (:valid (assoc/validate-relationship-uniqueness []))))

;; ---------------------------------------------------------------------------
;; String-key ISL (LLM output often uses string keys)
;; ---------------------------------------------------------------------------

(deftest string-key-isl-works
  (let [isl {"table" "trips"
             "columns" ["drivers.region" "miles"]}
        {:keys [join-specs]} (assoc/resolve-lazy-joins isl sample-model)]
    (is (= 1 (count join-specs)))))

(deftest string-key-isl-inject
  (let [isl {"table" "trips"
             "columns" ["vehicles.make"]}
        result (assoc/inject-lazy-joins isl sample-model)]
    (is (seq (:join result)))))
