(ns bitool.explode-patterns-test
  "Tests all 10 JSON explode patterns against schema inference and grain planner."
  (:require [bitool.ingest.schema-infer :as schema-infer]
            [bitool.ingest.grain-planner :as grain-planner]
            [clojure.test :refer :all]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Test data — mirrors the 10 mock-api /test/explode/* endpoints
;; ---------------------------------------------------------------------------

(def pattern-1-simple-array
  {"data" [{"id" "SA-1000" "name" "Widget A" "price" 19.99 "in_stock" true
            "created_at" "2025-01-10T00:00:00.000Z"}
           {"id" "SA-1001" "name" "Widget B" "price" 25.49 "in_stock" false
            "created_at" "2025-01-11T00:00:00.000Z"}]})

(def pattern-2-deep-nested
  {"response" {"results" {"items" [{"order_id" "ORD-3000" "customer" "Customer 1"
                                     "total" 120.0 "currency" "USD"
                                     "placed_at" "2025-03-01T00:00:00.000Z"}
                                    {"order_id" "ORD-3001" "customer" "Customer 2"
                                     "total" 165.3 "currency" "USD"
                                     "placed_at" "2025-03-02T00:00:00.000Z"}]
                           "total_count" 2
                           "query_time_ms" 42}
               "request_id" "req-abc-123"}})

(def pattern-3-root-array
  [{"sensor_id" "SENS-4000" "reading" 22.5 "unit" "celsius"
    "ts" "2025-04-15T08:00:00.000Z"}
   {"sensor_id" "SENS-4001" "reading" 23.8 "unit" "fahrenheit"
    "ts" "2025-04-15T08:10:00.000Z"}])

(def pattern-4-parent-child
  {"data" [{"invoice_id" "INV-5000" "vendor" "Acme Corp 1"
            "invoice_date" "2025-02-10T00:00:00.000Z" "status" "pending"
            "total_amount" 500.0
            "line_items" [{"line_id" "LI-5000-0" "description" "Part A"
                           "quantity" 1 "unit_price" 25.0 "tax_rate" 0.08}
                          {"line_id" "LI-5000-1" "description" "Part B"
                           "quantity" 3 "unit_price" 37.5 "tax_rate" 0.08}]
            "approvals" [{"approver" "manager_1@acme.com"
                          "approved_at" "2025-02-12T00:00:00.000Z" "level" 1}]}]})

(def pattern-5-nested-object
  {"data" [{"device_id" "DEV-6000" "name" "Gateway 1"
            "location" {"lat" 37.7749 "lng" -122.4194
                        "address" "100 Market St, San Francisco, CA" "floor" 1}
            "config" {"firmware_version" "3.0.1" "protocol" "mqtt"
                      "interval_seconds" 30}
            "last_seen" "2025-05-01T12:00:00.000Z"}]})

(def pattern-6-peer-arrays
  {"trucks" [{"id" "TRK-7000" "vin" "1HGBH41JXMN100000" "make" "Freightliner" "status" "active"}]
   "trailers" [{"id" "TRL-7100" "type" "dry_van" "capacity_lbs" 40000}]
   "drivers" [{"id" "DRV-7200" "name" "Driver A" "license_class" "A"
               "hire_date" "2022-01-01T00:00:00.000Z"}]})

(def pattern-7-cursor-paginated
  {"data" {"events" [{"event_id" "EVT-8000" "type" "geofence_enter"
                       "severity" "low" "vehicle_id" "VH-100"
                       "occurred_at" "2025-05-10T06:00:00.000Z"
                       "location" {"lat" 34.05 "lng" -118.24}
                       "metadata" {"speed_mph" 55 "posted_limit_mph" 65}}
                      {"event_id" "EVT-8001" "type" "harsh_brake"
                       "severity" "medium" "vehicle_id" "VH-101"
                       "occurred_at" "2025-05-10T07:00:00.000Z"
                       "location" {"lat" 34.052 "lng" -118.237}
                       "metadata" {"speed_mph" 58 "posted_limit_mph" 65}}]
            "total" 12}
   "pagination" {"endCursor" "EVT-8001" "hasNextPage" true}})

(def pattern-8-mixed-schema
  {"readings" [{"id" "MX-001" "kind" "temperature" "value" 72.5 "unit" "F"
                "tags" ["hvac" "zone-1"]}
               {"id" "MX-002" "kind" "humidity" "value" 45 "unit" "%"
                "tags" ["hvac"]}
               {"id" "MX-003" "kind" "pressure" "value" 14.7 "unit" "psi"
                "extra_field" "only_on_this_one" "tags" []}
               {"id" "MX-004" "kind" "vibration" "value" 0.03 "unit" "g"
                "nested_detail" {"frequency_hz" 120 "axis" "z"}}
               {"id" "MX-005" "kind" "temperature" "value" nil "unit" "F"
                "tags" ["offline" "fault"]}]})

(def pattern-9-single-object
  {"report" {"report_id" "RPT-9001"
             "generated_at" "2025-05-20T14:30:00.000Z"
             "period" {"start" "2025-04-01" "end" "2025-04-30"}
             "summary" {"total_miles" 125430 "total_fuel_gallons" 18920
                        "avg_mpg" 6.63 "total_drivers" 25}
             "top_drivers" [{"driver_id" "DR-201" "name" "Alice" "miles" 8500}
                            {"driver_id" "DR-202" "name" "Bob" "miles" 7800}]
             "cost_breakdown" {"fuel" 56760 "maintenance" 12400}}})

(def pattern-10-multi-level
  {"data" [{"warehouse_id" "WH-9000" "name" "Warehouse Alpha"
            "zones" [{"zone_id" "Z-0-0" "zone_name" "Zone A"
                      "racks" [{"rack_id" "R-0-0-0" "capacity" 100 "current_items" 40
                                "items" [{"sku" "SKU-1000" "name" "Product 0000"
                                          "quantity" 5 "weight_kg" 1.5}
                                         {"sku" "SKU-1001" "name" "Product 0001"
                                          "quantity" 8 "weight_kg" 2.3}]}]}]
            "updated_at" "2025-05-25T09:00:00.000Z"}]})

;; ---------------------------------------------------------------------------
;; Pattern 1: Simple array — explode "data"
;; ---------------------------------------------------------------------------

(deftest pattern-1-simple-array-extraction
  (let [records (schema-infer/logical-records-from-body pattern-1-simple-array "data")]
    (is (= 2 (count records)))
    (is (= "SA-1000" (get (first records) "id")))))

(deftest pattern-1-simple-array-schema-inference
  (let [records (schema-infer/logical-records-from-body pattern-1-simple-array "data")
        fields  (schema-infer/infer-fields-from-records records {:records_path "data"})]
    (is (>= (count fields) 5) "Should infer id, name, price, in_stock, created_at")
    (is (some #(string/includes? (:path %) "id") fields) "Should have id field")
    ;; Bug 1 fix: paths should contain [] markers for grain planner
    (is (some #(string/includes? (:path %) "[]") fields)
        "Paths should contain [] markers after normalization")))

(deftest pattern-1-simple-array-grain-planner
  (let [records (schema-infer/logical-records-from-body pattern-1-simple-array "data")
        fields  (schema-infer/infer-fields-from-records records {:records_path "data"})
        endpoint {:endpoint_name "test/explode/simple-array" :endpoint_url "test/explode/simple-array"}
        structure (grain-planner/analyze-endpoint-structure endpoint fields)
        recommendation (grain-planner/recommend-endpoint-config endpoint fields
                                                                {:configured-records-path "data"})]
    (is (some? (:grain recommendation))
        "Grain planner should produce a grain (Bug 1 fix)")
    (is (seq (:primary_key_fields recommendation))
        "Should recommend a PK")))

;; ---------------------------------------------------------------------------
;; Pattern 2: Deep nested — explode "response.results.items"
;; ---------------------------------------------------------------------------

(deftest pattern-2-deep-nested-extraction
  (let [records (schema-infer/logical-records-from-body pattern-2-deep-nested
                                                        "response.results.items")]
    (is (= 2 (count records)))
    (is (= "ORD-3000" (get (first records) "order_id")))))

(deftest pattern-2-deep-nested-schema-inference
  (let [records (schema-infer/logical-records-from-body pattern-2-deep-nested
                                                        "response.results.items")
        fields  (schema-infer/infer-fields-from-records records
                  {:records_path "response.results.items"})]
    (is (>= (count fields) 5))
    (is (some #(string/includes? (:path %) "[]") fields)
        "Paths should include [] markers")))

;; ---------------------------------------------------------------------------
;; Pattern 3: Root array — explode "$"
;; ---------------------------------------------------------------------------

(deftest pattern-3-root-array-extraction
  (let [records (schema-infer/logical-records-from-body pattern-3-root-array "$")]
    (is (= 2 (count records)))
    (is (= "SENS-4000" (get (first records) "sensor_id")))))

(deftest pattern-3-root-array-autodetect
  (let [detected (schema-infer/detect-dominant-records-path pattern-3-root-array)]
    (is (= "$[]" detected) "Root array should be detected as $[]")))

;; ---------------------------------------------------------------------------
;; Pattern 4: Parent with child arrays — explode "data"
;; ---------------------------------------------------------------------------

(deftest pattern-4-parent-child-extraction
  (let [records (schema-infer/logical-records-from-body pattern-4-parent-child "data")]
    (is (= 1 (count records)))
    (is (= "INV-5000" (get (first records) "invoice_id")))))

(deftest pattern-4-parent-child-grain-picks-parent
  (let [records (schema-infer/logical-records-from-body pattern-4-parent-child "data")
        fields  (schema-infer/infer-fields-from-records records {:records_path "data"})
        endpoint {:endpoint_name "test/explode/parent-child"
                  :endpoint_url "test/explode/parent-child"}
        recommendation (grain-planner/recommend-endpoint-config endpoint fields
                                                                {:configured-records-path "data"})]
    ;; Bug 3 fix: grain should be "data" with invoice_id as PK, not approvals
    (is (some? (:grain recommendation)))
    (is (= ["invoice_id"] (:primary_key_fields recommendation))
        "PK should be invoice_id, not approved_at (Bug 3 fix)")
    ;; Should identify child entities
    (is (seq (:children recommendation))
        "Should detect line_items and/or approvals as child entities")))

;; ---------------------------------------------------------------------------
;; Pattern 5: Nested objects (flatten) — explode "data"
;; ---------------------------------------------------------------------------

(deftest pattern-5-nested-object-flattening
  (let [records (schema-infer/logical-records-from-body pattern-5-nested-object "data")
        fields  (schema-infer/infer-fields-from-records records {:records_path "data"})]
    (is (some #(string/includes? (:path %) "location") fields)
        "Should flatten location sub-object fields")
    (is (some #(string/includes? (:path %) "config") fields)
        "Should flatten config sub-object fields")
    (is (some #(string/includes? (:path %) "lat") fields)
        "Should have lat from location")))

;; ---------------------------------------------------------------------------
;; Pattern 6: Peer arrays — explode "trucks"
;; ---------------------------------------------------------------------------

(deftest pattern-6-peer-arrays-selects-one
  (let [records (schema-infer/logical-records-from-body pattern-6-peer-arrays "trucks")]
    (is (= 1 (count records)))
    (is (= "TRK-7000" (get (first records) "id")))
    ;; Should NOT include trailers or drivers
    (is (nil? (get (first records) "type"))
        "Should not include trailer fields")))

;; ---------------------------------------------------------------------------
;; Pattern 7: Cursor paginated — explode "data.events"
;; ---------------------------------------------------------------------------

(deftest pattern-7-cursor-paginated-extraction
  (let [records (schema-infer/logical-records-from-body pattern-7-cursor-paginated
                                                        "data.events")]
    (is (= 2 (count records)))
    (is (= "EVT-8000" (get (first records) "event_id")))
    ;; pagination sibling should be ignored
    (is (nil? (get (first records) "endCursor")))))

(deftest pattern-7-cursor-paginated-flattens-nested
  (let [records (schema-infer/logical-records-from-body pattern-7-cursor-paginated "data.events")
        fields  (schema-infer/infer-fields-from-records records {:records_path "data.events"})]
    (is (some #(string/includes? (:path %) "location") fields)
        "Should flatten nested location object")
    (is (some #(string/includes? (:path %) "metadata") fields)
        "Should flatten nested metadata object")))

;; ---------------------------------------------------------------------------
;; Pattern 8: Mixed/inconsistent schemas — explode "readings"
;; ---------------------------------------------------------------------------

(deftest pattern-8-mixed-schema-handles-sparse-fields
  (let [records (schema-infer/logical-records-from-body pattern-8-mixed-schema "readings")
        fields  (schema-infer/infer-fields-from-records records {:records_path "readings"})]
    (is (= 5 (count records)))
    ;; Common fields should have high coverage
    (let [id-field (first (filter #(string/ends-with? (:path %) "id") fields))]
      (is (some? id-field))
      (is (= 1.0 (:sample_coverage id-field))))
    ;; Sparse field should have low coverage
    (let [extra (first (filter #(string/includes? (:path %) "extra_field") fields))]
      (is (some? extra) "Should detect extra_field even if sparse")
      (is (< (:sample_coverage extra) 0.5)))))

(deftest pattern-8-mixed-schema-handles-nulls
  (let [records (schema-infer/logical-records-from-body pattern-8-mixed-schema "readings")
        fields  (schema-infer/infer-fields-from-records records {:records_path "readings"})
        value-field (first (filter #(string/ends-with? (:path %) "value") fields))]
    (is (some? value-field))
    (is (:nullable value-field) "value field should be nullable (one record has null)")))

;; ---------------------------------------------------------------------------
;; Pattern 9: Single object — explode "report"
;; ---------------------------------------------------------------------------

(deftest pattern-9-single-object-extraction
  (let [records (schema-infer/logical-records-from-body pattern-9-single-object "report")]
    (is (= 1 (count records)))
    (is (= "RPT-9001" (get (first records) "report_id")))))

(deftest pattern-9-single-object-flattens-nested
  (let [records (schema-infer/logical-records-from-body pattern-9-single-object "report")
        fields  (schema-infer/infer-fields-from-records records {:records_path "report"})]
    (is (some #(string/includes? (:path %) "summary") fields)
        "Should flatten summary sub-object")
    (is (some #(string/includes? (:path %) "total_miles") fields)
        "Should have total_miles from summary")
    (is (some #(string/includes? (:path %) "top_drivers") fields)
        "Should detect top_drivers array")))

;; ---------------------------------------------------------------------------
;; Pattern 10: Multi-level nested — explode "data"
;; ---------------------------------------------------------------------------

(deftest pattern-10-multi-level-extraction
  (let [records (schema-infer/logical-records-from-body pattern-10-multi-level "data")]
    (is (= 1 (count records)))
    (is (= "WH-9000" (get (first records) "warehouse_id")))))

(deftest pattern-10-multi-level-depth-reaches-items
  (let [records (schema-infer/logical-records-from-body pattern-10-multi-level "data")
        fields  (schema-infer/infer-fields-from-records records {:records_path "data"})]
    ;; Bug 2 fix: with max-depth 6, rack and item fields should be present
    (is (some #(string/includes? (:path %) "rack_id") fields)
        "Should reach rack_id (depth 5) — Bug 2 fix")
    (is (some #(string/includes? (:path %) "sku") fields)
        "Should reach sku (depth 6) — Bug 2 fix")
    (is (some #(string/includes? (:path %) "weight_kg") fields)
        "Should reach weight_kg at deepest level")))

(deftest pattern-10-multi-level-autodetect-finds-deepest-array
  (let [detected (schema-infer/detect-dominant-records-path pattern-10-multi-level)]
    (is (some? detected))
    (is (string/includes? detected "data")
        "Auto-detection should find an array path under data")))
