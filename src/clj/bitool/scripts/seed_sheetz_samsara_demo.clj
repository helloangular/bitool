(ns bitool.scripts.seed-sheetz-samsara-demo
  (:require [bitool.control-plane :as control-plane]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.modeling.automation :as modeling]
            [bitool.pipeline.compiler :as pipeline-compiler]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.string :as string]))

(def ^:private graph-name "Sheetz Samsara Analytics Demo")
(def ^:private unique-graph-name "Sheetz Samsara Analytics Demo - Unique Endpoints")
(def ^:private canonical-http-graph-name "Sheetz Samsara Analytics Demo - Canonical HTTP Endpoints")
(def ^:private created-by "codex-demo-seed")

(def ^:private extra-demo-routes
  [{:method "POST" :path "/fleet/trailers"}
   {:method "POST" :path "/fleet/maintenance/dvirs"}
   {:method "POST" :path "/fleet/routes"}
   {:method "POST" :path "/fleet/documents"}])

(def ^:private singleton-routes
  #{"/me" "/user-roles"})

(def ^:private unique-endpoint-paths
  ["/fleet/vehicles"
   "/fleet/vehicles/stats"
   "/fleet/vehicles/locations"
   "/fleet/vehicles/fuel-energy"
   "/fleet/vehicles/:id/safety/score"
   "/fleet/drivers"
   "/fleet/drivers/:id/safety/score"
   "/fleet/drivers/:id/tachograph-files"
   "/fleet/drivers/vehicle-assignments"
   "/fleet/document-types"
   "/fleet/safety/events"
   "/fleet/safety/scores/vehicles"
   "/fleet/safety/scores/drivers"
   "/fleet/hos/logs"
   "/fleet/hos/violations"
   "/fleet/hos/clocks"
   "/fleet/trailers"
   "/fleet/trailers/locations"
   "/fleet/trailers/stats"
   "/fleet/assets"
   "/fleet/assets/locations"
   "/fleet/assets/reefers/stats"
   "/fleet/maintenance/dvirs"
   "/fleet/defects"
   "/fleet/routes"
   "/fleet/dispatch/routes"
   "/fleet/dispatch/jobs"
   "/fleet/ifta/summary"
   "/fleet/geofences"
   "/addresses"
   "/tags"
   "/contacts"
   "/webhooks"
   "/fleet/documents"
   "/alerts/configurations"
   "/alerts"
   "/industrial/data"
   "/sensors/list"
   "/sensors/temperature"
   "/sensors/humidity"
   "/sensors/door"
   "/fleet/equipment"
   "/fleet/equipment/locations"
   "/fleet/equipment/stats"
   "/gateways"
   "/industrial/assets"
   "/industrial/data-inputs"
   "/users"
   "/user-roles"])

(def ^:private canonical-http-endpoint-paths
  (->> unique-endpoint-paths
       (remove #{"/webhooks" "/user-roles"})
       vec))

(def ^:private dynamic-endpoint-overrides
  {"fleet/vehicles"
   {:entity-kind "dimension"
    :silver-model "silver_vehicle_master"
    :silver-table "main.silver.silver_vehicle_master"
    :business-keys ["vehicle_id"]
    :processing-policy {:business_keys ["vehicle_id"]
                        :event_time_column "updated_at"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "vehicle_id" :type "STRING" :nullable false}
                      {:path "$.data[].name" :column_name "vehicle_name" :type "STRING"}
                      {:path "$.data[].vin" :column_name "vin" :type "STRING"}
                      {:path "$.data[].make" :column_name "make" :type "STRING"}
                      {:path "$.data[].model" :column_name "model" :type "STRING"}
                      {:path "$.data[].year" :column_name "vehicle_year" :type "BIGINT"}
                      {:path "$.data[].licensePlate" :column_name "license_plate" :type "STRING"}
                      {:path "$.data[].createdAtTime" :column_name "created_at" :type "TIMESTAMP"}
                      {:path "$.data[].updatedAtTime" :column_name "updated_at" :type "TIMESTAMP"}]}

   "fleet/drivers"
   {:entity-kind "dimension"
    :silver-model "silver_driver_master"
    :silver-table "main.silver.silver_driver_master"
    :business-keys ["driver_id"]
    :processing-policy {:business_keys ["driver_id"]
                        :event_time_column "updated_at"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "driver_id" :type "STRING" :nullable false}
                      {:path "$.data[].name" :column_name "driver_name" :type "STRING"}
                      {:path "$.data[].username" :column_name "username" :type "STRING"}
                      {:path "$.data[].phone" :column_name "phone" :type "STRING"}
                      {:path "$.data[].email" :column_name "email" :type "STRING"}
                      {:path "$.data[].licenseState" :column_name "license_state" :type "STRING"}
                      {:path "$.data[].driverActivationStatus" :column_name "driver_status" :type "STRING"}
                      {:path "$.data[].createdAtTime" :column_name "created_at" :type "TIMESTAMP"}
                      {:path "$.data[].updatedAtTime" :column_name "updated_at" :type "TIMESTAMP"}]}

   "fleet/dispatch/jobs"
   {:entity-kind "fact"
    :silver-model "silver_dispatch_job"
    :silver-table "main.silver.silver_dispatch_job"
    :business-keys ["job_id"]
    :processing-policy {:business_keys ["job_id"]
                        :event_time_column "scheduled_start"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "job_id" :type "STRING" :nullable false}
                      {:path "$.data[].routeId" :column_name "route_id" :type "STRING"}
                      {:path "$.data[].status" :column_name "job_status" :type "STRING"}
                      {:path "$.data[].scheduled_start" :column_name "scheduled_start" :type "TIMESTAMP"}
                      {:path "$.data[].scheduled_end" :column_name "scheduled_end" :type "TIMESTAMP"}
                      {:path "$.data[].driverId" :column_name "driver_id" :type "STRING"}
                      {:path "$.data[].driverName" :column_name "driver_name" :type "STRING"}
                      {:path "$.data[].vehicleId" :column_name "vehicle_id" :type "STRING"}
                      {:path "$.data[].vehicleName" :column_name "vehicle_name" :type "STRING"}]}

   "fleet/vehicles/stats"
   {:entity-kind "fact"
    :silver-model "silver_vehicle_stat"
    :silver-table "main.silver.silver_vehicle_stat"
    :business-keys ["vehicle_id" "stat_time"]
    :processing-policy {:business_keys ["vehicle_id" "stat_time"]
                        :event_time_column "stat_time"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "vehicle_id" :type "STRING" :nullable false}
                      {:path "$.data[].time" :column_name "stat_time" :type "TIMESTAMP" :nullable false}
                      {:path "$.data[].engineStates[0].value" :column_name "engine_state" :type "STRING"}
                      {:path "$.data[].fuelPercents[0].value" :column_name "fuel_percent" :type "DOUBLE"}
                      {:path "$.data[].obdOdometerMeters[0].value" :column_name "odometer_meters" :type "DOUBLE"}
                      {:path "$.data[].gps[0].speedMilesPerHour" :column_name "speed_mph" :type "DOUBLE"}
                      {:path "$.data[].obdEngineSeconds[0].value" :column_name "engine_seconds" :type "DOUBLE"}]}

   "fleet/vehicles/fuel-energy"
   {:entity-kind "fact"
    :silver-model "silver_vehicle_fuel_energy"
    :silver-table "main.silver.silver_vehicle_fuel_energy"
    :business-keys ["vehicle_id" "reading_start_time"]
    :processing-policy {:business_keys ["vehicle_id" "reading_start_time"]
                        :event_time_column "reading_start_time"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].vehicleId" :column_name "vehicle_id" :type "STRING" :nullable false}
                      {:path "$.data[].vehicleName" :column_name "vehicle_name" :type "STRING"}
                      {:path "$.data[].startTime" :column_name "reading_start_time" :type "TIMESTAMP" :nullable false}
                      {:path "$.data[].endTime" :column_name "reading_end_time" :type "TIMESTAMP"}
                      {:path "$.data[].fuelConsumedGallons" :column_name "fuel_consumed_gallons" :type "DOUBLE"}
                      {:path "$.data[].distanceMiles" :column_name "distance_miles" :type "DOUBLE"}
                      {:path "$.data[].fuelEconomyMpg" :column_name "fuel_economy_mpg" :type "DOUBLE"}
                      {:path "$.data[].idleHours" :column_name "idle_hours" :type "DOUBLE"}
                      {:path "$.data[].idleFuelGallons" :column_name "idle_fuel_gallons" :type "DOUBLE"}
                      {:path "$.data[].co2EmissionsKg" :column_name "co2_emissions_kg" :type "DOUBLE"}]}

   "fleet/safety/events"
   {:entity-kind "fact"
    :silver-model "silver_safety_event"
    :silver-table "main.silver.silver_safety_event"
    :business-keys ["safety_event_id"]
    :processing-policy {:business_keys ["safety_event_id"]
                        :event_time_column "event_time"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "safety_event_id" :type "STRING" :nullable false}
                      {:path "$.data[].time" :column_name "event_time" :type "TIMESTAMP" :nullable false}
                      {:path "$.data[].type" :column_name "event_type" :type "STRING"}
                      {:path "$.data[].severity" :column_name "severity" :type "STRING"}
                      {:path "$.data[].driver.id" :column_name "driver_id" :type "STRING"}
                      {:path "$.data[].driver.name" :column_name "driver_name" :type "STRING"}
                      {:path "$.data[].vehicle.id" :column_name "vehicle_id" :type "STRING"}
                      {:path "$.data[].vehicle.name" :column_name "vehicle_name" :type "STRING"}
                      {:path "$.data[].speedMph" :column_name "speed_mph" :type "DOUBLE"}
                      {:path "$.data[].behaviorLabel" :column_name "behavior_label" :type "STRING"}
                      {:path "$.data[].coachable" :column_name "coachable" :type "BOOLEAN"}]}

   "sensors/temperature"
   {:entity-kind "fact"
    :silver-model "silver_temperature_reading"
    :silver-table "main.silver.silver_temperature_reading"
    :business-keys ["sensor_id" "recorded_at"]
    :processing-policy {:business_keys ["sensor_id" "recorded_at"]
                        :event_time_column "recorded_at"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "sensor_id" :type "STRING" :nullable false}
                      {:path "$.data[].name" :column_name "sensor_name" :type "STRING"}
                      {:path "$.data[].ambientTemperature" :column_name "ambient_temperature_c" :type "DOUBLE"}
                      {:path "$.data[].probeTemperature" :column_name "probe_temperature_c" :type "DOUBLE"}
                      {:path "$.data[].currentTime" :column_name "recorded_at" :type "TIMESTAMP" :nullable false}]}

   "fleet/assets/reefers/stats"
   {:entity-kind "fact"
    :silver-model "silver_reefer_stat"
    :silver-table "main.silver.silver_reefer_stat"
    :business-keys ["reefer_id" "reading_time"]
    :processing-policy {:business_keys ["reefer_id" "reading_time"]
                        :event_time_column "reading_time"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "reefer_id" :type "STRING" :nullable false}
                      {:path "$.data[].assetName" :column_name "asset_name" :type "STRING"}
                      {:path "$.data[].trailerId" :column_name "trailer_id" :type "STRING"}
                      {:path "$.data[].ambientTempMilliC" :column_name "ambient_temp_millic" :type "DOUBLE"}
                      {:path "$.data[].fuelPercent" :column_name "fuel_percent" :type "DOUBLE"}
                      {:path "$.data[].engineHours" :column_name "engine_hours" :type "DOUBLE"}
                      {:path "$.data[].returnAirTempMilliC" :column_name "return_air_temp_millic" :type "DOUBLE"}
                      {:path "$.data[].time" :column_name "reading_time" :type "TIMESTAMP" :nullable false}]}

   "fleet/defects"
   {:entity-kind "fact"
    :silver-model "silver_defect_event"
    :silver-table "main.silver.silver_defect_event"
    :business-keys ["defect_id"]
    :processing-policy {:business_keys ["defect_id"]
                        :event_time_column "created_at"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "defect_id" :type "STRING" :nullable false}
                      {:path "$.data[].dvirId" :column_name "dvir_id" :type "STRING"}
                      {:path "$.data[].comment" :column_name "defect_comment" :type "STRING"}
                      {:path "$.data[].defectType" :column_name "defect_type" :type "STRING"}
                      {:path "$.data[].isResolved" :column_name "is_resolved" :type "BOOLEAN"}
                      {:path "$.data[].resolvedAt" :column_name "resolved_at" :type "TIMESTAMP"}
                      {:path "$.data[].createdAtTime" :column_name "created_at" :type "TIMESTAMP" :nullable false}]}

   "fleet/maintenance/dvirs"
   {:entity-kind "fact"
    :silver-model "silver_dvir_event"
    :silver-table "main.silver.silver_dvir_event"
    :business-keys ["dvir_id"]
    :processing-policy {:business_keys ["dvir_id"]
                        :event_time_column "start_time"
                        :ordering_strategy "latest_event_time_wins"}
    :inferred-fields [{:path "$.data[].id" :column_name "dvir_id" :type "STRING" :nullable false}
                      {:path "$.data[].vehicleId" :column_name "vehicle_id" :type "STRING"}
                      {:path "$.data[].vehicleName" :column_name "vehicle_name" :type "STRING"}
                      {:path "$.data[].driverId" :column_name "driver_id" :type "STRING"}
                      {:path "$.data[].driverName" :column_name "driver_name" :type "STRING"}
                      {:path "$.data[].inspectionType" :column_name "inspection_type" :type "STRING"}
                      {:path "$.data[].status" :column_name "dvir_status" :type "STRING"}
                      {:path "$.data[].startTime" :column_name "start_time" :type "TIMESTAMP" :nullable false}
                      {:path "$.data[].endTime" :column_name "end_time" :type "TIMESTAMP"}
                      {:path "$.data[].location.formattedAddress" :column_name "location_address" :type "STRING"}]}})

(def ^:private gold-patches
  {"gold_fleet_utilization_daily"
   {:semantic_grain "vehicle_day"
    :source_model "silver_vehicle_stat"
    :depends_on ["silver_vehicle_stat" "silver_dispatch_job"]
    :group_by ["vehicle_id" "event_date"]
    :materialization {:mode "merge" :keys ["vehicle_id" "event_date"]}
    :columns [{:target_column "vehicle_id" :type "STRING" :nullable false :role "dimension"
               :expression "silver.`vehicle_id`"}
              {:target_column "event_date" :type "DATE" :nullable false :role "time_dimension"
               :expression "CAST(silver.`stat_time` AS DATE)"}
              {:target_column "avg_speed_mph" :type "DOUBLE" :nullable true :role "measure"
               :expression "AVG(silver.`speed_mph`)"}
              {:target_column "avg_fuel_percent" :type "DOUBLE" :nullable true :role "measure"
               :expression "AVG(silver.`fuel_percent`)"}
              {:target_column "max_odometer_meters" :type "DOUBLE" :nullable true :role "measure"
               :expression "MAX(silver.`odometer_meters`)"}
              {:target_column "record_count" :type "BIGINT" :nullable false :role "measure"
               :expression "COUNT(*)"}]
    :explanations ["Daily fleet utilization summary by vehicle and operating day."
                   "Designed to back the Fleet Utilization dashboard in the Sheetz demo."]}

   "gold_driver_safety_daily"
   {:semantic_grain "driver_day"
    :source_model "silver_safety_event"
    :depends_on ["silver_safety_event" "silver_driver_master"]
    :group_by ["driver_id" "event_date"]
    :materialization {:mode "merge" :keys ["driver_id" "event_date"]}
    :columns [{:target_column "driver_id" :type "STRING" :nullable true :role "dimension"
               :expression "silver.`driver_id`"}
              {:target_column "event_date" :type "DATE" :nullable false :role "time_dimension"
               :expression "CAST(silver.`event_time` AS DATE)"}
              {:target_column "total_events" :type "BIGINT" :nullable false :role "measure"
               :expression "COUNT(*)"}
              {:target_column "major_or_severe_events" :type "BIGINT" :nullable false :role "measure"
               :expression "SUM(CASE WHEN silver.`severity` IN ('major','severe') THEN 1 ELSE 0 END)"}
              {:target_column "coachable_events" :type "BIGINT" :nullable false :role "measure"
               :expression "SUM(CASE WHEN silver.`coachable` = TRUE THEN 1 ELSE 0 END)"}]
    :explanations ["Daily driver safety rollup for coaching and risk review."
                   "Designed to back the Driver Safety dashboard in the Sheetz demo."]}

   "gold_fuel_efficiency_daily"
   {:semantic_grain "vehicle_day"
    :source_model "silver_vehicle_fuel_energy"
    :depends_on ["silver_vehicle_fuel_energy"]
    :group_by ["vehicle_id" "event_date"]
    :materialization {:mode "merge" :keys ["vehicle_id" "event_date"]}
    :columns [{:target_column "vehicle_id" :type "STRING" :nullable false :role "dimension"
               :expression "silver.`vehicle_id`"}
              {:target_column "event_date" :type "DATE" :nullable false :role "time_dimension"
               :expression "CAST(silver.`reading_start_time` AS DATE)"}
              {:target_column "total_fuel_gallons" :type "DOUBLE" :nullable true :role "measure"
               :expression "SUM(silver.`fuel_consumed_gallons`)"}
              {:target_column "total_distance_miles" :type "DOUBLE" :nullable true :role "measure"
               :expression "SUM(silver.`distance_miles`)"}
              {:target_column "avg_fuel_economy_mpg" :type "DOUBLE" :nullable true :role "measure"
               :expression "AVG(silver.`fuel_economy_mpg`)"}
              {:target_column "total_idle_hours" :type "DOUBLE" :nullable true :role "measure"
               :expression "SUM(silver.`idle_hours`)"}]
    :explanations ["Daily fuel and idling rollup by vehicle."
                   "Designed to back the Fuel and Idling dashboard in the Sheetz demo."]}

   "gold_cold_chain_compliance_daily"
   {:semantic_grain "sensor_day"
    :source_model "silver_temperature_reading"
    :depends_on ["silver_temperature_reading" "silver_reefer_stat"]
    :group_by ["sensor_id" "event_date"]
    :materialization {:mode "merge" :keys ["sensor_id" "event_date"]}
    :columns [{:target_column "sensor_id" :type "STRING" :nullable false :role "dimension"
               :expression "silver.`sensor_id`"}
              {:target_column "event_date" :type "DATE" :nullable false :role "time_dimension"
               :expression "CAST(silver.`recorded_at` AS DATE)"}
              {:target_column "avg_ambient_temperature_c" :type "DOUBLE" :nullable true :role "measure"
               :expression "AVG(silver.`ambient_temperature_c`)"}
              {:target_column "avg_probe_temperature_c" :type "DOUBLE" :nullable true :role "measure"
               :expression "AVG(silver.`probe_temperature_c`)"}
              {:target_column "high_temp_breach_count" :type "BIGINT" :nullable false :role "measure"
               :expression "SUM(CASE WHEN silver.`probe_temperature_c` > 41 THEN 1 ELSE 0 END)"}
              {:target_column "low_temp_breach_count" :type "BIGINT" :nullable false :role "measure"
               :expression "SUM(CASE WHEN silver.`probe_temperature_c` < -10 THEN 1 ELSE 0 END)"}]
    :explanations ["Daily cold-chain compliance rollup from temperature telemetry."
                   "Designed to back the Cold Chain dashboard in the Sheetz demo."]}

   "gold_asset_health_daily"
   {:semantic_grain "vehicle_day"
    :source_model "silver_dvir_event"
    :depends_on ["silver_dvir_event" "silver_defect_event" "silver_vehicle_master"]
    :group_by ["vehicle_id" "event_date"]
    :materialization {:mode "merge" :keys ["vehicle_id" "event_date"]}
    :columns [{:target_column "vehicle_id" :type "STRING" :nullable true :role "dimension"
               :expression "silver.`vehicle_id`"}
              {:target_column "event_date" :type "DATE" :nullable false :role "time_dimension"
               :expression "CAST(silver.`start_time` AS DATE)"}
              {:target_column "dvir_count" :type "BIGINT" :nullable false :role "measure"
               :expression "COUNT(*)"}
              {:target_column "open_issue_count" :type "BIGINT" :nullable false :role "measure"
               :expression "SUM(CASE WHEN silver.`dvir_status` <> 'defects_corrected' THEN 1 ELSE 0 END)"}
              {:target_column "completed_issue_count" :type "BIGINT" :nullable false :role "measure"
               :expression "SUM(CASE WHEN silver.`dvir_status` = 'defects_corrected' THEN 1 ELSE 0 END)"}]
    :explanations ["Daily DVIR-based asset health rollup by vehicle."
                   "Designed to back the Asset Health dashboard in the Sheetz demo."]}})

(defn- connector-knowledge []
  (-> "connector-knowledge/samsara.edn"
      io/resource
      slurp
      edn/read-string))

(defn- route-definitions []
  (let [pattern #"app\.(get|post|patch|put|delete)\(\"([^\"]+)\""
        server-js (slurp "mock-api/server.js")]
    (->> (string/split-lines server-js)
         (keep (fn [line]
                 (when-let [[_ method path] (re-find pattern line)]
                   {:method (string/upper-case method)
                    :path path})))
         vec)))

(defn- business-get-routes []
  (->> (route-definitions)
       (filter #(= "GET" (:method %)))
       (remove #(or (= "/health" (:path %))
                    (string/starts-with? (:path %) "/test/")))
       (reduce (fn [acc route]
                 (if (some #(= (:path %) (:path route)) acc)
                   acc
                   (conj acc route)))
               [])))

(defn- curated-routes []
  (vec (concat (business-get-routes) extra-demo-routes)))

(defn- unique-curated-routes []
  (->> (business-get-routes)
       (filter #(some #{(:path %)} unique-endpoint-paths))
       vec))

(defn- canonical-http-routes []
  (->> (business-get-routes)
       (filter #(some #{(:path %)} canonical-http-endpoint-paths))
       vec))

(defn- endpoint-name [path]
  (string/replace-first path #"^/" ""))

(defn- sanitize-segment [s]
  (-> s
      (string/replace ":" "by_")
      (string/replace #"[^A-Za-z0-9]+" "_")
      (string/replace #"_{2,}" "_")
      (string/replace #"^_|_$" "")
      string/lower-case))

(defn- bronze-table-name [path]
  (str "main.bronze.samsara_" (sanitize-segment (endpoint-name path)) "_raw"))

(defn- knowledge-by-path []
  (into {}
        (map (juxt :path identity))
        (:endpoints (connector-knowledge))))

(defn- knowledge-field->inferred-field [explode-path {:keys [name type]}]
  {:path (str "$." explode-path "[]." name)
   :column_name (sanitize-segment name)
   :type type
   :nullable true})

(defn- detail-route? [path]
  (string/includes? path ":"))

(defn- collection-route? [{:keys [method path]}]
  (and (= "GET" method)
       (not (detail-route? path))
       (not (contains? singleton-routes path))))

(defn- watermark-column [endpoint-name knowledge]
  (or (:watermark knowledge)
      (cond
        (re-find #"stats/history|locations/history" endpoint-name) "time"
        (re-find #"fuel-energy" endpoint-name) "startTime"
        (re-find #"dispatch/jobs" endpoint-name) "scheduled_start"
        (re-find #"safety/events|safety-events" endpoint-name) "time"
        (re-find #"maintenance/dvirs|dvirs/history" endpoint-name) "startTime"
        (re-find #"defects/history|defects$" endpoint-name) "createdAtTime"
        (re-find #"sensors/(temperature|humidity|door)" endpoint-name) "currentTime"
        (re-find #"alerts$|industrial/data" endpoint-name) "time"
        (re-find #"vehicles|drivers|trailers|assets|equipment|documents|contacts|addresses|tags|users" endpoint-name) "updatedAtTime"
        :else nil)))

(defn- load-type [endpoint-name knowledge]
  (if (or (:watermark knowledge)
          (re-find #"stats|locations|fuel-energy|safety|hos|dispatch|alerts|industrial/data|sensors|defects|dvirs|reefers" endpoint-name))
    "incremental"
    "full"))

(defn- primary-keys [endpoint-name knowledge]
  (cond
    (some? (:key knowledge)) [(sanitize-segment (:key knowledge))]
    (re-find #"fuel-energy" endpoint-name) ["vehicle_id" "reading_start_time"]
    (re-find #"maintenance/dvirs" endpoint-name) ["dvir_id"]
    (re-find #"dispatch/jobs" endpoint-name) ["job_id"]
    (re-find #"sensors/temperature" endpoint-name) ["sensor_id" "recorded_at"]
    :else ["id"]))

(defn- default-inferred-fields [path knowledge]
  (let [endpoint-key (endpoint-name path)]
    (or (get-in dynamic-endpoint-overrides [endpoint-key :inferred-fields])
        (when (and knowledge (:explode knowledge) (seq (:fields knowledge)))
          (mapv #(knowledge-field->inferred-field (:explode knowledge) %) (:fields knowledge)))
        [])))

(defn- endpoint-config [{:keys [method path]}]
  (let [endpoint-key        (endpoint-name path)
        knowledge           (get (knowledge-by-path) endpoint-key)
        override            (get dynamic-endpoint-overrides endpoint-key)
        watermark           (watermark-column endpoint-key knowledge)
        inferred-fields     (default-inferred-fields path knowledge)
        collection?         (collection-route? {:method method :path path})
        requested-load-type (if (= "GET" method) (load-type endpoint-key knowledge) "full")
        load-type'          (if (and (not= requested-load-type "full")
                                     (string/blank? watermark))
                              "full"
                              requested-load-type)
        endpoint-selected   (if (seq inferred-fields)
                              (mapv :path inferred-fields)
                              (if collection? ["$.data[].id"] []))]
    {:endpoint_name endpoint-key
     :endpoint_url path
     :http_method method
     :enabled true
     :schema_mode "infer"
     :schema_evolution_mode "advisory"
     :schema_enforcement_mode "permissive"
     :schema_review_state "optional"
     :sample_records 100
     :max_inferred_columns 100
     :type_inference_enabled true
     :load_type load-type'
     :pagination_strategy (if (and (= "GET" method) collection?) "cursor" "none")
     :pagination_location "query"
     :cursor_field (if (and (= "GET" method) collection?) "pagination.endCursor" "")
     :cursor_param "after"
     :watermark_column (or watermark "")
     :time_field (or watermark "")
     :time_param "updated_since"
     :watermark_overlap_minutes (if (= load-type' "incremental") 15 0)
     :primary_key_fields (primary-keys endpoint-key knowledge)
     :selected_nodes endpoint-selected
     :json_explode_rules (if collection?
                           [{:path "$.data[]" :mode "records"}]
                           [])
     :bronze_table_name (bronze-table-name path)
     :silver_table_name (or (:silver-table override) "")
     :page_size 100
     :retry_policy {:max_retries 3 :base_backoff_ms 1000}
     :inferred_fields inferred-fields}))

(defn- silver-plans []
  (->> dynamic-endpoint-overrides
       (keep (fn [[endpoint-name cfg]]
               (when-let [target-model (:silver-model cfg)]
                 {:source-endpoint endpoint-name
                  :target-model target-model
                  :entity-kind (:entity-kind cfg)
                  :business-keys (:business-keys cfg)
                  :processing-policy (:processing-policy cfg)})))
       (sort-by :target-model)
       vec))

(defn- gold-plans []
  [{:target-model "gold_asset_health_daily"
    :grain "day"
    :depends-on ["silver_defect_event" "silver_vehicle_master"]
    :measures [{:name "dvir_count"} {:name "open_issue_count"} {:name "completed_issue_count"}]
    :dimensions [{:name "vehicle_id"} {:name "event_date"}]}
   {:target-model "gold_cold_chain_compliance_daily"
    :grain "day"
    :depends-on ["silver_reefer_stat"]
    :measures [{:name "avg_ambient_temperature_c"} {:name "avg_probe_temperature_c"}
               {:name "high_temp_breach_count"} {:name "low_temp_breach_count"}]
    :dimensions [{:name "sensor_id"} {:name "event_date"}]}
   {:target-model "gold_driver_safety_daily"
    :grain "day"
    :depends-on ["silver_driver_master"]
    :measures [{:name "total_events"} {:name "major_or_severe_events"} {:name "coachable_events"}]
    :dimensions [{:name "driver_id"} {:name "event_date"}]}
   {:target-model "gold_fleet_utilization_daily"
    :grain "day"
    :depends-on ["silver_vehicle_master" "silver_dispatch_job"]
    :measures [{:name "avg_speed_mph"} {:name "avg_fuel_percent"} {:name "max_odometer_meters"} {:name "record_count"}]
    :dimensions [{:name "vehicle_id"} {:name "event_date"}]}
   {:target-model "gold_fuel_efficiency_daily"
    :grain "day"
    :depends-on ["silver_vehicle_master"]
    :measures [{:name "total_fuel_gallons"} {:name "total_distance_miles"}
               {:name "avg_fuel_economy_mpg"} {:name "total_idle_hours"}]
    :dimensions [{:name "vehicle_id"} {:name "event_date"}]}])

(defn- pipeline-spec [{:keys [connection-id endpoint-mode]
                       :or {endpoint-mode :all}}]
  (let [unique-only?   (= endpoint-mode :unique-only)
        canonical?     (= endpoint-mode :canonical-http)
        endpoint-routes (cond
                          canonical? (canonical-http-routes)
                          unique-only? (unique-curated-routes)
                          :else (curated-routes))]
    {:pipeline-id (cond
                    canonical? "sheetz_samsara_analytics_demo_canonical_http"
                    unique-only? "sheetz_samsara_analytics_demo_unique"
                    :else "sheetz_samsara_analytics_demo")
   :pipeline-name (cond
                    canonical? canonical-http-graph-name
                    unique-only? unique-graph-name
                    :else graph-name)
   :bronze-nodes [{:node-ref "api1"
                   :node-type "api-connection"
                   :config {:name "Sheetz Samsara API"
                            :api_name "sheetz_samsara_demo"
                            :source_system "samsara"
                            :specification_url "https://raw.githubusercontent.com/samsarahq/api-docs/master/swagger.json"
                            :base_url "http://localhost:3001"
                            :auth_ref {:type "bearer" :token "mock-samsara-token"}
                            :endpoint_configs (mapv endpoint-config endpoint-routes)}}
                  {:node-ref "tg1"
                   :node-type "target"
                   :config {:connection_id connection-id
                            :target_kind "databricks"
                            :catalog "main"
                            :schema "bronze"
                            :table_name "sheetz_samsara_demo_seed"
                            :write_mode "append"
                            :table_format "delta"
                            :create_table true
                            :partition_columns ["partition_date"]}}]
   :silver-proposals (silver-plans)
   :gold-models (gold-plans)
   :assumptions ["Samsara demo source uses the local mock API on http://localhost:3001."
                 (cond
                   canonical?
                   "Bronze endpoint catalog includes 47 canonical HTTP endpoints for the demo narrative."
                   unique-only?
                   "Bronze endpoint catalog includes only unique-by-shape Samsara endpoints."
                   :else
                   "Bronze endpoint catalog includes 84 configured endpoints for UI review.")
                 "Gold proposals are aligned to the five Sheetz demo report domains."]}))

(defn- persist-target-config!
  [graph-id target-node-id connection-id]
  (let [graph         (db/getGraph graph-id)
        updated-graph (g2/save-target
                       graph
                       target-node-id
                       {:connection_id connection-id
                        :target_kind "databricks"
                        :catalog "main"
                        :schema "bronze"
                        :table_name "sheetz_samsara_demo_seed"
                        :write_mode "append"
                        :table_format "delta"
                        :create_table true
                        :partition_columns ["partition_date"]})]
    (db/insertGraph updated-graph)))

(defn- apply-gold-patches! [gold-results]
  (mapv (fn [result]
          (if-let [patch (get gold-patches (:target_model result))]
            (modeling/update-gold-proposal!
             (:proposal_id result)
             {:proposal patch
              :created_by created-by})
            result))
        gold-results))

(defn- ensure-workspace! [graph-id]
  (control-plane/ensure-control-plane-tables!)
  (control-plane/upsert-workspace!
   {:workspace_key "default"
    :tenant_key "default"
    :workspace_name "Default Workspace"})
  (control-plane/assign-graph-workspace! graph-id "default" created-by))

(defn seed-demo!
  [{:keys [connection-id endpoint-mode]
    :or {connection-id 478
         endpoint-mode :all}}]
  (let [spec          (pipeline-spec {:connection-id nil
                                      :endpoint-mode endpoint-mode})
        bronze        (pipeline-compiler/apply-bronze! spec {:created-by created-by})
        graph-id      (:graph_id bronze)
        _             (ensure-workspace! graph-id)
        silver-plans  (pipeline-compiler/plan-silver-proposals spec bronze)
        silver-result (pipeline-compiler/apply-silver-proposals! silver-plans {:created-by created-by})
        gold-result   (pipeline-compiler/apply-gold-proposals! (pipeline-compiler/plan-gold-models spec)
                                                               {:created-by created-by
                                                                :silver-proposal-ids silver-result})
        gold-result'  (apply-gold-patches! gold-result)
        _             (persist-target-config! graph-id (:target_node_id bronze) connection-id)]
    {:graph_id graph-id
     :graph_version (:graph_version bronze)
     :api_node_id (:api_node_id bronze)
     :target_node_id (:target_node_id bronze)
     :connection_id connection-id
     :endpoint_mode endpoint-mode
     :endpoint_count (count (get-in spec [:bronze-nodes 0 :config :endpoint_configs]))
     :silver_models (mapv #(select-keys % [:proposal_id :target_model :status]) silver-result)
     :gold_models (mapv #(select-keys % [:proposal_id :target_model :status]) gold-result')}))

(defn -main [& args]
  (let [connection-id (some-> (first args) Long/parseLong)
        endpoint-mode (case (some-> (second args) string/lower-case)
                        "unique" :unique-only
                        "canonical" :canonical-http
                        :all)
        result (seed-demo! {:connection-id (or connection-id 478)
                            :endpoint-mode endpoint-mode})]
    (pp/pprint result)
    (shutdown-agents)))
