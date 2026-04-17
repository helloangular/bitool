(ns bitool.scripts.repair-sheetz-gold-sources
  (:require [bitool.modeling.automation :as modeling]
            [clojure.pprint :as pp]))

(def ^:private source-model-by-target
  {"gold_fleet_utilization_daily" "silver_vehicle_stat"
   "gold_driver_safety_daily" "silver_safety_event"
   "gold_fuel_efficiency_daily" "silver_vehicle_fuel_energy"
   "gold_cold_chain_compliance_daily" "silver_temperature_reading"
   "gold_asset_health_daily" "silver_dvir_event"})

(defn -main
  [& _]
  (let [proposals (modeling/list-gold-proposals {:graph-id 2451 :limit 100})
        updated   (->> proposals
                       (keep (fn [{:keys [proposal_id target_model]}]
                               (when-let [source-model (get source-model-by-target target_model)]
                                 (modeling/update-gold-proposal! proposal_id
                                                                 {:proposal {:source_model source-model}
                                                                  :created_by "script"}))))
                       vec)]
    (pp/pprint {:updated (mapv #(select-keys % [:proposal_id :target_model :status]) updated)})))
