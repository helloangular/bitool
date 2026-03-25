(ns bitool.ingest-databricks-control-plane-test
  (:require [bitool.control-plane :as control-plane]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.ingest.databricks-control-plane :as dbx-control]
            [bitool.operations :as operations]
            [clojure.test :refer :all]
            [next.jdbc :as jdbc]))

(deftest bootstrap-returns-endpoint-configs-and-postgres-checkpoints
  (with-redefs [db/getGraph (fn [_]
                              {:n {2 {:e {7 {}}}
                                   7 {:na {:btype "Tg"
                                           :connection_id 478
                                           :catalog "main"
                                           :schema "bronze"
                                           :table_name "fleet_vehicles"}}}})
                g2/getData (fn [_ node-id]
                             (when (= 2 node-id)
                               {:source_system "samara"
                                :base_url "https://api.samsara.com"
                                :endpoint_configs [{:endpoint_name "fleet/vehicles"
                                                    :endpoint_url "/fleet/vehicles"
                                                    :http_method "GET"
                                                    :pagination_strategy "cursor"
                                                    :cursor_param "after"
                                                    :cursor_field "endCursor"
                                                    :watermark_column "updatedAtTime"
                                                    :time_param "updated_since"
                                                    :json_explode_rules [{:path "data"}]
                                                    :primary_key_fields ["id"]
                                                    :enabled true}]}))
                db/get-connection (fn [_]
                                    [{:id 478
                                      :dbtype "databricks"
                                      :catalog "main"
                                      :schema "bronze"}])
                control-plane/graph-workspace-context (fn [_]
                                                        {:tenant_key "tenant-a"
                                                         :workspace_key "ops"})
                dbx-control/current-checkpoint (fn [_ _ endpoint-name]
                                                 {:endpoint_name endpoint-name
                                                  :last_successful_watermark "2026-03-25T00:00:00Z"})]
    (let [result (dbx-control/bootstrap 2419 2 {:endpoint-name "fleet/vehicles"})]
      (is (= 2419 (:graph_id result)))
      (is (= "databricks" (:warehouse result)))
      (is (= "ops" (:workspace_key result)))
      (is (= "fleet/vehicles" (get-in result [:endpoints 0 :endpoint_name])))
      (is (= "2026-03-25T00:00:00Z"
             (get-in result [:endpoints 0 :checkpoint :last_successful_watermark])))
      (is (= "main.bronze.fleet_vehicles"
             (get-in result [:endpoints 0 :target_table]))))))

(deftest record-callback-upserts-control-plane-metadata-and-freshness
  (let [sql-calls (atom [])
        freshness-calls (atom [])]
    (with-redefs-fn {#'bitool.ingest.databricks-control-plane/ready? (atom true)
                     #'jdbc/execute! (fn [_ params]
                                       (swap! sql-calls conj params)
                                       [])
                     #'operations/record-endpoint-freshness! (fn [payload]
                                                               (swap! freshness-calls conj payload)
                                                               nil)
                     #'control-plane/graph-workspace-context (fn [_]
                                                               {:tenant_key "tenant-a"
                                                                :workspace_key "ops"})}
      (fn []
        (let [result (dbx-control/record-callback!
                      {:graph_id 2419
                       :api_node_id 2
                       :source_system "samara"
                       :completed_at_utc "2026-03-25T12:00:00Z"
                       :results [{:endpoint_name "fleet/vehicles"
                                  :target_table "main.bronze.fleet_vehicles"
                                  :run_id "11111111-1111-1111-1111-111111111111"
                                  :status "success"
                                  :rows_written 10
                                  :rows_extracted 10
                                  :pages_fetched 1
                                  :max_watermark "2026-03-25T11:59:00Z"
                                  :next_cursor "cursor-2"
                                  :manifests [{:batch_id "11111111-1111-1111-1111-111111111111-b000001"
                                               :batch_seq 1
                                               :status "committed"
                                               :row_count 10
                                               :page_count 1
                                               :max_watermark "2026-03-25T11:59:00Z"}]}]})]
          (is (= 1 (:result_count result)))
          (is (= 3 (count @sql-calls)))
          (is (= "fleet/vehicles" (:endpoint-name (first @freshness-calls))))
          (is (= 10 (:rows-written (first @freshness-calls)))))))))
