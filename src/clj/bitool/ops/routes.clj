(ns bitool.ops.routes
  (:require [bitool.ops.dashboard     :as dashboard]
            [bitool.ops.queue         :as queue]
            [bitool.ops.source-health :as source-health]
            [bitool.ops.alerts        :as alerts]
            [bitool.ops.admin         :as admin]
            [bitool.ops.correlation   :as correlation]
            [bitool.ops.timeseries    :as timeseries]
            [bitool.ops.schema-drift  :as schema-drift]
            [bitool.config :refer [env]]
            [ring.util.http-response  :as http-response]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;; ── Helpers ──

(defn- parse-int [s default]
  (try
    (if (some? s)
      (Integer/parseInt (str s))
      default)
    (catch Exception _ default)))

(defn- parse-optional-int [s]
  (try (when s (Integer/parseInt (str s))) (catch Exception _ nil)))

(defn- parse-bool [v]
  (contains? #{"true" "1" "yes" "on"} (string/lower-case (str v))))

(defn- parse-uuid!
  [value field]
  (try
    (java.util.UUID/fromString (str value))
    (catch Exception _
      (throw (ex-info "Invalid UUID parameter"
                      {:status 400
                       :error "bad_request"
                       :field field
                       :value value})))))

(defn- parse-long!
  [value field]
  (try
    (Long/parseLong (str value))
    (catch Exception _
      (throw (ex-info "Invalid numeric parameter"
                      {:status 400
                       :error "bad_request"
                       :field field
                       :value value})))))

(defn- parse-instant!
  [value field]
  (when (some? value)
    (try
      (cond
        (instance? java.time.Instant value) value
        (instance? java.util.Date value) (.toInstant ^java.util.Date value)
        :else (java.time.Instant/parse (str value)))
      (catch Exception _
        (throw (ex-info "Invalid timestamp parameter"
                        {:status 400
                         :error "bad_request"
                         :field field
                         :value value}))))))

(defn- rbac-enabled?
  []
  (let [raw (get env :bitool-rbac-enabled)]
    (if (nil? raw)
      false
      (contains? #{"1" "true" "yes" "on"}
                 (some-> raw str string/trim string/lower-case)))))

(defn- request-roles
  [request]
  (let [session-roles (or (get-in request [:session :roles])
                          (some-> (get-in request [:session :role]) vector))]
    (->> (if (sequential? session-roles) session-roles [session-roles])
         (map #(some-> % str string/trim string/lower-case))
         (remove string/blank?)
         set)))

(defn- ensure-authorized!
  [request required-role]
  (when (rbac-enabled?)
    (let [required-role (some-> required-role name string/lower-case)
          admin-role    (or (some-> (get env :bitool-admin-role) str string/trim string/lower-case)
                            "admin")
          roles         (request-roles request)]
      (when-not (or (contains? roles admin-role)
                    (contains? roles required-role))
        (throw (ex-info "Forbidden"
                        {:status 403
                         :error "forbidden"
                         :required_role required-role
                         :roles (vec roles)}))))))

(defn- actor
  [request params]
  (or (:actor params)
      (get-in request [:session :user])
      "operator"))

(defn- ok [data]
  (-> (http-response/ok
        (json/generate-string
          {:ok true
           :data data
           :meta {:timestamp (str (java.time.Instant/now))}}))
      (assoc-in [:headers "Content-Type"] "application/json; charset=utf-8")))

(defn- ok-raw [data]
  (-> (http-response/ok (json/generate-string data))
      (assoc-in [:headers "Content-Type"] "application/json; charset=utf-8")))

(defn- error-response [status error msg]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/generate-string {:ok false :error error :message msg})})

(defmacro with-ops-handler [& body]
  `(try
     (ok (do ~@body))
     (catch clojure.lang.ExceptionInfo e#
       (let [d# (ex-data e#)]
         (error-response (or (:status d#) 400) (or (:error d#) "bad_request") (ex-message e#))))
     (catch Exception e#
       (log/error e# "Ops handler error")
       (error-response 500 "internal_error" (.getMessage e#)))))

;; ── Pipeline Overview ──

(defn pipeline-kpis [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/pipeline-kpis
        {:workspace-key (:workspace_key p)
         :time-range    (or (:time_range p) "24h")}))))

(defn source-status-list [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/source-status-list {:workspace-key (:workspace_key p)}))))

(defn recent-activity [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/recent-activity
        {:workspace-key (:workspace_key p)
         :limit         (parse-int (:limit p) 50)}))))

;; ── Alerts ──

(defn list-alerts [request]
  (let [p (:params request)]
    (with-ops-handler
      (alerts/list-alerts
        {:workspace-key (:workspace_key p)
         :state         (:state p)
         :severity      (:severity p)
         :alert-type    (:alert_type p)
         :limit         (parse-int (:limit p) 100)
         :offset        (parse-int (:offset p) 0)}))))

(defn alert-history [request]
  (let [p (:params request)]
    (with-ops-handler
      (alerts/alert-history
        {:workspace-key (:workspace_key p)
         :limit         (parse-int (:limit p) 100)
         :offset        (parse-int (:offset p) 0)}))))

(defn acknowledge-alert [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (alerts/acknowledge!
       (parse-uuid! (:alert_id (:path-params request)) :alert_id)
       (actor request p)))))

(defn silence-alert [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (alerts/silence!
       (parse-uuid! (:alert_id (:path-params request)) :alert_id)
       (actor request p)
       (or (parse-instant! (:silence_until p) :silence_until)
           (throw (ex-info "silence_until is required"
                           {:status 400
                            :error "bad_request"
                            :field :silence_until})))))))

(defn resolve-alert [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (alerts/resolve!
       (parse-uuid! (:alert_id (:path-params request)) :alert_id)
       (actor request p)))))

;; ── Correlation ──

(defn correlation-from-request [request]
  (with-ops-handler
    (correlation/from-request
     (parse-uuid! (:request_id (:path-params request)) :request_id))))

(defn correlation-from-run [request]
  (with-ops-handler
    (correlation/from-run
     (parse-uuid! (:run_id (:path-params request)) :run_id))))

(defn correlation-from-batch [request]
  (with-ops-handler
    (correlation/from-batch
     (parse-long! (:batch_id (:path-params request)) :batch_id))))

;; ── Queue & Workers ──

(defn queue-status-counts [request]
  (let [p (:params request)]
    (with-ops-handler
      (queue/status-counts
        {:workspace-key (:workspace_key p)
         :time-range    (or (:time_range p) "24h")}))))

(defn queue-list-requests [request]
  (let [p (:params request)]
    (with-ops-handler
      (queue/list-requests
        {:workspace-key (:workspace_key p)
         :status        (:status p)
         :limit         (parse-int (:limit p) 100)
         :offset        (parse-int (:offset p) 0)}))))

(defn queue-bulk-retry [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (queue/bulk-retry!
        {:request-ids     (mapv #(parse-uuid! % :request_ids) (:request_ids p))
         :expected-status (or (:expected_status p) "failed")
         :operator        (actor request p)}))))

(defn queue-bulk-cancel [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (queue/bulk-cancel!
        {:request-ids (mapv #(parse-uuid! % :request_ids) (:request_ids p))
         :operator    (actor request p)}))))

(defn queue-bulk-requeue [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (queue/bulk-requeue!
        {:request-ids (mapv #(parse-uuid! % :request_ids) (:request_ids p))
         :operator    (actor request p)}))))

(defn queue-list-workers [request]
  (let [p (:params request)]
    (with-ops-handler
      (queue/list-workers {:workspace-key (:workspace_key p)}))))

(defn queue-drain-worker [request]
  (let [p (:params request)
        worker-id (:worker_id (:path-params request))]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (queue/drain-worker!
        {:worker-id worker-id
         :operator  (actor request p)}))))

(defn queue-undrain-worker [request]
  (let [p (:params request)
        worker-id (:worker_id (:path-params request))]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (queue/undrain-worker!
        {:worker-id worker-id
         :operator  (actor request p)}))))

(defn queue-force-release [request]
  (let [p (:params request)
        worker-id (:worker_id (:path-params request))]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (queue/force-release!
        {:worker-id worker-id
         :operator  (actor request p)}))))

(defn queue-list-dlq [request]
  (let [p (:params request)]
    (with-ops-handler
      (queue/list-dlq
        {:workspace-key (:workspace_key p)
         :limit         (parse-int (:limit p) 100)
         :offset        (parse-int (:offset p) 0)}))))

;; ── Source Health ──

(defn sources-kafka [request]
  (let [p (:params request)]
    (with-ops-handler
      (source-health/kafka-sources {:workspace-key (:workspace_key p)}))))

(defn sources-file [request]
  (let [p (:params request)]
    (with-ops-handler
      (source-health/file-sources {:workspace-key (:workspace_key p)}))))

(defn sources-api [request]
  (let [p (:params request)]
    (with-ops-handler
      (source-health/api-sources {:workspace-key (:workspace_key p)}))))

(defn sources-data-loss-risk [request]
  (let [p (:params request)]
    (with-ops-handler
      (source-health/data-loss-risk {:workspace-key (:workspace_key p)}))))

(defn source-circuit-breaker [request]
  (let [source-id (:source_id (:path-params request))]
    (with-ops-handler
      (source-health/circuit-breaker-state source-id))))

(defn source-circuit-breaker-reset [request]
  (let [source-id (:source_id (:path-params request))
        p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (source-health/reset-circuit-breaker! source-id (actor request p)))))

(defn kafka-stream-detail [request]
  (let [source-id (:source_id (:path-params request))]
    (with-ops-handler
      (source-health/kafka-stream-detail source-id))))

;; ── Batches & Manifests ──

(defn batch-summary [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/batch-summary {:workspace-key (:workspace_key p)}))))

(defn batch-detail [request]
  (let [batch-id (:batch_id (:path-params request))]
    (with-ops-handler
      (dashboard/batch-detail batch-id))))

(defn batch-artifacts [request]
  (let [batch-id (:batch_id (:path-params request))]
    (with-ops-handler
      (dashboard/batch-artifacts batch-id))))

;; ── Checkpoints & Replay ──

(defn current-checkpoints [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/current-checkpoints {:workspace-key (:workspace_key p)}))))

(defn checkpoint-history [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/checkpoint-history
        {:workspace-key (:workspace_key p)
         :source-key    (:source_key p)
         :limit         (parse-int (:limit p) 50)}))))

(defn replay-from-checkpoint [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (dashboard/replay-from-checkpoint!
        {:workspace-key (:workspace_key p)
         :source-key    (:source_key p)
         :source-kind   (some-> (:source_kind p) keyword)
         :graph-id      (parse-optional-int (:graph_id p))
         :node-id       (parse-optional-int (:node_id p))
         :endpoint-name (:endpoint_name p)
         :from-batch    (:from_batch p)
         :dry-run       (parse-bool (:dry_run p))
         :operator      (actor request p)}))))

(defn active-replays [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/active-replays {:workspace-key (:workspace_key p)}))))

;; ── Bad Records ──

(defn bad-record-summary [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/bad-record-summary {:workspace-key (:workspace_key p)}))))

(defn bad-record-payload [request]
  (let [record-id (:record_id (:path-params request))]
    (with-ops-handler
      (dashboard/bad-record-payload (parse-long! record-id :record_id)))))

(defn replay-bad-records [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (dashboard/replay-bad-records!
       {:record-ids (:record_ids p)
        :workspace-key (:workspace_key p)
        :dry-run (parse-bool (:dry_run p))
        :operator (actor request p)}))))

(defn bulk-ignore-bad-records [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (dashboard/bulk-ignore-bad-records!
        {:record-ids (:record_ids p)
         :workspace-key (:workspace_key p)
         :operator   (actor request p)}))))

(defn export-bad-records [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (dashboard/export-bad-records
        {:workspace-key (:workspace_key p)
         :format        (or (:format p) "json")
         :limit         (parse-int (:limit p) 10000)}))))

;; ── Schema Drift ──

(defn list-drift-events [request]
  (let [p (:params request)]
    (with-ops-handler
      (schema-drift/list-drift-events
        {:workspace-key  (:workspace_key p)
         :graph-id       (parse-optional-int (:graph_id p))
         :endpoint-name  (:endpoint_name p)
         :severity       (:severity p)
         :acknowledged   (when (some? (:acknowledged p))
                           (parse-bool (:acknowledged p)))
         :limit          (parse-int (:limit p) 100)
         :offset         (parse-int (:offset p) 0)}))))

(defn get-drift-event-detail [request]
  (let [p (:params request)
        event-id (parse-long! (:event_id (:path-params request)) :event_id)]
    (with-ops-handler
      (or (schema-drift/get-drift-event event-id (:workspace_key p))
          (throw (ex-info "Drift event not found"
                          {:status 404 :error "not_found" :event_id event-id
                           :workspace_key (:workspace_key p)}))))))

(defn acknowledge-drift-event [request]
  (let [p (:params request)
        event-id (parse-long! (:event_id (:path-params request)) :event_id)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (schema-drift/acknowledge-drift-event! event-id (:workspace_key p) (actor request p)))))

(defn list-schema-notifications [request]
  (let [p (:params request)]
    (with-ops-handler
      (schema-drift/list-notifications
        {:workspace-key (:workspace_key p)
         :unread-only   (when (some? (:unread_only p)) (parse-bool (:unread_only p)))
         :severity      (:severity p)
         :limit         (parse-int (:limit p) 50)}))))

(defn mark-schema-notifications-read [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (schema-drift/mark-notifications-read!
        (:workspace_key p)
        (or (:notification_ids p) [])))))

(defn schema-notification-count [request]
  (let [p (:params request)]
    (with-ops-handler
      (schema-drift/unread-notification-count
        {:workspace-key (:workspace_key p)}))))

(defn schema-timeline-route [request]
  (let [p (:params request)]
    (with-ops-handler
      (schema-drift/schema-timeline
        {:workspace-key (:workspace_key p)
         :graph-id      (parse-int (:graph_id p) nil)
         :endpoint-name (:endpoint_name p)
         :limit         (parse-int (:limit p) 50)}))))

(defn preview-schema-ddl [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (schema-drift/preview-schema-ddl
        {:workspace-key (:workspace_key p)
         :graph-id      (parse-optional-int (:graph_id p))
         :api-node-id   (parse-optional-int (:api_node_id p))
         :endpoint-name (:endpoint_name p)
         :event-id      (parse-optional-int (or (:event_id p) (:approval_id p)))
         :schema-hash   (:schema_hash p)}))))

(defn apply-schema-ddl [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (schema-drift/apply-drift-schema-ddl!
       {:workspace-key (:workspace_key p)
        :graph-id      (parse-optional-int (:graph_id p))
        :api-node-id   (parse-optional-int (:api_node_id p))
        :endpoint-name (:endpoint_name p)
        :event-id      (parse-optional-int (or (:event_id p) (:approval_id p)))
        :schema-hash   (:schema_hash p)
        :applied-by    (actor request p)}))))

(defn list-ddl-history [request]
  (let [p (:params request)]
    (with-ops-handler
      (schema-drift/list-ddl-history
        {:workspace-key (:workspace_key p)
         :graph-id      (parse-int (:graph_id p) nil)
         :endpoint-name (:endpoint_name p)
         :limit         (parse-int (:limit p) 50)}))))

;; ── Schema & Medallion ──

(defn freshness-chain [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/freshness-chain {:workspace-key (:workspace_key p)}))))

(defn schema-drift [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/schema-drift {:workspace-key (:workspace_key p)}))))

(defn medallion-releases [request]
  (let [p (:params request)]
    (with-ops-handler
      (dashboard/medallion-releases
        {:workspace-key (:workspace_key p)
         :limit         (parse-int (:limit p) 50)}))))

;; ── Admin & Policies ──

(defn admin-get-config [request]
  (let [p (:params request)
        config-key (or (:config_key p)
                       (second (re-find #"/admin/([^/]+)" (:uri request))))]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (when-let [row (admin/get-config {:config-key (admin/normalize-config-key config-key)
                                        :workspace-key (:workspace_key p)})]
        {:config_key (:config_key row)
         :workspace_key (:workspace_key row)
         :version (:version row)
         :updated_by (:updated_by row)
         :updated_at_utc (:updated_at_utc row)
         :value (:config_value row)}))))

(defn admin-preview-config [request]
  (let [p (:params request)
        config-key (or (:config_key p)
                       (second (re-find #"/admin/([^/]+)" (:uri request))))]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (admin/preview-config-change
        {:config-key    (admin/normalize-config-key config-key)
         :workspace-key (:workspace_key p)
         :new-value     (:value p)}))))

(defn admin-apply-config [request]
  (let [p (:params request)
        config-key (or (:config_key p)
                       (second (re-find #"/admin/([^/]+)" (:uri request))))]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (admin/apply-config-change!
        {:config-key       (admin/normalize-config-key config-key)
        :workspace-key    (:workspace_key p)
         :new-value        (:value p)
         :expected-version (parse-optional-int (:expected_version p))
         :operator         (actor request p)}))))

(defn admin-config-history [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (admin/config-history
        {:config-key    (admin/normalize-config-key (:config_key p))
         :workspace-key (:workspace_key p)
         :limit         (parse-int (:limit p) 50)}))))

(defn admin-rollback-config [request]
  (let [p (:params request)]
    (ensure-authorized! request :api.ops)
    (with-ops-handler
      (admin/rollback-config!
        {:config-key     (admin/normalize-config-key (:config_key p))
         :workspace-key  (:workspace_key p)
         :target-version (parse-int (:target_version p) nil)
         :operator       (actor request p)}))))

;; ── Timeseries ──

(defn ts-sparkline [request]
  (let [p (:params request)]
    (with-ops-handler
      (timeseries/sparkline-data
        {:metric-key    (:metric_key p)
         :workspace-key (:workspace_key p)
         :hours         (parse-int (:hours p) 24)}))))

(defn ts-delta [request]
  (let [p (:params request)]
    (with-ops-handler
      (timeseries/delta-from-yesterday
        {:metric-key    (:metric_key p)
         :workspace-key (:workspace_key p)}))))

;; ══════════════════════════════════════════════════════════════
;; Route Table
;; ══════════════════════════════════════════════════════════════

(defn ops-routes []
  ["/ops"
   ;; Pipeline Overview
   ["/pipeline/kpis"            {:get pipeline-kpis}]
   ["/pipeline/sourceStatus"    {:get source-status-list}]
   ["/pipeline/recentActivity"  {:get recent-activity}]

   ;; Alerts (cross-screen)
   ["/alerts"                   {:get list-alerts}]
   ["/alerts/history"           {:get alert-history}]
   ["/alerts/:alert_id/ack"     {:post acknowledge-alert}]
   ["/alerts/:alert_id/silence" {:post silence-alert}]
   ["/alerts/:alert_id/resolve" {:post resolve-alert}]

   ;; Correlation (drill-down, typed by entity kind)
   ["/correlation/request/:request_id" {:get correlation-from-request}]
   ["/correlation/run/:run_id"         {:get correlation-from-run}]
   ["/correlation/batch/:batch_id"     {:get correlation-from-batch}]

   ;; Queue & Workers
   ["/queue/statusCounts"                    {:get queue-status-counts}]
   ["/queue/requests"                        {:get queue-list-requests}]
   ["/queue/requests/bulkRetry"              {:post queue-bulk-retry}]
   ["/queue/requests/bulkCancel"             {:post queue-bulk-cancel}]
   ["/queue/requests/bulkRequeue"            {:post queue-bulk-requeue}]
   ["/queue/workers"                         {:get queue-list-workers}]
   ["/queue/workers/:worker_id/drain"        {:post queue-drain-worker}]
   ["/queue/workers/:worker_id/undrain"      {:post queue-undrain-worker}]
   ["/queue/workers/:worker_id/forceRelease" {:post queue-force-release}]
   ["/queue/dlq"                             {:get queue-list-dlq}]

   ;; Source Health
   ["/sources/kafka"                              {:get sources-kafka}]
   ["/sources/file"                               {:get sources-file}]
   ["/sources/api"                                {:get sources-api}]
   ["/sources/dataLossRisk"                       {:get sources-data-loss-risk}]
   ["/sources/:source_id/circuitBreaker"          {:get source-circuit-breaker}]
   ["/sources/:source_id/circuitBreaker/reset"    {:post source-circuit-breaker-reset}]
   ["/sources/kafka/:source_id/stream"            {:get kafka-stream-detail}]

   ;; Batches & Manifests
   ["/batches/summary"              {:get batch-summary}]
   ["/batches/:batch_id/detail"     {:get batch-detail}]
   ["/batches/:batch_id/artifacts"  {:get batch-artifacts}]

   ;; Checkpoints & Replay
   ["/checkpoints/current"       {:get current-checkpoints}]
   ["/checkpoints/history"       {:get checkpoint-history}]
   ["/replay/fromCheckpoint"     {:post replay-from-checkpoint}]
   ["/replay/active"             {:get active-replays}]

   ;; Bad Records
   ["/badRecords/summary"              {:get bad-record-summary}]
   ["/badRecords/:record_id/payload"   {:get bad-record-payload}]
   ["/badRecords/replay"               {:post replay-bad-records}]
   ["/badRecords/bulkIgnore"           {:post bulk-ignore-bad-records}]
   ["/badRecords/export"               {:post export-bad-records}]

   ;; Schema Drift Detection & Alerting
   ["/schema/driftEvents"                    {:get list-drift-events}]
   ["/schema/driftEvents/:event_id"          {:get get-drift-event-detail}]
   ["/schema/driftEvents/:event_id/ack"      {:post acknowledge-drift-event}]
   ["/schema/notifications"                  {:get list-schema-notifications}]
   ["/schema/notifications/markRead"         {:post mark-schema-notifications-read}]
   ["/schema/notifications/unreadCount"      {:get schema-notification-count}]
   ["/schema/timeline"                       {:get schema-timeline-route}]
   ["/schema/previewDdl"                     {:post preview-schema-ddl}]
   ["/schema/applyDdl"                       {:post apply-schema-ddl}]
   ["/schema/ddlHistory"                     {:get list-ddl-history}]

   ;; Schema & Medallion (existing)
   ["/schema/freshnessChain"    {:get freshness-chain}]
   ["/schema/driftDetection"    {:get schema-drift}]
   ["/medallion/releases"       {:get medallion-releases}]

   ;; Admin & Policies (config CRUD)
   ["/admin/config"              {:get  admin-get-config
                                  :post admin-apply-config}]
   ["/admin/config/preview"      {:post admin-preview-config}]
   ["/admin/config/history"      {:get  admin-config-history}]
   ["/admin/config/rollback"     {:post admin-rollback-config}]

   ;; Timeseries
   ["/timeseries/sparkline"      {:get ts-sparkline}]
   ["/timeseries/delta"          {:get ts-delta}]])
