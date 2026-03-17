# Ops Console Backend Design

> **Status**: Implementation contract (not exploratory).
> **DB engine**: PostgreSQL (`TIMESTAMPTZ`, `now()`, `pg_advisory_xact_lock`, `FOR UPDATE SKIP LOCKED`).
> **Frontend status**: `opsConsole.html` is a **static prototype** with mock data. A dismissible banner marks it as such. No backend wiring exists yet.

---

## 1. Architecture Decision

**Keep ops inside bitool.** The ops console reads in-process atoms (`source-circuit-breaker-state`, `adaptive-backpressure-state`, `rl-state`, `endpoint-registry`, `execution-handler-registry`) and calls mutating functions (`replay-execution-run!`, `rollback-api-batch!`) that operate on shared state. Extracting to a separate project would require building an internal RPC layer with no benefit -- ops **is** bitool's operational surface.

### Namespace Structure

```
src/clj/bitool/
├── ops/
│   ├── routes.clj          ;; All ops HTTP routes (extract from home.clj)
│   ├── dashboard.clj        ;; Aggregation queries for Pipeline Overview KPIs
│   ├── queue.clj            ;; Queue introspection, worker lease management
│   ├── source_health.clj    ;; Source lag, circuit breaker, backpressure, Kafka stream state
│   ├── alerts.clj           ;; Alert lifecycle: fire, ack, silence, resolve, history
│   ├── admin.clj            ;; Policy mutation with validation, dry-run, versioned rollback
│   ├── correlation.clj      ;; Drill-down: request -> run -> batch -> checkpoint -> bad records
│   └── timeseries.clj       ;; Time-series rollup storage for sparklines and trends
```

All ops routes move from `home.clj` to `ops/routes.clj`. Core app routes (graph CRUD, node saves, compiler) stay in `home.clj`.

---

## 2. Existing Route Inventory (93 routes)

### Already Built -- Mapped to Ops Screens

| Screen | Existing Routes | Count |
|--------|----------------|-------|
| Pipeline Overview | `GET /freshnessDashboard`, `GET /freshnessAlerts`, `GET /usageDashboard`, `GET /apiObservabilitySummary`, `GET /apiObservabilityAlerts`, `GET /bronzeSourceObservabilitySummary`, `GET /bronzeSourceObservabilityAlerts` | 7 |
| Queue & Workers | `GET /executionRuns`, `GET /executionRuns/:run_id`, `POST /executionRuns/:run_id/replay`, `GET /executionDemand` | 4 |
| Source Health | `GET /apiObservabilitySummary`, `GET /apiObservabilityAlerts`, `GET /bronzeSourceObservabilitySummary`, `GET /bronzeSourceObservabilityAlerts` | 4 (shared) |
| Batches & Manifests | `GET /apiBatches`, `GET /bronzeSourceBatches`, `POST /rollbackApiBatch`, `POST /archiveApiBatch`, `POST /applyApiRetention` | 5 |
| Checkpoints & Replay | `POST /resetApiCheckpoint`, `GET /verifyApiCommitClosure`, `POST /replayApiBadRecords`, `POST /executionRuns/:run_id/replay` | 4 |
| Bad Records | `GET /apiBadRecords`, `POST /replayApiBadRecords` | 2 |
| Schema & Medallion | `GET /apiSchemaApprovals`, `POST /reviewApiSchema`, `POST /promoteApiSchema`, `POST /previewApiSchemaInference`, `POST /previewCopybookSchema`, Silver proposals (12 routes), Gold proposals (12 routes) | 29 |
| Admin & Policies | `GET /controlPlane/tenants`, `POST /controlPlane/tenants`, `GET /controlPlane/workspaces`, `POST /controlPlane/workspaces`, `POST /controlPlane/graphAssignment`, `POST /controlPlane/secrets`, `GET /controlPlane/auditEvents`, `POST /controlPlane/graphDependencies`, `GET /graphLineage` | 9 |

**Total existing ops-applicable routes: ~64**

---

## 3. Correlation Model (Drill-Down)

Every ops investigation follows a causal chain. The design must expose this chain as navigable links across screens.

### Join Chain

Entities are connected through different keys at each level -- there is no single universal correlation ID:

```
execution_request.request_id          (UUID, origin of a pipeline run)
    └── execution_run.request_id      (joins via request_id)
            └── manifest_table.run_id (joins via run_id, NOT request_id)
                    ├── checkpoint_table.last_successful_batch_id  (joins via batch_id)
                    └── bad_records_table.run_id  (joins via run_id)
```

The correlation engine must resolve across these different join keys, not assume a single shared ID.

### Drill-Down Endpoints (Typed by Entity Kind)

Routes are **explicitly typed** to avoid ambiguity -- each entity kind has its own path:

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/ops/correlation/request/:request_id` | From request: find runs, then batches, checkpoint state, bad records |
| GET | `/ops/correlation/run/:run_id` | From run: batches produced, checkpoint advanced?, bad records created |
| GET | `/ops/correlation/batch/:batch_id` | From batch: which run produced it, which request triggered that run |

```clojure
;; correlation.clj

(defn from-request [request-id]
  "Start from request_id, walk down: request -> runs -> batches -> bad records."
  (jdbc/with-transaction [tx db/ds]
    (let [request (jdbc/execute-one! tx
                    ["SELECT * FROM execution_request WHERE request_id = ?" request-id])
          runs    (jdbc/execute! tx
                    ["SELECT * FROM execution_run WHERE request_id = ?
                      ORDER BY started_at_utc" request-id])
          run-ids (mapv :run_id runs)
          batches (when (seq run-ids)
                    (find-batches-by-run-ids tx run-ids))
          bad-recs (when (seq run-ids)
                     (find-bad-records-by-run-ids tx run-ids))]
      {:entity_kind "request"
       :request     request
       :runs        runs
       :batches     batches
       :bad_records {:count (count bad-recs) :sample (take 5 bad-recs)}})))

(defn from-run [run-id]
  "Start from run_id, walk down to batches/bad-records and up to request."
  (jdbc/with-transaction [tx db/ds]
    (let [run      (jdbc/execute-one! tx
                     ["SELECT * FROM execution_run WHERE run_id = ?" run-id])
          request  (when (:request_id run)
                     (jdbc/execute-one! tx
                       ["SELECT * FROM execution_request WHERE request_id = ?" (:request_id run)]))
          batches  (find-batches-by-run-ids tx [run-id])
          bad-recs (find-bad-records-by-run-ids tx [run-id])]
      {:entity_kind "run"
       :run         run
       :request     request
       :batches     batches
       :bad_records {:count (count bad-recs) :sample (take 5 bad-recs)}})))

(defn from-batch [batch-id]
  "Start from batch_id, walk up to run and request."
  (jdbc/with-transaction [tx db/ds]
    (let [batch   (find-batch-across-manifests tx batch-id)
          run     (when (:run_id batch)
                    (jdbc/execute-one! tx
                      ["SELECT * FROM execution_run WHERE run_id = ?" (:run_id batch)]))
          request (when (:request_id run)
                    (jdbc/execute-one! tx
                      ["SELECT * FROM execution_request WHERE request_id = ?" (:request_id run)]))]
      {:entity_kind "batch"
       :batch       batch
       :run         run
       :request     request})))
```

### Frontend Contract

Every table row renders IDs as clickable links with explicit kind:
- `request_id` links to `/ops/correlation/request/<id>`
- `run_id` links to `/ops/correlation/run/<id>`
- `batch_id` links to `/ops/correlation/batch/<id>`

Clicking opens a detail panel on the current screen showing the full chain for that entity.

---

## 4. Missing Endpoints

### 4.1 Pipeline Overview (Screen 1)

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/pipeline/kpis` | `dashboard/pipeline-kpis` | Aggregate KPIs: active pipelines, total throughput, error rate, avg latency, SLA compliance %, active alerts |
| GET | `/ops/pipeline/sourceStatus` | `dashboard/source-status-list` | Per-source row: name, type, status, throughput/min, last-run, error-count, sparkline data (from timeseries) |
| GET | `/ops/pipeline/recentActivity` | `dashboard/recent-activity` | Last N events across all subsystems (ingestion runs, schema changes, policy updates, alerts) |

**Data sources:**
- `operations_endpoint_freshness_status` -- SLA compliance, freshness lag
- `execution_request` -- queue depth, active runs
- `execution_run` -- throughput, error rate, latency
- `source-circuit-breaker-state` atom -- circuit breaker alerts
- `ops_timeseries_rollup` -- sparkline data (see Section 9)

```clojure
;; dashboard.clj
(defn pipeline-kpis [{:keys [workspace-key time-range]}]
  (jdbc/with-transaction [tx db/ds]
    (let [cutoff   (time-range->timestamp time-range)
          runs     (jdbc/execute! tx
                     ["SELECT status, count(*) as cnt,
                             avg(EXTRACT(EPOCH FROM (finished_at_utc - started_at_utc)) * 1000) as avg_ms,
                             sum(rows_written) as total_rows
                       FROM execution_run
                       WHERE started_at_utc > ?
                       GROUP BY status" cutoff])
          freshness (operations/freshness-dashboard
                      {:workspace-key workspace-key :only-alerts? false})
          alerts   (count (filter :sla_breached freshness))]
      {:active_pipelines   (count-active-pipelines tx workspace-key)
       :throughput_total   (reduce + 0 (map #(or (:total_rows %) 0) runs))
       :error_rate         (error-rate runs)
       :avg_latency_ms     (weighted-avg runs :avg_ms :cnt)
       :sla_compliance_pct (sla-compliance freshness)
       :active_alerts      alerts})))
```

### 4.2 Alert Lifecycle (Cross-Screen)

Alerts are not just a read-only list. They have a state machine:

```
fired -> acknowledged -> (silenced | resolved)
                              |
                              v
                         auto-unsilence (after silence_until)
```

#### New Table: `ops_alert`

```sql
CREATE TABLE IF NOT EXISTS ops_alert (
  alert_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  alert_type     TEXT NOT NULL,         -- 'freshness_sla', 'circuit_open', 'dlq_depth', 'lag_threshold', 'run_failed', 'data_loss_risk'
  severity       TEXT NOT NULL,         -- 'critical', 'warning', 'info'
  source_key     TEXT NOT NULL,         -- dedup key: e.g. 'freshness:graph-1:node-5'
  title          TEXT NOT NULL,
  detail_json    JSONB,
  state          TEXT NOT NULL DEFAULT 'fired',  -- 'fired', 'acknowledged', 'silenced', 'resolved'
  fired_at_utc   TIMESTAMPTZ NOT NULL DEFAULT now(),
  acked_by       TEXT,
  acked_at_utc   TIMESTAMPTZ,
  silenced_until TIMESTAMPTZ,
  resolved_at_utc TIMESTAMPTZ,
  resolved_by    TEXT,                  -- 'auto' or username
  workspace_key  TEXT NOT NULL
);
CREATE INDEX idx_ops_alert_active ON ops_alert(workspace_key, state)
  WHERE state IN ('fired', 'acknowledged', 'silenced');
CREATE UNIQUE INDEX idx_ops_alert_dedup ON ops_alert(workspace_key, source_key)
  WHERE state IN ('fired', 'acknowledged', 'silenced');
```

**Dedup scope**: The unique index is on `(workspace_key, source_key)` so two workspaces with the same logical source key get independent alerts.

**Silence suppresses re-firing**: The dedup index includes `'silenced'` state. A silenced alert still occupies the `(workspace_key, source_key)` slot, so the background checker's `fire!` call hits the conflict and does nothing. The alert stays silenced until `silence_until` expires, at which point a background job transitions it back to `fired` (if the condition still holds) or `resolved`.

**PostgreSQL note**: `ON CONFLICT ON CONSTRAINT` only works with named table constraints, not plain indexes. For partial unique indexes, use `ON CONFLICT (columns) WHERE predicate` syntax, matching the index definition exactly.

#### Alert Endpoints

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/alerts` | `alerts/list-alerts` | Active alerts with filters (state, severity, type, source) |
| GET | `/ops/alerts/history` | `alerts/alert-history` | Resolved/silenced alerts for post-incident review |
| POST | `/ops/alerts/:alert_id/ack` | `alerts/acknowledge!` | Mark alert as acknowledged by operator |
| POST | `/ops/alerts/:alert_id/silence` | `alerts/silence!` | Silence alert until a given time (body: `{silence_until: "..."}`) |
| POST | `/ops/alerts/:alert_id/resolve` | `alerts/resolve!` | Manually resolve alert |
| POST | `/ops/alerts/fire` | `alerts/fire!` | Internal: fire a new alert (called by background checks, not UI) |

```clojure
;; alerts.clj
(defn fire! [{:keys [alert-type severity source-key title detail workspace-key]}]
  ;; Dedup: if the same (workspace_key, source_key) already has an active or silenced alert,
  ;; no new row is created. This means silenced alerts block re-firing.
  (jdbc/execute-one! db/ds
    ["INSERT INTO ops_alert (alert_type, severity, source_key, title, detail_json, workspace_key)
      VALUES (?, ?, ?, ?, ?::jsonb, ?)
      ON CONFLICT (workspace_key, source_key)
        WHERE state IN ('fired', 'acknowledged', 'silenced')
        DO NOTHING"
     alert-type severity source-key title (json/write-str detail) workspace-key]))

(defn acknowledge! [alert-id user]
  (let [updated (jdbc/execute-one! db/ds
                  ["UPDATE ops_alert SET state = 'acknowledged', acked_by = ?, acked_at_utc = now()
                    WHERE alert_id = ? AND state = 'fired'
                    RETURNING *" user alert-id])]
    (when updated
      (control-plane/record-audit-event!
        {:event_type "alert.acknowledged" :entity_id (str alert-id) :actor user}))
    updated))

(defn unsilence-expired-alerts!
  "Background job: transitions silenced alerts past their silence_until deadline.
   If condition still holds, moves back to 'fired'. If cleared, moves to 'resolved'."
  []
  (let [expired (jdbc/execute! db/ds
                  ["SELECT * FROM ops_alert
                    WHERE state = 'silenced' AND silenced_until <= now()"])]
    (doseq [alert expired]
      (if (condition-cleared? alert)
        (jdbc/execute! db/ds
          ["UPDATE ops_alert SET state = 'resolved', resolved_by = 'auto', resolved_at_utc = now()
            WHERE alert_id = ?" (:alert_id alert)])
        ;; Reset timing metadata so the re-fired alert appears fresh:
        ;; - fired_at_utc = now (age starts over, no stale MTTA)
        ;; - clear ack fields (previous ack was for the pre-silence instance)
        ;; - clear silenced_until
        (jdbc/execute! db/ds
          ["UPDATE ops_alert
            SET state = 'fired', fired_at_utc = now(),
                silenced_until = NULL,
                acked_by = NULL, acked_at_utc = NULL
            WHERE alert_id = ?" (:alert_id alert)])))))

(defn auto-resolve-stale-alerts!
  "Background job: resolve active alerts whose condition is no longer true."
  []
  (let [open-alerts (jdbc/execute! db/ds
                      ["SELECT * FROM ops_alert WHERE state IN ('fired','acknowledged')"])]
    (doseq [alert open-alerts]
      (when (condition-cleared? alert)
        (jdbc/execute! db/ds
          ["UPDATE ops_alert SET state = 'resolved', resolved_by = 'auto', resolved_at_utc = now()
            WHERE alert_id = ? AND state IN ('fired','acknowledged')"
           (:alert_id alert)])))))
```

**Background jobs** (run every 30s):
1. `auto-resolve-stale-alerts!` -- resolve `fired`/`acknowledged` alerts whose condition cleared
2. `unsilence-expired-alerts!` -- transition silenced alerts past `silence_until` back to `fired` or `resolved`
3. Fire new alerts for detected conditions (freshness, circuit breaker, DLQ, lag, data loss)

#### Alert Notification & Escalation

The DB-backed alert lifecycle handles state tracking. **Delivery** to operators is handled by a pluggable notification dispatcher that runs after each `fire!`:

```clojure
;; alerts.clj
(defonce notification-channels (atom []))

(defn register-notification-channel!
  "Register a delivery channel. Each channel is a map with:
   :name     - 'slack', 'webhook', 'email', 'pagerduty'
   :deliver! - (fn [alert] ...) side-effecting delivery function
   :filter   - (fn [alert] bool) optional predicate to scope delivery"
  [channel]
  (swap! notification-channels conj channel))

(defn dispatch-notifications! [alert]
  (doseq [ch @notification-channels]
    (when (or (nil? (:filter ch))
              ((:filter ch) alert))
      (try
        ((:deliver! ch) alert)
        (catch Exception e
          (log/warn "Notification delivery failed" {:channel (:name ch) :alert_id (:alert_id alert) :error (.getMessage e)}))))))
```

**Built-in channels** (configured via `ops_config` key `notification_channels`):

| Channel | Trigger | Config |
|---------|---------|--------|
| **Webhook** | All `fired` alerts | `{url: "https://...", headers: {...}}` -- POST alert JSON to URL |
| **Slack** | `critical` alerts | `{webhook_url: "https://hooks.slack.com/..."}` -- formatted message |
| **Email** | `critical` alerts unacknowledged > 15 min | `{smtp_host, to, from}` -- escalation |

**Escalation policy**: A background job checks for `fired` alerts older than the escalation threshold (configurable per workspace, default 15 min). If unacknowledged, it re-dispatches with `severity` bumped in the notification payload (the DB row stays the same). This is delivery-tier escalation, not DB-state escalation.

**Future**: PagerDuty, OpsGenie, or custom integrations via the same `register-notification-channel!` API.

**Alert firing triggers**:
- Freshness SLA breach -> `alert-type = 'freshness_sla'`
- Circuit breaker open -> `alert-type = 'circuit_open'`
- DLQ depth > threshold -> `alert-type = 'dlq_depth'`
- Kafka lag > retention threshold -> `alert-type = 'lag_threshold'`
- Data loss risk detected -> `alert-type = 'data_loss_risk'`

### 4.3 Queue & Workers (Screen 2)

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/queue/statusCounts` | `queue/status-counts` | Counts by status: queued, leased, running, retrying, failed, DLQ |
| GET | `/ops/queue/requests` | `queue/list-requests` | Paginated request list with status filter |
| POST | `/ops/queue/requests/bulkRetry` | `queue/bulk-retry!` | Retry selected failed requests (transactional, CAS) |
| POST | `/ops/queue/requests/bulkCancel` | `queue/bulk-cancel!` | Cancel selected queued/leased requests (transactional, CAS) |
| POST | `/ops/queue/requests/bulkRequeue` | `queue/bulk-requeue!` | Move DLQ items back to queue (transactional, CAS) |
| GET | `/ops/queue/workers` | `queue/list-workers` | Active worker leases with heartbeat age, drain state |
| POST | `/ops/queue/workers/:worker_id/drain` | `queue/drain-worker!` | Graceful drain (finish current, stop accepting) |
| POST | `/ops/queue/workers/:worker_id/forceRelease` | `queue/force-release!` | Force-release lease (orphan recovery) |
| GET | `/ops/queue/dlq` | `queue/list-dlq` | Dead letter queue contents |

**Data sources:**
- `execution_request` table -- status counts, request list
- `execution_lease_heartbeat` table -- worker leases
- `execution_dlq` table -- dead letter items
- `execution_orphan_recovery_event` table -- recovery history

#### Bulk Mutation Safety

All bulk mutations run inside a single transaction with CAS (compare-and-swap on expected status) and advisory locks:

```clojure
;; queue.clj
(defn bulk-retry! [request-ids expected-status operator]
  {:pre [(<= (count request-ids) 100)]}  ;; enforce max 100 items
  (jdbc/with-transaction [tx db/ds]
    ;; Lock the workspace to prevent concurrent bulk ops
    (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext('ops-bulk-queue'))"])
    (let [updated (jdbc/execute! tx
                    ["WITH targets AS (
                        SELECT request_id FROM execution_request
                        WHERE request_id = ANY(?)
                          AND status = ?
                        FOR UPDATE SKIP LOCKED
                      )
                      UPDATE execution_request r
                      SET status = 'queued',
                          retry_count = r.retry_count + 1,
                          updated_at_utc = now()
                      FROM targets t
                      WHERE r.request_id = t.request_id
                      RETURNING r.request_id"
                     (into-array java.util.UUID request-ids)
                     expected-status])]
      (control-plane/record-audit-event!
        {:event_type "queue.bulk_retry"
         :actor operator
         :detail {:requested (count request-ids)
                  :updated (count updated)
                  :expected_status expected-status}})
      {:requested (count request-ids)
       :updated   (count updated)
       :skipped   (- (count request-ids) (count updated))})))
```

Key safety properties:
- **Single transaction**: the UPDATE itself is atomic -- all matched rows are updated together or none are
- **Partial update is expected**: `FOR UPDATE SKIP LOCKED` means rows currently locked by a worker (being processed) are silently skipped. The response reports `requested`, `updated`, and `skipped` counts so the operator knows exactly what happened. This is intentional -- it avoids blocking on in-flight work. Operators should retry skipped items after the in-flight work completes.
- **CAS on status**: only retries requests still in `expected-status`. A request that transitioned to a different status between the operator's selection and the bulk action is skipped (not errored).
- **Advisory lock**: prevents two operators from running overlapping bulk ops on the same workspace. Without this, two concurrent `bulk-retry!` calls could produce confusing audit trails.
- **Cap at 100**: prevents unbounded bulk operations
- **Audit**: every bulk action logged with requested vs actual counts, so the operator and audit trail both reflect partial outcomes honestly

#### Worker Drain

Drain must be **worker-level, not request-level**. A per-request flag is insufficient because:
- An idle worker has no active requests to mark
- A running worker that finishes its current request would immediately claim a new one

Drain requires a persistent worker-level registry that the claim path checks.

```sql
-- New table: worker drain state (not per-request)
CREATE TABLE IF NOT EXISTS ops_worker_drain (
  worker_id        TEXT PRIMARY KEY,
  drain_requested  BOOLEAN NOT NULL DEFAULT true,
  requested_by     TEXT NOT NULL,
  requested_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Drain flow:**

1. Operator calls `POST /ops/queue/workers/:worker_id/drain`
2. Handler inserts into `ops_worker_drain`
3. The **claim path** (in `execution.clj`) must check this table before granting a lease:

```clojure
;; queue.clj
(defn drain-worker! [worker-id operator]
  (jdbc/with-transaction [tx db/ds]
    ;; Insert worker-level drain flag (idempotent via ON CONFLICT)
    (jdbc/execute! tx
      ["INSERT INTO ops_worker_drain (worker_id, requested_by)
        VALUES (?, ?)
        ON CONFLICT (worker_id) DO UPDATE
        SET drain_requested = true, requested_by = EXCLUDED.requested_by, requested_at_utc = now()"
       worker-id operator])
    (control-plane/record-audit-event!
      {:event_type "worker.drain" :actor operator :detail {:worker_id worker-id}})
    {:worker_id worker-id :status "drain_requested"}))

(defn undrain-worker! [worker-id operator]
  (jdbc/execute! db/ds
    ["DELETE FROM ops_worker_drain WHERE worker_id = ?" worker-id])
  (control-plane/record-audit-event!
    {:event_type "worker.undrain" :actor operator :detail {:worker_id worker-id}}))
```

**Required change to claim path** (in `execution.clj`, the CTE that claims a request):

```sql
-- Add to the claim CTE's WHERE clause:
AND NOT EXISTS (
  SELECT 1 FROM ops_worker_drain
  WHERE worker_id = ? AND drain_requested = true
)
```

This ensures a drained worker cannot claim new work regardless of whether it currently holds any requests. The worker's current in-flight request finishes normally; it simply cannot pick up another one.

**Undrain**: `POST /ops/queue/workers/:worker_id/undrain` removes the drain flag, allowing the worker to resume claiming.

**Implementation dependency**: Drain is **not effective** until the claim CTE in `execution.clj` includes the `NOT EXISTS` check above. The `ops_worker_drain` table and ops endpoints can be shipped independently, but drain must not be exposed to operators as functional until the claim-path change is deployed. The frontend should disable the Drain button and show "Drain support requires execution engine update" until the backend confirms drain enforcement is active (e.g., via a `/ops/queue/capabilities` endpoint or feature flag).

### 4.4 Source Health (Screen 3)

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/sources/kafka` | `source-health/kafka-sources` | Kafka sources with stream-level observability |
| GET | `/ops/sources/file` | `source-health/file-sources` | File sources: discovered, processed, failures, last scan |
| GET | `/ops/sources/api` | `source-health/api-sources` | API sources: rate limit remaining, circuit state, last call, error rate |
| GET | `/ops/sources/:source_id/circuitBreaker` | `source-health/circuit-breaker-state` | Circuit breaker detail |
| POST | `/ops/sources/:source_id/circuitBreaker/reset` | `source-health/reset-circuit-breaker!` | Force circuit breaker to closed |
| GET | `/ops/sources/dataLossRisk` | `source-health/data-loss-risk` | Data loss detection (see Section 5) |
| GET | `/ops/sources/kafka/:source_id/stream` | `source-health/kafka-stream-detail` | Kafka stream observability (see Section 6) |

### 4.5 Batches & Manifests (Screen 4)

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/batches/summary` | `dashboard/batch-summary` | Counts by status: committed, preparing, pending_checkpoint, rolled_back, archived |
| GET | `/ops/batches/:batch_id/detail` | `dashboard/batch-detail` | Full batch detail with correlation links to run, checkpoint, bad records |
| GET | `/ops/batches/:batch_id/artifacts` | `dashboard/batch-artifacts` | Artifact listing (schema snapshot, sample data, state file) |

Existing routes cover core operations (list, rollback, archive, retention). Batch detail includes correlation links:

```clojure
(defn batch-detail [batch-id]
  (let [batch     (find-batch-across-manifests batch-id)
        run       (when (:run_id batch)
                    (jdbc/execute-one! db/ds
                      ["SELECT run_id, request_id, status, started_at_utc, finished_at_utc
                        FROM execution_run WHERE run_id = ?" (:run_id batch)]))
        bad-count (count-bad-records-for-run (:run_id batch))]
    (assoc batch
      :correlation {:run run
                    :request_id (:request_id run)
                    :bad_record_count bad-count
                    :links {:run (str "/ops/correlation/run/" (:run_id batch))
                            :request (str "/ops/correlation/request/" (:request_id run))}})))
```

### 4.6 Checkpoints & Replay (Screen 5)

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/checkpoints/current` | `dashboard/current-checkpoints` | Current checkpoint per source with correlation to last batch |
| GET | `/ops/checkpoints/history` | `dashboard/checkpoint-history` | Checkpoint timeline: transitions with timestamps |
| POST | `/ops/replay/fromCheckpoint` | `dashboard/replay-from-checkpoint!` | Replay from specific checkpoint |
| GET | `/ops/replay/active` | `dashboard/active-replays` | Currently running replay operations |

### 4.7 Bad Records (Screen 6)

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/badRecords/summary` | `dashboard/bad-record-summary` | Counts by failure class with correlation to source runs |
| GET | `/ops/badRecords/:record_id/payload` | `dashboard/bad-record-payload` | Raw payload (hex + decoded) for inspection |
| POST | `/ops/badRecords/bulkIgnore` | `dashboard/bulk-ignore-bad-records!` | Mark selected as ignored (transactional, max 100) |
| POST | `/ops/badRecords/export` | `dashboard/export-bad-records` | Export as CSV/JSON (cap: 10,000 records) |

### 4.8 Schema & Medallion (Screen 7)

Existing routes are comprehensive (29 routes). Missing pieces:

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/schema/freshnessChain` | `dashboard/freshness-chain` | Bronze->Silver->Gold freshness per source |
| GET | `/ops/schema/driftDetection` | `dashboard/schema-drift` | Sources where inferred schema differs from approved |
| GET | `/ops/medallion/releases` | `dashboard/medallion-releases` | Release history: version, table, deployed_at, active |

### 4.9 Admin & Policies (Screen 8) -- Safe Mutations

All admin mutations use a **preview/validate/apply** pattern with versioned rollback. See Section 8 for details.

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/ops/admin/workerSettings` | `admin/get-config` | Current worker pool config |
| POST | `/ops/admin/workerSettings/preview` | `admin/preview-config-change` | Dry-run: show diff + validation results |
| POST | `/ops/admin/workerSettings` | `admin/apply-config-change!` | Apply validated config change |
| GET | `/ops/admin/queueSettings` | `admin/get-config` | Queue config |
| POST | `/ops/admin/queueSettings/preview` | `admin/preview-config-change` | Dry-run |
| POST | `/ops/admin/queueSettings` | `admin/apply-config-change!` | Apply |
| GET | `/ops/admin/sourceLimits` | `admin/get-config` | Source concurrency limits |
| POST | `/ops/admin/sourceLimits/preview` | `admin/preview-config-change` | Dry-run |
| POST | `/ops/admin/sourceLimits` | `admin/apply-config-change!` | Apply |
| GET | `/ops/admin/alertThresholds` | `admin/get-config` | Alert thresholds |
| POST | `/ops/admin/alertThresholds/preview` | `admin/preview-config-change` | Dry-run |
| POST | `/ops/admin/alertThresholds` | `admin/apply-config-change!` | Apply |
| GET | `/ops/admin/retentionPolicy` | `admin/get-config` | Retention/archive policies |
| POST | `/ops/admin/retentionPolicy/preview` | `admin/preview-config-change` | Dry-run |
| POST | `/ops/admin/retentionPolicy` | `admin/apply-config-change!` | Apply |
| GET | `/ops/admin/configHistory` | `admin/config-history` | Versioned change history with diffs |
| POST | `/ops/admin/configRollback` | `admin/rollback-config!` | Rollback to previous version |

---

## 5. Data Loss Detection

The design must detect silent data loss, not just duplicates. Sources of data loss:

| Risk | Detection Method | Endpoint |
|------|-----------------|----------|
| **Kafka retention horizon breach** | Compare consumer lag (messages behind) against topic retention time. If `lag / throughput_per_sec > retention_seconds`, data will be lost before it's consumed. | `GET /ops/sources/dataLossRisk` |
| **Missing partitions** | Compare assigned partition set against topic partition count. If `assigned < total`, some partitions are unread. | `GET /ops/sources/kafka/:source_id/stream` |
| **Sequence gaps** | For sources with monotonic IDs/offsets, detect gaps: `max(checkpoint_offset) - expected_next != 0`. | `GET /ops/sources/dataLossRisk` |
| **Expected file arrival SLA** | File sources have an expected arrival window (e.g., daily at 02:00). If no file discovered within SLA window, fire alert. | `GET /ops/sources/dataLossRisk` |
| **Source-vs-Bronze reconciliation** | Compare source row count (API pagination total, Kafka high watermark, file line count) against Bronze batch row_count sums. Drift beyond threshold = potential loss. | `GET /ops/sources/dataLossRisk` |
| **Checkpoint-to-manifest gap** | If checkpoint advanced but no matching committed manifest exists, data may have been acknowledged but not durably stored. | `GET /ops/sources/dataLossRisk` |

```clojure
;; source_health.clj
(defn data-loss-risk [{:keys [workspace-key]}]
  (let [kafka-risks   (kafka-retention-risk workspace-key)
        file-risks    (file-arrival-sla-risk workspace-key)
        seq-gaps      (sequence-gap-risk workspace-key)
        ckpt-manifest (checkpoint-manifest-gap-risk workspace-key)]
    {:risks (concat kafka-risks file-risks seq-gaps ckpt-manifest)
     :summary {:critical (count (filter #(= :critical (:severity %)) (concat kafka-risks file-risks seq-gaps ckpt-manifest)))
               :warning  (count (filter #(= :warning (:severity %)) (concat kafka-risks file-risks seq-gaps ckpt-manifest)))}}))

(defn kafka-retention-risk [workspace-key]
  ;; For each Kafka source, check if lag/throughput exceeds retention
  (let [sources (kafka-sources {:workspace-key workspace-key})]
    (->> sources
         (map (fn [src]
                (let [lag             (:consumer_lag src)
                      throughput-sec  (max 1 (:throughput_per_sec src))
                      time-to-catch   (/ lag throughput-sec)
                      retention-sec   (:retention_seconds src)
                      runway          (- retention-sec time-to-catch)]
                  (when (< runway 0)
                    {:source_key  (:source_key src)
                     :risk_type   "kafka_retention_breach"
                     :severity    :critical
                     :detail      {:lag lag
                                   :time_to_catch_up_sec time-to-catch
                                   :retention_sec retention-sec
                                   :runway_sec runway}}))))
         (remove nil?))))
```

Each detected risk fires an alert via `alerts/fire!` with `alert-type = 'data_loss_risk'`.

---

## 6. Kafka Stream Observability

The existing source health endpoints only cover lag and circuit breaker state. For production Kafka operations, operators need deeper visibility.

### Stream Detail Endpoint

`GET /ops/sources/kafka/:source_id/stream`

Returns:

```json
{
  "consumer_group": "bitool-ingest-orders",
  "state": "Stable",
  "assigned_partitions": [0, 1, 2, 3],
  "topic_partition_count": 4,
  "missing_partitions": [],
  "rebalance_count_24h": 2,
  "last_rebalance_at": "2026-03-17T10:15:00Z",
  "paused_partitions": [],
  "per_partition": [
    {"partition": 0, "current_offset": 145230, "high_watermark": 145235, "lag": 5, "commit_latency_ms": 12},
    {"partition": 1, "current_offset": 89102, "high_watermark": 89900, "lag": 798, "commit_latency_ms": 45}
  ],
  "poll_stall_duration_ms": 0,
  "avg_poll_latency_ms": 85,
  "retention_runway_sec": 172800,
  "throughput_per_sec": 1250
}
```

### Data Source

Most of this data comes from the Kafka consumer instance itself (not a database table). Since Kafka consumers are **not thread-safe** (only `.wakeup()` is safe cross-thread), we collect metrics via a periodic snapshot written to an atom:

```clojure
;; In runtime.clj, inside the consumer loop:
(defonce kafka-stream-metrics (atom {}))

;; After each poll cycle, update metrics:
(swap! kafka-stream-metrics assoc
  [graph-id source-key]
  {:consumer_group    group-id
   :state             (.state consumer)  ;; only read from consumer thread
   :assigned_partitions (mapv #(.partition %) (.assignment consumer))
   :per_partition     (partition-metrics consumer)
   :last_poll_at      (java.time.Instant/now)
   :rebalance_count   @rebalance-counter
   :paused_partitions (mapv #(.partition %) (.paused consumer))
   :poll_latency_ms   last-poll-ms})
```

The ops endpoint reads from this atom (safe, immutable snapshot) and enriches with topic metadata from the admin client.

### New Atom: `kafka-stream-metrics`

| Field | Source | Thread Safety |
|-------|--------|--------------|
| `consumer_group` | Consumer config | Set once at init |
| `state` | `consumer.groupMetadata()` | Read from consumer thread only |
| `assigned_partitions` | `consumer.assignment()` | Read from consumer thread only |
| `per_partition` (offsets, lag) | `consumer.position()` + admin `listOffsets()` | Consumer thread for position; admin client is thread-safe |
| `rebalance_count` | `ConsumerRebalanceListener` callback | Callback runs on consumer thread |
| `paused_partitions` | `consumer.paused()` | Read from consumer thread only |
| `poll_latency_ms` | Timer around `.poll()` | Computed on consumer thread |

All reads happen on the consumer thread; the atom snapshot is what ops endpoints read.

---

## 7. New Database Tables

### 7.1 `ops_alert` (Alert Lifecycle)

See Section 4.2 for full DDL.

### 7.2 `ops_config` (Admin Settings with Versioning)

```sql
CREATE TABLE IF NOT EXISTS ops_config (
  config_id      BIGSERIAL PRIMARY KEY,
  config_key     TEXT NOT NULL,          -- 'worker_settings', 'queue_settings', etc.
  config_value   JSONB NOT NULL,
  workspace_key  TEXT,                   -- NULL = global
  version        INT NOT NULL DEFAULT 1,
  updated_by     TEXT NOT NULL,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded     BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX idx_ops_config_active ON ops_config(config_key, workspace_key) WHERE NOT superseded;
CREATE INDEX idx_ops_config_history ON ops_config(config_key, workspace_key, version DESC);
```

Every config change creates a new row with `version = prev_version + 1` and marks the old row as `superseded = true`. This gives full version history and rollback capability.

### 7.3 `ops_timeseries_rollup` (Sparklines & Trends)

See Section 9 for full DDL and rollup strategy.

### 7.4 `ops_worker_drain` (Worker Drain State)

```sql
CREATE TABLE IF NOT EXISTS ops_worker_drain (
  worker_id        TEXT PRIMARY KEY,
  drain_requested  BOOLEAN NOT NULL DEFAULT true,
  requested_by     TEXT NOT NULL,
  requested_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

See Section 4.3 (Worker Drain) for usage. The claim path in `execution.clj` must check this table before granting leases.

---

## 8. Safe Admin Mutation Controls

Admin policy changes (concurrency limits, retry policies, retention, alert thresholds) can cause cascading failures if applied carelessly. Every mutation follows a **preview -> validate -> apply** flow.

### Preview (Dry-Run)

```clojure
;; admin.clj
(defn preview-config-change [{:keys [config-key workspace-key new-value]}]
  (let [current (current-config config-key workspace-key)
        diff    (config-diff (:config_value current) new-value)
        errors  (validate-config config-key new-value)]
    {:current  (:config_value current)
     :proposed new-value
     :diff     diff        ;; [{:path "max_concurrency" :old 10 :new 50}]
     :valid    (empty? errors)
     :errors   errors      ;; ["max_concurrency cannot exceed workspace limit of 20"]
     :version  (:version current)}))
```

### Validate

Validation rules per config key:

| Config Key | Validation |
|-----------|-----------|
| `worker_settings` | `max_concurrency` <= workspace limit; `lease_timeout_sec` >= 30; `heartbeat_interval_sec` < `lease_timeout_sec` |
| `queue_settings` | `max_depth` > 0; `retry_limit` <= 10; `dlq_threshold` <= `retry_limit` |
| `source_limits` | `concurrency_limit` > 0; `rate_limit_per_sec` > 0 |
| `alert_thresholds` | `freshness_sla_sec` > 0; `lag_threshold` > 0; `error_rate_pct` between 0 and 100 |
| `retention_policy` | `keep_days` >= 1; `archive_target` is valid path/bucket |

### Apply (with version check)

```clojure
(defn apply-config-change! [{:keys [config-key workspace-key new-value expected-version operator]}]
  (jdbc/with-transaction [tx db/ds]
    (let [current (jdbc/execute-one! tx
                    ["SELECT * FROM ops_config
                      WHERE config_key = ? AND COALESCE(workspace_key, '') = COALESCE(?, '')
                        AND NOT superseded
                      FOR UPDATE" config-key workspace-key])]
      ;; Optimistic concurrency: check version hasn't changed since preview
      (when (and current (not= (:version current) expected-version))
        (throw (ex-info "Config was modified since preview"
                 {:expected expected-version :actual (:version current)})))
      ;; Validate again (defense in depth)
      (let [errors (validate-config config-key new-value)]
        (when (seq errors)
          (throw (ex-info "Validation failed" {:errors errors}))))
      ;; Supersede old version
      (when current
        (jdbc/execute! tx
          ["UPDATE ops_config SET superseded = true WHERE config_id = ?" (:config_id current)]))
      ;; Insert new version
      (jdbc/execute! tx
        ["INSERT INTO ops_config (config_key, config_value, workspace_key, version, updated_by)
          VALUES (?, ?::jsonb, ?, ?, ?)"
         config-key (json/write-str new-value) workspace-key
         (inc (or (:version current) 0)) operator])
      ;; Audit with diff
      (control-plane/record-audit-event!
        {:event_type "config.changed"
         :actor operator
         :detail {:config_key config-key
                  :old_version (:version current)
                  :new_version (inc (or (:version current) 0))
                  :diff (config-diff (:config_value current) new-value)}}))))
```

### Rollback

```clojure
(defn rollback-config! [{:keys [config-key workspace-key target-version operator]}]
  (jdbc/with-transaction [tx db/ds]
    (let [target (jdbc/execute-one! tx
                   ["SELECT * FROM ops_config
                     WHERE config_key = ? AND COALESCE(workspace_key, '') = COALESCE(?, '')
                       AND version = ?" config-key workspace-key target-version])]
      (when-not target
        (throw (ex-info "Target version not found" {:version target-version})))
      ;; Apply the old config as a new version (not literally reverting, creating a new version with old value)
      (apply-config-change!
        {:config-key config-key
         :workspace-key workspace-key
         :new-value (json/read-str (:config_value target))
         :expected-version nil  ;; skip version check for rollback
         :operator operator}))))
```

---

## 9. Time-Series Storage (Sparklines & Trends)

The UI shows sparklines, trend badges ("up 2 from yesterday"), throughput history, and "from yesterday" deltas. These require historical rollup data, not just current snapshots.

### Rollup Table

```sql
CREATE TABLE IF NOT EXISTS ops_timeseries_rollup (
  metric_key     TEXT NOT NULL,       -- 'throughput:orders-api', 'error_rate:kafka-events', 'lag:kafka-orders'
  bucket         TIMESTAMPTZ NOT NULL, -- truncated to interval (e.g., 5-minute bucket)
  workspace_key  TEXT NOT NULL,
  value          DOUBLE PRECISION NOT NULL,
  sample_count   INT NOT NULL DEFAULT 1,
  PRIMARY KEY (metric_key, bucket, workspace_key)
);
CREATE INDEX idx_ops_ts_query ON ops_timeseries_rollup(workspace_key, metric_key, bucket DESC);
```

### Cardinality Guardrails

`metric_key` is **not free-form**. It must be one of a fixed set of prefixes, and the suffix is a registered source/endpoint key:

```clojure
;; timeseries.clj
(def allowed-metric-prefixes
  #{"throughput" "error_rate" "lag" "bad_records" "queue_depth" "freshness_lag"})

(defn validate-metric-key [metric-key]
  (let [[prefix _suffix] (str/split metric-key #":" 2)]
    (when-not (contains? allowed-metric-prefixes prefix)
      (throw (ex-info "Invalid metric key prefix" {:metric_key metric-key :allowed allowed-metric-prefixes})))))
```

**Cardinality bounds**: The number of distinct `metric_key` values is bounded by `|prefixes| * |sources|`. With 6 prefixes and ~50 sources, that's ~300 keys. At 288 buckets/day (5-min intervals), this produces ~86,400 rows/day -- well within PostgreSQL's comfort zone.

The cleanup job (Section 9, retention) also enforces a hard cap: if `COUNT(DISTINCT metric_key)` exceeds 1,000, it logs a warning and skips new keys until stale ones are cleaned up.

### Rollup Strategy

A background job runs every 5 minutes and aggregates current state into the rollup table:

```clojure
;; timeseries.clj
(defn record-rollup! []
  (let [now       (java.time.Instant/now)
        bucket    (truncate-to-5min now)
        ;; Collect current metrics from atoms + recent queries
        metrics   (collect-current-metrics)]
    (doseq [{:keys [metric-key workspace-key value]} metrics]
      (jdbc/execute! db/ds
        ["INSERT INTO ops_timeseries_rollup (metric_key, bucket, workspace_key, value)
          VALUES (?, ?, ?, ?)
          ON CONFLICT (metric_key, bucket, workspace_key)
          DO UPDATE SET value = EXCLUDED.value, sample_count = ops_timeseries_rollup.sample_count + 1"
         metric-key bucket workspace-key value]))))

(defn sparkline-data [metric-key workspace-key hours]
  "Returns time-series points for sparkline rendering."
  (jdbc/execute! db/ds
    ["SELECT bucket, value FROM ops_timeseries_rollup
      WHERE metric_key = ? AND workspace_key = ?
        AND bucket >= now() - (? || ' hours')::interval
      ORDER BY bucket ASC"
     metric-key workspace-key (str hours)]))

(defn delta-from-yesterday [metric-key workspace-key]
  "Returns {current: N, yesterday: M, delta: N-M, direction: 'up'|'down'|'flat'}."
  (let [today     (jdbc/execute-one! db/ds
                    ["SELECT sum(value) as total FROM ops_timeseries_rollup
                      WHERE metric_key = ? AND workspace_key = ?
                        AND bucket >= CURRENT_DATE" metric-key workspace-key])
        yesterday (jdbc/execute-one! db/ds
                    ["SELECT sum(value) as total FROM ops_timeseries_rollup
                      WHERE metric_key = ? AND workspace_key = ?
                        AND bucket >= CURRENT_DATE - interval '1 day'
                        AND bucket < CURRENT_DATE" metric-key workspace-key])
        curr (or (:total today) 0)
        prev (or (:total yesterday) 0)]
    {:current curr :yesterday prev :delta (- curr prev)
     :direction (cond (> curr prev) "up" (< curr prev) "down" :else "flat")}))
```

### Metrics Collected

| Metric Key Pattern | Source | Frequency |
|-------------------|--------|-----------|
| `throughput:<source_key>` | `execution_run` rows_written sum | 5 min |
| `error_rate:<source_key>` | `execution_run` failed / total ratio | 5 min |
| `lag:<source_key>` | `kafka-stream-metrics` atom | 5 min |
| `bad_records:<source_key>` | `bad_records_table` count | 5 min |
| `queue_depth` | `execution_request` pending count | 5 min |
| `freshness_lag:<source_key>` | `operations_endpoint_freshness_status` | 5 min |

### Retention

Rollup data is retained for 30 days. A daily cleanup job deletes older rows:

```clojure
(defn cleanup-old-rollups! []
  (jdbc/execute! db/ds
    ["DELETE FROM ops_timeseries_rollup WHERE bucket < now() - interval '30 days'"]))
```

---

## 10. Polling Error UX Contract

Since the console is poll-based, operators will routinely hit stale or partial reads during DB/network incidents. The frontend must handle this gracefully.

### Response Envelope (All Endpoints)

```json
{
  "ok": true,
  "data": { ... },
  "meta": {
    "timestamp": "2026-03-17T14:30:00Z",
    "workspace": "default",
    "time_range": "24h",
    "server_time": "2026-03-17T14:30:01Z"
  }
}
```

Error:

```json
{
  "ok": false,
  "error": "db_unavailable",
  "message": "Database connection pool exhausted"
}
```

### Frontend Staleness Contract

```javascript
// Each screen maintains last-good state
const screenState = {};

async function pollScreen(screenId, fetchFn, renderFn) {
  try {
    const resp = await fetchFn();
    if (!resp.ok) throw new Error(resp.message || 'Request failed');
    const data = await resp.json();

    // Store last-good snapshot
    screenState[screenId] = {
      data: data,
      fetchedAt: Date.now(),
      error: null,
    };
    renderFn(data);
    hideStaleBanner(screenId);
    hideErrorBanner(screenId);
  } catch (err) {
    const lastGood = screenState[screenId];

    if (lastGood && lastGood.data) {
      // Show stale banner but keep last-good data visible
      const ageMs = Date.now() - lastGood.fetchedAt;
      showStaleBanner(screenId, ageMs, err.message);
    } else {
      // No cached data — show error state
      showErrorBanner(screenId, err.message);
    }

    // Exponential backoff on consecutive failures
    screenState[screenId] = {
      ...lastGood,
      error: err.message,
      consecutiveFailures: (lastGood?.consecutiveFailures || 0) + 1,
    };
  }
}
```

### Visual States

| State | Visual | Behavior |
|-------|--------|----------|
| **Fresh** | Normal rendering, green "Last updated: Xs ago" | Data is current |
| **Stale** | Yellow banner: "Data is Xs old -- last fetch failed: {error}" | Last-good data still visible, banner dismissible |
| **Error** | Red banner: "Unable to load data: {error}" | No data to show, full-screen error with retry button |
| **Loading** | Skeleton/pulse animation on metric cards | First load or after error-state retry |

### Polling Backoff & Jitter

On consecutive failures, double the poll interval (capped at 5x normal). All intervals include **random jitter** to prevent synchronized bursts when multiple operator sessions are open:

```javascript
function getEffectiveInterval(screenId) {
  const base = POLL_INTERVALS[screenId];
  const failures = screenState[screenId]?.consecutiveFailures || 0;
  const multiplier = Math.min(Math.pow(2, failures), 5);
  // Add +/- 20% jitter to prevent thundering herd across browser tabs/operators
  const jitter = 0.8 + Math.random() * 0.4;  // range: [0.8, 1.2]
  return Math.round(base * multiplier * jitter);
}
```

This means a 10s base interval becomes 8-12s in practice, and multiple tabs/operators will naturally desynchronize within a few cycles.

---

## 11. Common Query Parameters

| Param | Type | Default | Used By |
|-------|------|---------|---------|
| `workspace_key` | string | session default | All |
| `time_range` | enum (`1h`, `6h`, `24h`, `7d`) | `24h` | Dashboard, alerts |
| `status` | string | all | Queue, batches, alerts |
| `limit` | int | 100 | List endpoints |
| `offset` | int | 0 | Pagination |
| `sort` | string | `-created_at` | List endpoints |
| `search` | string | -- | Tables with search |

### Polling Strategy

| Screen | Poll Interval | Endpoints Polled |
|--------|--------------|-----------------|
| Pipeline Overview | 30s | `/ops/pipeline/kpis`, `/ops/alerts` |
| Queue & Workers | 10s | `/ops/queue/statusCounts`, `/ops/queue/requests` |
| Source Health | 15s | `/ops/sources/kafka`, `/ops/sources/api`, `/ops/sources/dataLossRisk` |
| Batches & Manifests | 60s | `/ops/batches/summary` |
| Checkpoints & Replay | 30s | `/ops/checkpoints/current`, `/ops/replay/active` |
| Bad Records | 60s | `/ops/badRecords/summary` |
| Schema & Medallion | 120s | `/ops/schema/freshnessChain` |
| Admin & Policies | -- | On-demand only |

---

## 12. Route Registration

### ops/routes.clj

```clojure
(ns bitool.ops.routes
  (:require [bitool.ops.dashboard     :as dashboard]
            [bitool.ops.queue         :as queue]
            [bitool.ops.source-health :as source-health]
            [bitool.ops.alerts        :as alerts]
            [bitool.ops.admin         :as admin]
            [bitool.ops.correlation   :as correlation]
            [bitool.ops.timeseries    :as timeseries]
            [ring.util.http-response  :as http-response]))

(defn ops-routes []
  ["/ops"
   ;; Pipeline Overview
   ["/pipeline/kpis"            {:get dashboard/pipeline-kpis}]
   ["/pipeline/sourceStatus"    {:get dashboard/source-status-list}]
   ["/pipeline/recentActivity"  {:get dashboard/recent-activity}]

   ;; Alerts (cross-screen)
   ["/alerts"                   {:get alerts/list-alerts}]
   ["/alerts/history"           {:get alerts/alert-history}]
   ["/alerts/:alert_id/ack"     {:post alerts/acknowledge!}]
   ["/alerts/:alert_id/silence" {:post alerts/silence!}]
   ["/alerts/:alert_id/resolve" {:post alerts/resolve!}]

   ;; Correlation (drill-down, typed by entity kind)
   ["/correlation/request/:request_id" {:get correlation/from-request}]
   ["/correlation/run/:run_id"         {:get correlation/from-run}]
   ["/correlation/batch/:batch_id"     {:get correlation/from-batch}]

   ;; Queue & Workers
   ["/queue/statusCounts"       {:get queue/status-counts}]
   ["/queue/requests"           {:get queue/list-requests}]
   ["/queue/requests/bulkRetry" {:post queue/bulk-retry!}]
   ["/queue/requests/bulkCancel" {:post queue/bulk-cancel!}]
   ["/queue/requests/bulkRequeue" {:post queue/bulk-requeue!}]
   ["/queue/workers"            {:get queue/list-workers}]
   ["/queue/workers/:worker_id/drain"   {:post queue/drain-worker!}]
   ["/queue/workers/:worker_id/undrain" {:post queue/undrain-worker!}]
   ["/queue/workers/:worker_id/forceRelease" {:post queue/force-release!}]
   ["/queue/dlq"                {:get queue/list-dlq}]

   ;; Source Health
   ["/sources/kafka"            {:get source-health/kafka-sources}]
   ["/sources/file"             {:get source-health/file-sources}]
   ["/sources/api"              {:get source-health/api-sources}]
   ["/sources/dataLossRisk"     {:get source-health/data-loss-risk}]
   ["/sources/:source_id/circuitBreaker" {:get source-health/circuit-breaker-state}]
   ["/sources/:source_id/circuitBreaker/reset" {:post source-health/reset-circuit-breaker!}]
   ["/sources/kafka/:source_id/stream" {:get source-health/kafka-stream-detail}]

   ;; Batches & Manifests
   ["/batches/summary"          {:get dashboard/batch-summary}]
   ["/batches/:batch_id/detail" {:get dashboard/batch-detail}]
   ["/batches/:batch_id/artifacts" {:get dashboard/batch-artifacts}]

   ;; Checkpoints & Replay
   ["/checkpoints/current"      {:get dashboard/current-checkpoints}]
   ["/checkpoints/history"      {:get dashboard/checkpoint-history}]
   ["/replay/fromCheckpoint"    {:post dashboard/replay-from-checkpoint!}]
   ["/replay/active"            {:get dashboard/active-replays}]

   ;; Bad Records
   ["/badRecords/summary"       {:get dashboard/bad-record-summary}]
   ["/badRecords/:record_id/payload" {:get dashboard/bad-record-payload}]
   ["/badRecords/bulkIgnore"    {:post dashboard/bulk-ignore-bad-records!}]
   ["/badRecords/export"        {:post dashboard/export-bad-records}]

   ;; Schema & Medallion
   ["/schema/freshnessChain"    {:get dashboard/freshness-chain}]
   ["/schema/driftDetection"    {:get dashboard/schema-drift}]
   ["/medallion/releases"       {:get dashboard/medallion-releases}]

   ;; Admin & Policies (with preview/validate/apply)
   ["/admin/workerSettings"           {:get admin/get-config}]
   ["/admin/workerSettings/preview"   {:post admin/preview-config-change}]
   ["/admin/workerSettings"           {:post admin/apply-config-change!}]
   ["/admin/queueSettings"            {:get admin/get-config}]
   ["/admin/queueSettings/preview"    {:post admin/preview-config-change}]
   ["/admin/queueSettings"            {:post admin/apply-config-change!}]
   ["/admin/sourceLimits"             {:get admin/get-config}]
   ["/admin/sourceLimits/preview"     {:post admin/preview-config-change}]
   ["/admin/sourceLimits"             {:post admin/apply-config-change!}]
   ["/admin/alertThresholds"          {:get admin/get-config}]
   ["/admin/alertThresholds/preview"  {:post admin/preview-config-change}]
   ["/admin/alertThresholds"          {:post admin/apply-config-change!}]
   ["/admin/retentionPolicy"          {:get admin/get-config}]
   ["/admin/retentionPolicy/preview"  {:post admin/preview-config-change}]
   ["/admin/retentionPolicy"          {:post admin/apply-config-change!}]
   ["/admin/configHistory"            {:get admin/config-history}]
   ["/admin/configRollback"           {:post admin/rollback-config!}]

   ;; Time-series (for sparklines)
   ["/timeseries/sparkline"     {:get timeseries/sparkline-data-handler}]
   ["/timeseries/delta"         {:get timeseries/delta-handler}]])
```

### Wire into handler.clj

```clojure
(require '[bitool.ops.routes :as ops])

;; In app-routes:
(ring/ring-handler
  (ring/router
    [(home/home-routes)
     (ops/ops-routes)]    ;; <-- add here
    ...))
```

---

## 13. Frontend Wiring Strategy

### Phase 1: Wire to Existing Legacy Routes

The frontend should first wire to the 64 existing routes (which already work), not the proposed `/ops/*` routes. This avoids the integration gap where the frontend targets an API that doesn't exist yet.

```javascript
// Phase 1: Use existing legacy routes that already work
const OPS_API = {
  // Pipeline Overview
  freshnessDashboard: (ws) => fetchOps(`/freshnessDashboard?workspace_key=${ws}`),
  freshnessAlerts:    (ws) => fetchOps(`/freshnessAlerts?workspace_key=${ws}`),
  usageDashboard:     (ws) => fetchOps(`/usageDashboard?workspace_key=${ws}`),
  observability:      (ws) => fetchOps(`/apiObservabilitySummary?workspace_key=${ws}`),
  observabilityAlerts:(ws) => fetchOps(`/apiObservabilityAlerts?workspace_key=${ws}`),

  // Queue & Workers
  executionRuns:      (params) => fetchOps(`/executionRuns?${qs(params)}`),
  executionDemand:    ()       => fetchOps('/executionDemand'),
  replayRun:          (runId)  => postOps(`/executionRuns/${runId}/replay`),

  // Batches
  apiBatches:         (params) => fetchOps(`/apiBatches?${qs(params)}`),
  bronzeBatches:      (params) => fetchOps(`/bronzeSourceBatches?${qs(params)}`),
  rollbackBatch:      (body)   => postOps('/rollbackApiBatch', body),
  archiveBatch:       (body)   => postOps('/archiveApiBatch', body),

  // Bad Records
  badRecords:         (params) => fetchOps(`/apiBadRecords?${qs(params)}`),
  replayBadRecords:   (body)   => postOps('/replayApiBadRecords', body),

  // Checkpoints
  resetCheckpoint:    (body)   => postOps('/resetApiCheckpoint', body),
  verifyCommitClosure:()       => fetchOps('/verifyApiCommitClosure'),

  // Schema
  schemaApprovals:    (params) => fetchOps(`/apiSchemaApprovals?${qs(params)}`),
  reviewSchema:       (body)   => postOps('/reviewApiSchema', body),
  promoteSchema:      (body)   => postOps('/promoteApiSchema', body),

  // Control Plane
  tenants:            ()       => fetchOps('/controlPlane/tenants'),
  workspaces:         ()       => fetchOps('/controlPlane/workspaces'),
  auditEvents:        (params) => fetchOps(`/controlPlane/auditEvents?${qs(params)}`),
};

function fetchOps(path) {
  return fetch(path).then(handleResponse);
}
function postOps(path, body) {
  return fetch(path, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: body ? JSON.stringify(body) : undefined,
  }).then(handleResponse);
}
function handleResponse(resp) {
  if (!resp.ok) throw new Error(`${resp.status}: ${resp.statusText}`);
  return resp.json();
}
```

### Phase 2: Switch to `/ops/*` Routes

After the new `/ops/*` backend is implemented, update the API object to use new routes. Old routes remain as aliases.

```javascript
// Phase 2: Switch to /ops/* after backend is implemented
const OPS_API_V2 = {
  pipelineKpis:    (ws, tr) => fetchOps(`/ops/pipeline/kpis?workspace_key=${ws}&time_range=${tr}`),
  alerts:          (ws)     => fetchOps(`/ops/alerts?workspace_key=${ws}&state=fired,acknowledged,silenced`),
  queueCounts:     (ws)     => fetchOps(`/ops/queue/statusCounts?workspace_key=${ws}`),
  // ... etc
};
```

---

## 14. Existing Route Migration

Move existing routes from `home.clj` to `ops/routes.clj`, keeping old paths as aliases:

| Current Path | New Path | Notes |
|-------------|----------|-------|
| `/freshnessDashboard` | `/ops/pipeline/freshness` | Wraps `operations/freshness-dashboard` |
| `/freshnessAlerts` | `/ops/alerts?type=freshness` | Wraps `alerts/list-alerts` with filter |
| `/usageDashboard` | `/ops/pipeline/usage` | Wraps `operations/usage-dashboard` |
| `/executionRuns` | `/ops/queue/runs` | Wraps `execution/list-execution-runs` |
| `/executionDemand` | `/ops/queue/demand` | Wraps `execution/execution-demand-snapshot` |
| `/apiBatches` | `/ops/batches/api` | Wraps `runtime/list-api-batches` |
| `/bronzeSourceBatches` | `/ops/batches/bronze` | Wraps `runtime/list-bronze-source-batches` |
| `/apiBadRecords` | `/ops/badRecords/list` | Wraps `runtime/list-api-bad-records` |
| `/apiSchemaApprovals` | `/ops/schema/approvals` | Wraps `runtime/list-api-schema-approvals` |
| `/controlPlane/*` | `/ops/admin/*` | Keep `/controlPlane/*` as-is during migration |

---

## 15. Data Flow Summary

```
┌──────────────────────────────────────────────────────────────┐
│                    opsConsole.html                             │
│  Phase 1: polls legacy routes (/freshnessDashboard, etc.)    │
│  Phase 2: polls /ops/* routes with staleness/error handling  │
└──────────────────────┬───────────────────────────────────────┘
                       │ HTTP (poll with backoff)
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                  ops/routes.clj                                │
│  /ops/pipeline/*  /ops/alerts/*    /ops/correlation/*         │
│  /ops/queue/*     /ops/sources/*   /ops/batches/*             │
│  /ops/checkpoints/* /ops/badRecords/* /ops/schema/*           │
│  /ops/admin/*     /ops/timeseries/*                           │
└───┬───────┬────────┬────────┬────────┬────────┬──────────────┘
    │       │        │        │        │        │
    ▼       ▼        ▼        ▼        ▼        ▼
┌───────┐┌───────┐┌────────┐┌───────┐┌────────┐┌──────────┐
│dashbrd││queue  ││src_hlth││alerts ││admin   ││correlate │ ops/
│       ││       ││        ││       ││        ││timeseries│
└───┬───┘└───┬───┘└───┬────┘└───┬───┘└───┬────┘└────┬─────┘
    │        │        │         │        │           │
    ▼        ▼        ▼         ▼        ▼           ▼
┌──────────────────────────────────────────────────────────────┐
│              Existing bitool internals                         │
│                                                                │
│  runtime.clj ─── execution.clj ─── operations.clj            │
│       │                │                  │                    │
│  [manifest]      [exec_request]    [freshness_status]         │
│  [checkpoint]    [exec_run]        [usage_meter]              │
│  [bad_records]   [lease_heartbeat] [ops_alert] (new)          │
│  [schema_appr]   [dlq]            [ops_config] (new)         │
│                                    [ops_ts_rollup] (new)      │
│  Atoms:                Atoms:                                  │
│  circuit-breaker-state execution-handler-registry             │
│  backpressure-state    endpoint-registry                      │
│  rl-state              kafka-stream-metrics (new)             │
└──────────────────────────────────────────────────────────────┘
```

---

## 16. Implementation Order

| Phase | Scope | Files | Dependencies |
|-------|-------|-------|-------------|
| **P0** | DDL: create `ops_alert`, `ops_config`, `ops_timeseries_rollup` tables; ALTER `execution_request` for drain columns | migration SQL | None |
| **P1** | Alert lifecycle (`ops/alerts.clj`) + background alert-check job | `ops/alerts.clj`, `ops/routes.clj` | P0 |
| **P2** | Pipeline Overview KPIs + time-series rollup | `ops/dashboard.clj`, `ops/timeseries.clj` | P0 |
| **P3** | Queue & Workers (status, list, transactional bulk actions, drain) | `ops/queue.clj` | P0 |
| **P4** | Source Health + Kafka stream observability + data loss detection | `ops/source_health.clj` | P1 (fires alerts) |
| **P5** | Correlation engine (drill-down across request/run/batch/checkpoint/bad-records) | `ops/correlation.clj` | P3, P4 |
| **P6** | Admin settings with preview/validate/apply/rollback | `ops/admin.clj` | P0 |
| **P7** | Batch detail + checkpoint history + bad record detail (with correlation links) | `ops/dashboard.clj` additions | P5 |
| **P8** | Schema freshness chain + drift + medallion releases | `ops/dashboard.clj` additions | P2 |
| **P9** | Frontend Phase 1: wire to existing legacy routes + polling/staleness UX | `opsConsole.js` | None (uses existing routes) |
| **P10** | Frontend Phase 2: switch to `/ops/*` routes | `opsConsole.js` | P1-P8 |
| **P11** | Migrate existing routes from `home.clj` to `ops/routes.clj` | `ops/routes.clj`, `home.clj` | P10 |

---

## 17. Security Considerations

1. **All `/ops/*` routes require authentication** -- wrap with existing auth middleware
2. **Mutation endpoints (`POST`) require `admin` or `ops-admin` role** -- add role check middleware
3. **No dynamic SQL** -- all queries use parameterized statements
4. **Audit trail** -- all mutations call `control-plane/record-audit-event!`
5. **Rate limiting on bulk operations** -- max 100 items per bulk action
6. **Payload size limit on bad record export** -- cap at 10,000 records per export
7. **Alert silence has max duration** -- silence_until cannot exceed 7 days from now
8. **Config changes require version check** -- prevents silent overwrite by concurrent operators
