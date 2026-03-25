# Modeling Release Execution Integration — Tech Design

**Scope:** Gap 1 (Execution Framework Integration) + Gap 2 (Release Binding Model)

**Status:** Draft v2 — addresses review findings on identity, dedupe, node_id, audit drift, lease model

**Depends on:** existing `execution.clj` framework, `automation.clj` release model, `plugins.clj` handler registry

---

## 1. Problem Statement

Silver and Gold release execution currently runs inline on the HTTP request thread:

```
POST /executeSilverRelease
  → execute-silver-release! (inline, blocking)
    → Snowflake/Postgres: execute SQL, return result
    → Databricks: trigger-job!, return submitted status (async poll happens separately)
```

This means:
- No retry on transient failure (network, rate limit, warehouse unavailable)
- No lease/heartbeat for long-running Databricks jobs
- No concurrency control (two users can trigger the same release simultaneously — caught only by advisory lock inside the TX)
- No dead-letter queue for permanent failures
- No auditable trigger-source tracking (manual vs scheduled vs chained)
- No queuing — every trigger is immediate or fails

Bronze ingestion solved all of these via `execution.clj`. The goal is to bring Silver/Gold releases into the same framework.

---

## 2. Design Principles

1. **Reuse, don't fork.** Silver/Gold use the same `execution_request` table, worker loop, lease system, retry engine, and DLQ as Bronze. No parallel infrastructure.
2. **Request kinds are the extension point.** The plugin system (`plugins/register-execution-handler!`) already supports arbitrary request kinds. Add `:silver_release` and `:gold_release`.
3. **State bridging, not state duplication.** `compiled_model_run` continues to own modeling-specific state (SQL artifact, Databricks run_id, warehouse response). `execution_request` owns lifecycle state (queued/leased/running/succeeded/failed). The handler bridges the two.
4. **Trigger source is first-class metadata.** Every enqueued request records why it exists.
5. **Resolve at enqueue time, audit faithfully.** The active release is resolved when the request is enqueued. The `execution_request` row records exactly what was requested and what was resolved. No drift between what the queue says and what runs.

---

## 3. Trigger Source Taxonomy

Every `execution_request` already has a `trigger_type` column (VARCHAR 64). Define the following values for modeling releases:

| trigger_type | Meaning |
|---|---|
| `manual` | User clicked "Execute" in the UI or called the API directly |
| `schedule` | Cron-based modeling release schedule fired |
| `bronze_success` | Upstream Bronze ingestion completed successfully |
| `silver_success` | Upstream Silver release completed successfully |
| `retry` | Automatic retry of a failed request (set by execution framework) |
| `reconcile` | Operator-initiated re-execution of a stale or orphaned run |

These are recorded at enqueue time and are immutable for the life of the request.

---

## 4. Release Binding Model

### 4.1 The Problem

A schedule or dependency says "run Silver for target model X." But which release? If we bind to a specific `release_id`, the schedule breaks every time a new release is published.

### 4.2 Binding Abstraction

Introduce a `release_binding` concept used by both schedules and dependencies:

```
release_binding = {
  layer:        "silver" | "gold"
  target_model: "bronze_fleet_vehicles_silver"    -- the target_model identifier
  mode:         "follow_active" | "pinned"
  pinned_release_id: NULL | 42                     -- only when mode = "pinned"
}
```

**Default:** `follow_active` — at **enqueue time**, resolve to the latest `active = true` release for that `(layer, target_model)`.

**Pinned mode:** Advanced use case — execute a specific release version regardless of what's currently active. Useful for rollback or A/B testing.

### 4.2.1 Canonical Persistence Shape

The binding must have one canonical serialized shape shared by:

- manual execute requests
- schedule rows
- dependency rows
- operator re-run / reconcile actions

Canonical JSON representation:

```json
{
  "layer": "silver",
  "target_model": "bronze_fleet_vehicles_silver",
  "mode": "follow_active",
  "pinned_release_id": null
}
```

Rules:

- `layer` and `target_model` are always required
- `mode` defaults to `"follow_active"`
- `pinned_release_id` must be null unless `mode = "pinned"`
- schedules and dependencies should persist this JSON verbatim, not partially denormalize it in different shapes

This keeps schedule resolution, dependency resolution, and manual re-execution on the same contract.

### 4.3 Resolution at Enqueue Time

The binding is resolved **when the request is enqueued**, not when the worker claims it. This eliminates audit drift (Finding 3) and makes the dedupe key stable (Finding 1).

```clojure
(defn- resolve-release-binding
  "Resolve a release_binding to a concrete release row.
   Called at enqueue time. The resolved release_id, graph_id, and target_node_id
   are persisted on the execution_request so the queue row is self-describing."
  [binding]
  (case (or (:mode binding) "follow_active")
    "pinned"
    (let [row (release-by-id (:pinned_release_id binding))]
      (when-not row
        (throw (ex-info "Pinned release not found"
                        {:pinned_release_id (:pinned_release_id binding)
                         :status 404})))
      row)

    ;; default: follow_active
    (let [row (active-release-for-target-model (:layer binding) (:target_model binding))]
      (when-not row
        (throw (ex-info "No active published release for target model"
                        {:layer (:layer binding)
                         :target_model (:target_model binding)
                         :status 404})))
      row)))
```

### 4.4 What Happens When a New Release Is Published

- **Queued requests:** Run the release that was active when they were enqueued. They do NOT auto-upgrade. This is intentional — the queue row says exactly what will run.
- **Schedules:** Next cron tick resolves the new active release and enqueues a fresh request with it.
- **Dependencies:** Next upstream success resolves the new active release at trigger time.

For `pinned` bindings: no effect. Pinned means pinned.

### 4.5 Public API Surface for Route Layer

`automation.clj` currently defines `release-by-id` and `graph-artifact-by-id` as private (`defn-`). The route layer needs these for enqueue-time resolution.

Add public wrappers:

```clojure
;; In automation.clj — public API for execution integration
(defn get-release
  "Public wrapper for release-by-id. Returns nil if not found."
  [release-id]
  (release-by-id release-id))

(defn get-graph-artifact
  "Public wrapper for graph-artifact-by-id. Returns nil if not found."
  [artifact-id]
  (graph-artifact-by-id nil artifact-id))

(defn resolve-active-release
  "Resolve a release binding to a concrete release row. Throws if not found."
  [binding]
  (resolve-release-binding binding))
```

### 4.6 Binding Resolution Policy

Binding resolution behavior must be uniform across all trigger sources:

- `manual` execute from the UI
- cron schedule
- Bronze-success dependency
- Silver-success dependency
- operator retry / reconcile

Default policy:

- bindings are resolved at enqueue time
- queued requests are pinned to the resolved release they were created with
- later publishes do not mutate already-queued work
- the next trigger resolves again and picks up the new active release

This means the queue is immutable and auditable, while schedules and dependencies still follow the latest published release on their next trigger.

---

## 5. Request Identity and Dedupe Key

### 5.1 The Problem (Finding 1)

The current `request_key` formula is:

```
request_kind::graph_id::node_id::environment::endpoint_name
```

For Bronze, `graph_id` and `node_id` are stable inputs. For modeling releases, the execution target is `(layer, target_model)`, and the graph/node are derived artifacts that can change across releases. If the dedupe key uses `graph_id::node_id`, a new release pointing to a different generated graph would create a different key, breaking `skip_if_running`.

### 5.2 Binding-Centric Request Key

For `:silver_release` and `:gold_release`, the request key must be keyed on the **binding target**, not on derived graph artifacts:

```
silver_release::{layer}::{target_model}::default::
gold_release::{layer}::{target_model}::default::
```

Concrete example:
```
silver_release::silver::bronze_fleet_vehicles_silver::default::
```

This means: at most one active (queued/leased/running) execution per `(layer, target_model)`, regardless of which `release_id` or `graph_id` is involved.

### 5.3 Implementation

Override the request_key construction for modeling request kinds. The `enqueue-request!` function computes the key from its arguments. The enqueue wrapper passes the binding components instead of `graph_id`/`node_id`:

```clojure
(defn enqueue-silver-release-request!
  [resolved-context {:keys [trigger-type created-by max-retries workspace-key] :as opts}]
  (let [{:keys [release-row graph-id target-node-id binding]} resolved-context]
    (enqueue-request! :silver_release graph-id target-node-id
      {:trigger-type   (or trigger-type "manual")
       :max-retries    (or max-retries (default-modeling-max-retries))
       :workspace-key  workspace-key
       :request-params {:release_binding  binding
                        :resolved_release_id (:release_id release-row)
                        :resolved_graph_id   graph-id
                        :resolved_target_node_id target-node-id
                        :created_by       (or created-by "system")}})))
```

But the request_key must use the binding, not `graph_id::node_id`. Two options:

**Option A: Custom request_key override.**

Add a `:request-key-override` option to `enqueue-request!`:

```clojure
;; In enqueue-request!, after computing default request-key:
(let [request-key (or request-key-override
                      (string/join "::" [(name request-kind) graph-id node-id environment (or endpoint-name "")]))]
  ...)
```

The enqueue wrapper passes:

```clojure
:request-key-override (str (name :silver_release) "::" (:layer binding) "::" (:target_model binding) "::default::")
```

**Option B: Encode binding into graph_id/node_id slots.**

Use the `graph_id` column to store a synthetic binding identifier (e.g., hash of `layer::target_model`) and `node_id` to store the resolved target_node_id. This preserves the existing key formula but repurposes the fields.

**Recommendation: Option A.** It's explicit, doesn't repurpose columns, and the change to `enqueue-request!` is minimal (one `or` on the key computation). The advisory lock and dedup query both use `request_key`, so the change is localized.

---

## 6. Concrete Enqueue-Time Values for execution_request Columns

### 6.1 The Problem (Finding 2 + Finding 3)

`execution_request` has `NOT NULL` columns: `graph_id`, `node_id`, `graph_version_id`, `graph_version`. These are written at enqueue time. For modeling releases:

- `graph_id` — the generated graph from the graph artifact (available at enqueue time via resolved release)
- `node_id` — the target node in the generated graph (available at enqueue time via graph artifact)
- `graph_version_id` / `graph_version` — from `ensure-active-release!` which creates/finds a graph_release row

All of these **can and should** be resolved at enqueue time because we resolve the binding at enqueue time.

### 6.2 Resolution Chain at Enqueue Time

```clojure
(defn- resolve-modeling-enqueue-context!
  "Resolve everything needed to enqueue a modeling release request.
   Returns a map with :release-row, :graph-id, :target-node-id, :binding."
  [binding]
  (let [release-row    (resolve-release-binding binding)
        graph-artifact (graph-artifact-by-id nil (:graph_artifact_id release-row))
        _              (when-not graph-artifact
                         (throw (ex-info "Release has no graph artifact"
                                         {:release_id (:release_id release-row)
                                          :status 409})))
        graph-id       (:graph_id graph-artifact)
        graph          (db/getGraph graph-id)
        target-node-id (target-node-id-from-graph-artifact graph-artifact graph)
        _              (when-not target-node-id
                         (throw (ex-info "Generated graph has no target node"
                                         {:release_id (:release_id release-row)
                                          :graph_id graph-id
                                          :status 409})))]
    {:release-row    release-row
     :graph-id       graph-id
     :target-node-id target-node-id
     :binding        binding}))
```

This means:
- `execution_request.graph_id` = the graph that will actually be executed
- `execution_request.node_id` = the target node in that graph (NOT NULL satisfied)
- `execution_request.graph_version_id/graph_version` = from `ensure-active-release!` on that graph_id
- `execution_request.request_params` = `{release_binding, resolved_release_id, resolved_graph_id, resolved_target_node_id, created_by}`

The `execution_request` row is fully self-describing. No drift. No nulls.

### 6.3 "Requested Binding" vs "Resolved Release" Audit Fields

The `request_params` JSONB carries both:

```json
{
  "release_binding": {
    "layer": "silver",
    "target_model": "bronze_fleet_vehicles_silver",
    "mode": "follow_active"
  },
  "resolved_release_id": 42,
  "resolved_graph_id": 2317,
  "resolved_target_node_id": 15,
  "created_by": "system"
}
```

An auditor can see: "This request was enqueued because the schedule fired for `bronze_fleet_vehicles_silver` (binding), and at that moment the active release was #42 from graph 2317 (resolution)."

---

## 7. New Request Kinds

### 7.1 Registration

In `execution.clj` `register-builtin-execution-handlers!`, add:

```clojure
(plugins/register-execution-handler!
  :silver_release
  {:description "Silver modeling release execution handler"
   :workload-classifier (fn [{:keys [trigger-type]}]
                          (if (= "manual" trigger-type) "interactive" "modeling"))
   :execute execute-silver-release-handler!})

(plugins/register-execution-handler!
  :gold_release
  {:description "Gold modeling release execution handler"
   :workload-classifier (fn [{:keys [trigger-type]}]
                          (if (= "manual" trigger-type) "interactive" "modeling"))
   :execute execute-gold-release-handler!})
```

### 7.2 Enqueue Functions

```clojure
(defn enqueue-silver-release-request!
  "Resolve the binding, then enqueue a silver_release execution request.
   All graph/node/version values are resolved at enqueue time."
  [binding {:keys [trigger-type created-by max-retries workspace-key]}]
  (let [ctx (resolve-modeling-enqueue-context! binding)]
    (enqueue-request! :silver_release (:graph-id ctx) (:target-node-id ctx)
      {:trigger-type        (or trigger-type "manual")
       :max-retries         (or max-retries (default-modeling-max-retries))
       :workspace-key       workspace-key
       :request-key-override (modeling-request-key :silver_release binding)
       :request-params      {:release_binding         binding
                             :resolved_release_id     (:release_id (:release-row ctx))
                             :resolved_graph_id       (:graph-id ctx)
                             :resolved_target_node_id (:target-node-id ctx)
                             :created_by              (or created-by "system")}})))

(defn enqueue-gold-release-request!
  "Resolve the binding, then enqueue a gold_release execution request."
  [binding {:keys [trigger-type created-by max-retries workspace-key]}]
  (let [ctx (resolve-modeling-enqueue-context! binding)]
    (enqueue-request! :gold_release (:graph-id ctx) (:target-node-id ctx)
      {:trigger-type        (or trigger-type "manual")
       :max-retries         (or max-retries (default-modeling-max-retries))
       :workspace-key       workspace-key
       :request-key-override (modeling-request-key :gold_release binding)
       :request-params      {:release_binding         binding
                             :resolved_release_id     (:release_id (:release-row ctx))
                             :resolved_graph_id       (:graph-id ctx)
                             :resolved_target_node_id (:target-node-id ctx)
                             :created_by              (or created-by "system")}})))

(defn- modeling-request-key
  [request-kind binding]
  (string/join "::" [(name request-kind) (:layer binding) (:target_model binding) "default" ""]))
```

### 7.3 Changes to enqueue-request!

Add support for `:request-key-override` in the opts map:

```clojure
;; In enqueue-request!, line ~521:
(let [request-key (or (:request-key-override opts)
                      (string/join "::" [(name request-kind) graph-id node-id environment (or endpoint-name "")]))]
  ...)
```

One new key in the opts destructuring. No other changes to the shared path.

---

## 8. Handler Implementation

### 8.1 Silver Release Handler

The handler receives a fully-resolved `execution_request`. It reads the `resolved_release_id` from `request_params` — no re-resolution needed.

```clojure
(defn- execute-silver-release-handler!
  [request-row request-params]
  (let [release-id (:resolved_release_id request-params)
        created-by (or (:created_by request-params) "system")
        release-row (release-by-id release-id)
        _           (when-not release-row
                      (throw (ex-info "Resolved release no longer exists"
                                      {:release_id release-id
                                       :failure_class "config_error"})))]

    ;; Delegate to existing execute-silver-release-tx! inside a transaction
    (let [pending-run (jdbc/with-transaction [tx db/ds]
                        (execute-silver-release-tx! tx release-row created-by))]

      ;; Link compiled_model_run back to execution_request
      (link-model-run-to-request! (:model_run_id pending-run) (:request_id request-row))

      ;; Execute based on warehouse type
      (case (:warehouse pending-run)
        "snowflake"
        (let [response (jdbc/execute! (db/get-opts (:conn_id pending-run) nil)
                                      [(:compiled_sql pending-run)])]
          (complete-model-run! (:model_run_id pending-run)
                               {:status "succeeded"
                                :response-json {:result response}
                                :completed-at (now-utc)})
          {:status "succeeded"
           :model_run_id (:model_run_id pending-run)
           :release_id release-id})

        "postgresql"
        (let [response (execute-postgresql-materialization!
                         (:conn_id pending-run)
                         (:sql_ir pending-run)
                         (:compiled_sql pending-run))]
          (complete-model-run! (:model_run_id pending-run)
                               {:status "succeeded"
                                :response-json {:result response}
                                :completed-at (now-utc)})
          {:status "succeeded"
           :model_run_id (:model_run_id pending-run)
           :release_id release-id})

        ;; Databricks: handler owns the full poll lifecycle
        (execute-databricks-release-with-polling! pending-run request-row)))))
```

Gold handler is structurally identical, calling `execute-gold-release-tx!` instead.

### 8.2 Databricks Polling Inside the Handler (Option B only)

Option A (reconciler bridge) is removed. The handler owns the full Databricks lifecycle under the worker lease.

```clojure
(defn- execute-databricks-release-with-polling!
  "Trigger a Databricks job and poll until terminal. Runs under the worker's
   lease heartbeat, which keeps renewing as long as this thread is alive."
  [pending-run request-row]
  (let [response (dbx-jobs/trigger-job!
                   (:conn_id pending-run)
                   (:job_id pending-run)
                   (:params pending-run))
        model-run-id (:model_run_id pending-run)]
    (update-model-run-progress! model-run-id
                                 {:status "submitted"
                                  :response-json response
                                  :external-run-id (some-> (:run_id response) str)})
    ;; Poll until terminal state
    (loop [poll-count 0]
      (Thread/sleep (min 30000 (* 1000 (+ 5 (* 2 poll-count)))))
      (let [poll-result (poll-silver-model-run! model-run-id)
            status      (:status poll-result)]
        (cond
          (= "succeeded" status)
          {:status "succeeded" :model_run_id model-run-id :release_id (:release_id pending-run)}

          (#{"failed" "timed_out"} status)
          (throw (ex-info (str "Databricks job " status)
                          {:failure_class (if (= "timed_out" status)
                                            "transient_platform_error"
                                            "permanent_model_error")
                           :model_run_id model-run-id
                           :databricks_status status
                           :response poll-result}))

          :else (recur (inc poll-count)))))))
```

### 8.3 Reconciler as Safety Net

The existing `reconcile-silver-model-runs!` poller remains as orphan recovery only. If a worker crashes mid-poll:
1. The lease expires
2. `sweep-expired-leases!` marks the execution_request as `worker_orphaned`
3. Retry logic re-enqueues or sends to DLQ
4. The `compiled_model_run` row may still show `submitted`/`running` — the reconciler catches these and completes them based on the actual Databricks run status

This is the same orphan recovery pattern Bronze uses.

---

## 9. Failure Classification Additions (Gap 6 — inline)

Extend `classify-failure` in `execution.clj` to recognize modeling-specific failures:

```clojure
;; Add to classify-failure cond branches:

;; Warehouse SQL/model errors — permanent, do not retry
(re-find #"(?i)syntax error|semantic error|table.*not found|column.*not found|permission denied|access denied"
         lower)
"permanent_model_error"

;; Databricks platform errors — transient, retry
(re-find #"(?i)cluster.*terminated|spot.*interrupted|workspace.*unavailable|internal error.*databricks"
         lower)
"transient_platform_error"

;; Config errors (already exists, extend with modeling patterns)
(re-find #"no active published release|pinned release not found|no.*job_id configured|release no longer exists"
         lower)
"config_error"
```

Add `"transient_platform_error"` to `retryable-failure-classes`.

Three-way classification:

| Class | Retryable? | Meaning |
|---|---|---|
| `permanent_model_error` | No → DLQ | SQL syntax, missing table/column, permission denied. Human must fix and re-publish. |
| `config_error` | No → DLQ | No active release, no job_id, bad binding, deleted release. Operator must fix config. |
| `transient_platform_error` | Yes | Databricks cluster terminated, spot interruption, network timeout. Auto-retry with backoff. |

Concrete examples by backend:

| Backend | Example failure | Class |
|---|---|---|
| PostgreSQL | `syntax error at or near ...` | `permanent_model_error` |
| PostgreSQL | `column ... does not exist` | `permanent_model_error` |
| PostgreSQL | `permission denied for table ...` | `permanent_model_error` |
| PostgreSQL | `password authentication failed` | `config_error` |
| PostgreSQL | connection timeout / socket reset | `transient_platform_error` |
| Snowflake | SQL compilation error / object does not exist | `permanent_model_error` |
| Snowflake | invalid username/password / expired token | `config_error` |
| Snowflake | transient warehouse unavailable / statement timeout from platform instability | `transient_platform_error` |
| Databricks | Jobs API 400 due to bad job_id / missing parameter contract | `config_error` |
| Databricks | run result `FAILED` because compiled SQL is invalid | `permanent_model_error` |
| Databricks | run result `TIMEDOUT` / cluster terminated / internal error | `transient_platform_error` |

---

## 10. Request-Key and Overlap Policy (Gap 5 — inline)

### 10.1 Default: skip_if_running

The existing `enqueue-request!` function already implements this:

```clojure
(if-let [existing (active-request-by-key tx request-key)]
  (request->response existing)   ;; ← returns the existing request, does NOT enqueue a new one
  ...)
```

Since the request_key for modeling releases is `silver_release::silver::bronze_fleet_vehicles_silver::default::`, any attempt to enqueue while one is already `queued`, `leased`, or `running` returns the existing request.

This is correct because the key is binding-centric. Even if the active release changes between two enqueue attempts, the key is the same — the first enqueue wins.

### 10.2 Overlap Scenarios

| Scenario | Behavior |
|---|---|
| Scheduled Silver fires while one is running | Returns existing request (skip) |
| Bronze success triggers Silver while cron also triggers | First enqueue wins; second returns existing |
| User clicks "Execute" while scheduled run is in-flight | Returns existing request (UI shows "already running") |
| Multiple Bronze endpoints complete in rapid succession | All trigger attempts hit the same request_key lock; first enqueue wins |
| New release published while request is queued | Queued request runs the release it was enqueued with. Next trigger picks up the new one. |

### 10.3 Future: enqueue_once

If needed later, add a `force` option that allows enqueuing even when one is active (for operator overrides). Not in initial implementation.

### 10.4 Bronze -> Silver Trigger Scoping

Bronze-success chaining must not be graph-wide by default. The trigger matching contract should use the most specific available source identity:

```json
{
  "graph_id": 2247,
  "source_node_id": 2,
  "endpoint_name": "fleet/vehicles"
}
```

Resolution rules:

1. Match on `graph_id + source_node_id + endpoint_name` when all three are available.
2. Fall back to `graph_id + source_node_id` only when the source does not have endpoint granularity.
3. Fall back to `graph_id` only for sources that genuinely cannot identify a narrower unit.

Default chaining semantics:

- one Bronze endpoint success should only enqueue the Silver models explicitly bound to that source scope
- multiple endpoints inside the same Bronze graph must not fan out to every Silver model attached to the graph
- multiple rapid upstream successes for the same bound model collapse through the binding-centric request key

This is required to avoid over-triggering when one Bronze graph contains multiple APIs or multiple endpoint streams.

### 10.5 Operator-Visible Dedupe Behavior

When `skip_if_running` returns an existing request instead of creating a new one, the response should make that explicit:

- `request_id` = existing request
- `request_status` = current status of that request
- `deduped` = `true`
- `resolved_release_id` = release already attached to that request

This avoids the current ambiguity of "did Bitool queue a new run or ignore me?"

---

## 11. Execution_Request ↔ Compiled_Model_Run Lifecycle Mapping

```
execution_request.status    compiled_model_run.status    Notes
─────────────────────────   ─────────────────────────    ─────
queued                      (not yet created)            Request waiting for worker
leased                      (not yet created)            Worker claimed, handler starting
running                     pending                      Handler created model_run, TX committed
running                     submitted                    Databricks job triggered (handler polling)
running                     running                      Databricks polling shows in-progress
succeeded                   succeeded                    Handler returned successfully
failed                      failed                       Handler threw, classify-failure routed it
failed (DLQ)                failed                       Max retries exhausted
```

### 11.1 External Warehouse Status Mapping

The implementation should use an explicit normalization table for warehouse-specific states:

| External system | External state | compiled_model_run.status | execution_request.status |
|---|---|---|---|
| PostgreSQL | SQL started | `pending` | `running` |
| PostgreSQL | SQL completed | `succeeded` | `succeeded` |
| PostgreSQL | SQL exception | `failed` | `failed` |
| Snowflake | query submitted | `pending` | `running` |
| Snowflake | query completed | `succeeded` | `succeeded` |
| Snowflake | query failed | `failed` | `failed` |
| Databricks | run submitted | `submitted` | `running` |
| Databricks | `PENDING` | `pending` | `running` |
| Databricks | `RUNNING` | `running` | `running` |
| Databricks | `TERMINATED/SUCCESS` | `succeeded` | `succeeded` |
| Databricks | `TERMINATED/FAILED` | `failed` | `failed` |
| Databricks | `TIMEDOUT` | `timed_out` | `failed` |
| Databricks | `CANCELED` | `cancelled` | `failed` |
| Worker lease expiry | orphaned poll thread | unchanged until reconciled | `worker_orphaned` |

Rules:

- `execution_request` is the control-plane lifecycle
- `compiled_model_run` is the modeling/runtime lifecycle
- external states are normalized into those two models, never exposed as ad hoc free-form strings in the queue layer
- a request that reaches `worker_orphaned` must remain linked to the last known `compiled_model_run` so reconciliation has a stable handoff point

Key invariants:
- `compiled_model_run` is created inside the handler (as it is today)
- `execution_request` lifecycle wraps it
- The handler is responsible for calling `complete-model-run!` on success and throwing on failure
- `compiled_model_run.execution_request_id` links back to the parent request (set by `link-model-run-to-request!` immediately after creation)
- For Databricks, execution_request stays `running` for the entire poll duration — the handler thread is alive, the lease heartbeat renews

---

## 12. Migration from Inline to Queued Execution

### 12.1 Route Handlers

Update `/executeSilverRelease` and `/executeGoldRelease` in `home.clj` to enqueue instead of executing inline. Use the public API from `automation.clj`:

```clojure
(defn execute-silver-release [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params     (:params request)
          release-id (parse-required-int (:release_id params) :release_id)
          release    (modeling-automation/get-release release-id)
          _          (when-not release
                       (throw (ex-info "Silver release not found"
                                       {:release_id release-id :status 404})))
          _          (when-not (= "silver" (:layer release))
                       (throw (ex-info "Release is not a Silver release"
                                       {:release_id release-id :status 400})))
          binding    {:layer        "silver"
                      :target_model (:target_model release)
                      :mode         "follow_active"}
          result     (ingest-execution/enqueue-silver-release-request!
                       binding
                       {:trigger-type "manual"
                        :created-by   (or (:created_by params)
                                          (get-in request [:session :user])
                                          "system")})]
      (http-response/ok result))
    (catch ...)))
```

Note: the route no longer needs to call private functions. It uses `get-release` (public) and passes the binding to the enqueue function, which resolves everything internally.

### 12.1.1 Route Response Contract

The queued execute routes should return a stable response contract:

```json
{
  "request_id": "uuid",
  "request_status": "queued",
  "request_kind": "silver_release",
  "deduped": false,
  "release_binding": {
    "layer": "silver",
    "target_model": "bronze_fleet_vehicles_silver",
    "mode": "follow_active"
  },
  "resolved_release_id": 42,
  "resolved_graph_id": 2317,
  "resolved_target_node_id": 15
}
```

If the request was deduped because one is already active:

- return HTTP 200, not an error
- set `deduped = true`
- return the existing `request_id`
- preserve the same binding and resolved-release metadata from the existing request

The UI should poll by `request_id` first, then follow the linked `compiled_model_run` once it exists.

### 12.2 Backward Compatibility

During transition, support both paths:
- `POST /executeSilverRelease` → enqueues (new path)
- `POST /executeSilverReleaseSync` → inline execution (deprecated, for debugging)

Remove the sync path after validation.

### 12.3 UI / Polling Migration

The modeling console currently reasons mostly in terms of `model_run_id`. The migration path should be:

1. enqueue route returns `request_id`
2. UI polls queue/request status using `request_id`
3. once `compiled_model_run.execution_request_id` is populated, the UI may also show the linked model run details
4. execution monitor should display both:
   - control-plane status (`queued`, `running`, `failed`, `succeeded`)
   - modeling/runtime status (`pending`, `submitted`, `running`, `succeeded`, `failed`)

This avoids losing visibility during the gap between request claim and model-run creation.

---

## 13. Data Model Changes

### 13.1 No New Tables for Gap 1+2

`execution_request` and `compiled_model_run` already exist. No new tables needed.

### 13.2 Column Additions

**`execution_request`** — no schema changes. `request_params` (JSONB) carries the full audit context:

```json
{
  "release_binding": {"layer": "silver", "target_model": "...", "mode": "follow_active"},
  "resolved_release_id": 42,
  "resolved_graph_id": 2317,
  "resolved_target_node_id": 15,
  "created_by": "system"
}
```

**`compiled_model_run`** — add optional FK back to execution_request:

```sql
ALTER TABLE compiled_model_run
  ADD COLUMN IF NOT EXISTS execution_request_id UUID REFERENCES execution_request(request_id);
```

Set by the handler immediately after `execute-silver-release-tx!` creates the model_run.

### 13.3 Changes to enqueue-request!

Add one optional key to the opts destructuring:

```clojure
;; Current:
[request-kind graph-id node-id {:keys [environment endpoint-name trigger-type request-params max-retries workspace-key]}]

;; New:
[request-kind graph-id node-id {:keys [environment endpoint-name trigger-type request-params max-retries workspace-key request-key-override]}]
```

And one `or` in the key computation:

```clojure
(let [request-key (or request-key-override
                      (string/join "::" [(name request-kind) graph-id node-id environment (or endpoint-name "")]))]
  ...)
```

No other changes to the shared enqueue path. The advisory lock, dedup check, queue partition, and workload classification all use `request-key` as before.

### 13.4 Observability and Operator Controls

Gap 1 is not operationally complete without explicit surfaces for operators. At minimum, the queue/run UI and API should expose:

- `trigger_type`
- requested `release_binding`
- resolved `release_id`
- resolved `graph_id`
- resolved `target_node_id`
- upstream trigger context when present:
  - `upstream_request_id`
  - `upstream_run_id`
  - `upstream_graph_id`
  - `upstream_source_node_id`
  - `upstream_endpoint_name`
- current `execution_request.status`
- linked `compiled_model_run.status`
- retry count / next retry time
- whether the request was deduped

Operator actions should eventually include:

- retry now
- reconcile stale/orphaned run
- cancel queued request
- inspect why-triggered lineage

These do not all need to ship in Phase 1 UI, but the request and run models must retain enough metadata for them.

---

## 14. Worker Configuration and Lease Model

### 14.1 Workload Classes

New workload class `"modeling"` for scheduled/chained releases, `"interactive"` for manual triggers.

Workers can be configured to accept modeling workloads:

```
BITOOL_WORKER_WORKLOAD_CLASSES=api,scheduled,modeling,interactive
```

Or run dedicated modeling workers:

```
BITOOL_WORKER_WORKLOAD_CLASSES=modeling
```

### 14.2 Lease Duration (Finding 6)

The current worker computes one global lease duration per process from `EXECUTION_WORKER_LEASE_SECONDS` (default 300). The heartbeat thread renews every `heartbeat_seconds` (lease / 3). For Bronze API calls that finish in seconds, 300s is fine.

For Databricks modeling jobs that run for minutes to hours, 300s is still safe **because the heartbeat thread renews the lease continuously while the handler's poll loop is alive.** The risk window is:

```
lease_duration - heartbeat_interval = time between last heartbeat and lease expiry
```

With default 300s lease and 100s heartbeat, the risk window is 200s. If the worker crashes and doesn't renew, the orphan sweeper claims the request after 200s. This is acceptable.

**However**, if the org runs mixed workers (API + modeling), the global lease applies to all. Two deployment options:

**Option A: Dedicated modeling workers.** Run a separate worker process with `BITOOL_WORKER_WORKLOAD_CLASSES=modeling` and `EXECUTION_WORKER_LEASE_SECONDS=3600`. Cleanest separation.

**Option B: Per-request-kind lease in claim SQL.** Modify `claim-next-request!` to read a `requested_lease_seconds` column (or derive from request_kind). More complex, deferred unless needed.

**Recommendation: Option A for initial rollout.** Dedicated workers are operationally simpler and avoid the coupling. Option B is available if mixed-worker deployment becomes a requirement.

---

## 15. Sequence Diagrams

### 15.1 Manual Silver Execution (PostgreSQL)

```
User → POST /executeSilverRelease {release_id: 42}
  → home.clj:
    → get-release(42) → release row (layer: silver, target_model: X)
    → build binding {layer: silver, target_model: X, mode: follow_active}
  → execution.clj: enqueue-silver-release-request!(binding, {trigger-type: manual})
    → resolve-modeling-enqueue-context!(binding)
      → active-release-for-target-model("silver", X) → release #42
      → graph-artifact-by-id(release.graph_artifact_id) → graph 2317
      → target-node-id-from-graph-artifact(...) → node 15
    → enqueue-request!(:silver_release, 2317, 15, {request-key-override: "silver_release::silver::X::default::"})
    → INSERT execution_request (status: queued, graph_id: 2317, node_id: 15)
  → HTTP 200 {request_id, status: queued}

Worker loop (2s poll):
  → claim-next-request! → UPDATE status = leased
  → execute-silver-release-handler!(request-row, request-params)
    → release-by-id(42) → release row
    → execute-silver-release-tx!(tx, release-row) → INSERT compiled_model_run
    → link-model-run-to-request!(model_run_id, request_id)
    → case "postgresql": execute SQL directly
    → complete-model-run!(succeeded)
  → handler returns → UPDATE execution_request status = succeeded
```

### 15.2 Scheduled Silver Execution (Databricks)

```
Modeling schedule poller (30s):
  → cron matches for target_model X
  → build binding {layer: silver, target_model: X, mode: follow_active}
  → enqueue-silver-release-request!(binding, {trigger-type: "schedule"})
    → resolve at enqueue time → release #43 (latest active), graph 2320, node 18
    → INSERT execution_request (status: queued)
  → skip_if_running if one already active for this binding

Dedicated modeling worker:
  → claim-next-request! → UPDATE status = leased
  → execute-silver-release-handler!
    → release-by-id(43) → release row
    → execute-silver-release-tx! → INSERT compiled_model_run (status: pending)
    → trigger-job! → Databricks API → run_id 12345
    → update-model-run-progress!(submitted, external_run_id: 12345)
    → poll loop (under lease heartbeat, heartbeat renews every ~100s):
      → poll-silver-model-run! → get-run! → RUNNING → sleep 15s
      → poll-silver-model-run! → get-run! → RUNNING → sleep 20s
      → poll-silver-model-run! → get-run! → TERMINATED/SUCCESS
      → complete-model-run!(succeeded)
  → handler returns → UPDATE execution_request status = succeeded
```

### 15.3 Bronze Success → Silver Chain (Gap 4 preview)

```
Bronze execution_request for graph 2317, endpoint "fleet/vehicles/fuel-energy" succeeds:
  → post-completion hook:
    → find modeling_release_dependencies where upstream matches (graph_id: 2317, endpoint: ...)
    → for each downstream silver binding:
      → enqueue-silver-release-request!(binding, {trigger-type: "bronze_success"})
      → resolve at enqueue time → latest active silver release
      → INSERT execution_request (status: queued)
      → skip_if_running dedupes concurrent triggers from multiple endpoints
  → normal worker flow picks it up
```

### 15.4 Trigger Metadata for Chained Runs

Every chained request should carry upstream provenance in `request_params`:

```json
{
  "trigger_type": "bronze_success",
  "release_binding": {"layer": "silver", "target_model": "bronze_fleet_vehicles_silver", "mode": "follow_active"},
  "resolved_release_id": 42,
  "upstream_request_id": "uuid",
  "upstream_run_id": "uuid",
  "upstream_graph_id": 2247,
  "upstream_source_node_id": 2,
  "upstream_endpoint_name": "fleet/vehicles"
}
```

This is required for:

- audit lineage
- debugging unexpected auto-triggers
- future operator features like "re-run downstream from this Bronze run"

---

## 16. Implementation Plan

### Phase 1: Framework Extension
1. Add `:request-key-override` support to `enqueue-request!` (one `or` in key computation)
2. Add public API wrappers to `automation.clj` (`get-release`, `get-graph-artifact`, `resolve-active-release`)
3. Implement `resolve-modeling-enqueue-context!` and `modeling-request-key`
4. Add `execution_request_id` column to `compiled_model_run`
5. Implement `link-model-run-to-request!`

### Phase 2: Handlers + Registration
6. Implement `execute-silver-release-handler!` and `execute-gold-release-handler!`
7. Implement `execute-databricks-release-with-polling!`
8. Register `:silver_release` and `:gold_release` in `register-builtin-execution-handlers!`
9. Extend `classify-failure` with `permanent_model_error`, `transient_platform_error`, modeling `config_error` patterns
10. Add `"transient_platform_error"` to `retryable-failure-classes`

### Phase 3: Enqueue Functions + Route Migration
11. Implement `enqueue-silver-release-request!` and `enqueue-gold-release-request!`
12. Update `/executeSilverRelease` and `/executeGoldRelease` routes to enqueue
13. Add `/executeSilverReleaseSync` as deprecated fallback
14. Update modeling console JS to handle queued response (poll for completion)

### Phase 4: Operational Readiness
15. Configure dedicated modeling worker (env vars, documentation)
16. Validate reconciler as orphan safety net
17. End-to-end test: manual PG, manual Databricks, retry on transient failure, DLQ on permanent failure

### Validation Criteria
- Manual silver/gold execution works end-to-end through the queue (PG, Snowflake, Databricks)
- `execution_request` row has correct `graph_id`, `node_id`, `graph_version` at enqueue time
- `request_params` contains both binding and resolved release metadata
- Bronze-success chaining matches only the intended `(graph_id, source_node_id, endpoint_name)` scope
- Retry fires on `transient_platform_error` (Databricks spot interruption)
- `permanent_model_error` (SQL syntax) goes to DLQ
- Duplicate trigger returns existing request (`skip_if_running` via binding-centric key)
- Execute route returns `request_id`, `deduped`, and resolved-release metadata
- Databricks jobs complete within the lease (heartbeat renews)
- `compiled_model_run.execution_request_id` links back correctly
- Reconciler catches orphaned model_runs from crashed workers

---

## 17. Decisions Log

| # | Decision | Rationale |
|---|---|---|
| D1 | Resolve binding at enqueue time, not claim time | Eliminates audit drift (Finding 3) and makes dedupe key stable (Finding 1). Schedules pick up new releases on next tick. |
| D2 | Binding-centric request_key via `request-key-override` | `(layer, target_model)` is the true identity. Graph/node are derived. One `or` change to `enqueue-request!`. |
| D3 | All execution_request NOT NULL columns populated at enqueue | Resolved from binding → release → graph_artifact → target_node. No nulls, no drift. |
| D4 | Option B only (handler polls Databricks) | Single ownership model. No bridge needed. Reconciler kept as orphan safety net. |
| D5 | Public API wrappers in automation.clj | Route layer calls public functions, not private internals. |
| D6 | Dedicated modeling workers for lease isolation | Global lease is safe with heartbeat, but dedicated workers are operationally simpler. Per-request lease deferred. |
| D7 | Default overlap policy: skip_if_running | Inherited from existing advisory lock + active-request-by-key check. No new code needed. |
