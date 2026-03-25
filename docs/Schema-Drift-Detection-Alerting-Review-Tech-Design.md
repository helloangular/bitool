# Schema Drift Detection, Alerting, and Human Review — Tech Design

**Scope:** End-to-end schema lifecycle: detect drift → alert operator → review → apply or reject → audit

**Status:** Draft

**Depends on:** existing `endpoint_schema_snapshot`, `endpoint_schema_approval` tables, `schema_infer.clj`, enforcement modes in `runtime.clj`

---

## 1. Current State

Bitool already has the foundational pieces. This design identifies what's incomplete, what's missing, and what needs to be wired together.

### 1.1 What Exists

| Capability | Status | Location |
|---|---|---|
| Schema inference from API responses | Complete | `schema_infer.clj` |
| Schema snapshot capture per run | Complete | `runtime.clj:persist-endpoint-schema-snapshot!` |
| Schema diff (new fields, missing fields, type changes) | Complete | `runtime.clj:compute-schema-drift` |
| Type compatibility matrix (widening rules) | Complete | `runtime.clj:widening-type-change?` |
| Enforcement modes (strict/additive/permissive) | Complete | `runtime.clj:apply-schema-enforcement` |
| Schema approval table and review workflow | Complete | `runtime.clj:review-api-schema!`, `promote-api-schema!` |
| Schema approval requirement check | Complete | `runtime.clj:ensure-schema-approved!` |
| Route handlers for review/promote | Complete | `home.clj: /apiSchemaApprovals`, `/reviewApiSchema`, `/promoteApiSchema` |
| Ops dashboard query for drift | Complete | `ops/dashboard.clj` |

### 1.2 What's Missing

| Capability | Status |
|---|---|
| **Alerting** — no automatic notification when drift is detected | Missing |
| **UI for schema review** — no in-app review screen; operator must use API routes directly | Missing |
| **DDL application** — no `ALTER TABLE ADD COLUMN` for approved schema changes on Bronze tables | Missing |
| **Drift history timeline** — snapshots exist but no consolidated view of how a schema evolved | Missing |
| **Auto-apply for additive mode** — additive merges fields in memory during the run but doesn't persist the DDL change to the Bronze table | Incomplete |
| **Schema diff visibility in execution monitor** — drift is captured in `schema_drift_json` but not surfaced in the run result UI | Missing |
| **Promotion → DDL pipeline** — approving + promoting a schema doesn't trigger table alteration | Missing |

---

## 2. Design Overview

The schema lifecycle has five stages:

```
DETECT → ALERT → REVIEW → APPLY → AUDIT
```

### Stage 1: Detect (exists)
During each API ingestion run, the inferrer samples records and compares against the current schema. Drift is stored in `endpoint_schema_snapshot.schema_drift_json`.

### Stage 2: Alert (new)
When drift is detected, create a `schema_drift_event` record and notify the operator via configurable channels (in-app notification, optional webhook).

### Stage 3: Review (partially exists)
Operator views the diff, decides to approve/reject. Currently requires direct API calls. Needs a UI.

### Stage 4: Apply (new)
When a schema change is approved and promoted, apply the DDL change to the Bronze table (`ALTER TABLE ADD COLUMN`, type widening).

### Stage 5: Audit (partially exists)
All decisions are recorded. Needs a consolidated timeline view.

---

## 3. Schema Drift Event Model

### 3.1 New Table: `schema_drift_event`

Captures each detected drift as a discrete event, separate from the snapshot (which is a full schema picture). This is the alerting source.

```sql
CREATE TABLE IF NOT EXISTS schema_drift_event (
  event_id          BIGSERIAL PRIMARY KEY,
  workspace_id      INTEGER NOT NULL,
  graph_id          INTEGER NOT NULL,
  api_node_id       INTEGER NOT NULL,
  endpoint_name     VARCHAR(512) NOT NULL,
  source_system     VARCHAR(128) NOT NULL,
  run_id            VARCHAR(64),
  snapshot_id       BIGINT,

  -- Diff summary
  new_field_count       INTEGER NOT NULL DEFAULT 0,
  missing_field_count   INTEGER NOT NULL DEFAULT 0,
  type_change_count     INTEGER NOT NULL DEFAULT 0,
  drift_json            TEXT NOT NULL,

  -- Classification
  drift_severity    VARCHAR(16) NOT NULL DEFAULT 'info',
  -- info: new optional fields only
  -- warning: missing fields or compatible type changes (widening)
  -- breaking: incompatible type changes (e.g., STRING→INT)

  -- State
  acknowledged      BOOLEAN NOT NULL DEFAULT FALSE,
  acknowledged_by   VARCHAR(128),
  acknowledged_at   TIMESTAMP,

  -- Metadata
  schema_hash_before VARCHAR(64),
  schema_hash_after  VARCHAR(64),
  enforcement_mode   VARCHAR(32),
  detected_at_utc    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_drift_event_endpoint
  ON schema_drift_event (workspace_id, graph_id, api_node_id, endpoint_name, detected_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_drift_event_unacked
  ON schema_drift_event (workspace_id, acknowledged, detected_at_utc DESC)
  WHERE acknowledged = FALSE;
```

### 3.2 Drift Severity Classification

```clojure
(defn- classify-drift-severity
  [{:keys [new_fields missing_fields type_changes]}]
  (cond
    ;; Incompatible type changes (e.g., STRING→INT)
    (seq (incompatible-type-changes type_changes))
    "breaking"

    ;; Missing fields (columns disappeared) or compatible type changes (widening)
    (or (seq missing_fields)
        (seq type_changes))
    "warning"

    ;; New optional fields only
    (seq new_fields)
    "info"

    :else nil))   ;; no drift
```

| Severity | Meaning | Operator Action |
|---|---|---|
| `info` | New optional fields appeared. Source added data. | Review and approve if useful. |
| `warning` | Fields disappeared or types widened. Source changed behavior. | Investigate. May need schema update. |
| `breaking` | Incompatible type change (e.g., STRING→INT). Would break existing consumers. | Must review. Ingestion blocked under strict mode. |

> **Future:** If the source schema provides nullability metadata (e.g., from OpenAPI specs or database connectors), missing non-nullable fields can be escalated from `warning` to `breaking`. The current inferrer samples API responses and does not produce reliable nullability data, so all missing fields are classified as `warning` for now.

---

## 4. Detection Integration

### 4.1 Where to Hook

In `runtime.clj`, after `compute-schema-drift` returns a non-nil result, persist a `schema_drift_event`:

```clojure
;; In apply-schema-enforcement, after drift is computed:
(when drift
  (persist-schema-drift-event!
    conn
    {:graph-id       graph-id
     :api-node-id    api-node-id
     :endpoint-name  endpoint-name
     :source-system  source-system
     :run-id         run-id
     :drift          drift
     :enforcement-mode enforcement-mode
     :schema-hash-before (schema-fields-hash current-fields)
     :schema-hash-after  (schema-fields-hash inferred-fields)}))
```

### 4.2 Deduplication

Don't create a new event if the same drift (same `schema_hash_before` → `schema_hash_after` transition) was already detected and is unacknowledged:

```clojure
(defn- persist-schema-drift-event!
  [conn params]
  (let [existing (jdbc/execute-one!
                   (db-opts conn)
                   [(str "SELECT event_id FROM schema_drift_event
                          WHERE workspace_id = ? AND graph_id = ? AND api_node_id = ? AND endpoint_name = ?
                            AND schema_hash_before = ? AND schema_hash_after = ?
                            AND acknowledged = FALSE
                          LIMIT 1")
                    (:workspace-id params) (:graph-id params) (:api-node-id params) (:endpoint-name params)
                    (:schema-hash-before params) (:schema-hash-after params)])]
    (when-not existing
      ;; Insert new event + fire alert
      ...)))
```

This prevents alert spam when the same drift is detected on every run.

---

## 5. Alerting

### 5.1 In-App Notification Table

```sql
CREATE TABLE IF NOT EXISTS schema_notification (
  notification_id   BIGSERIAL PRIMARY KEY,
  workspace_id      INTEGER NOT NULL,
  event_id          BIGINT NOT NULL REFERENCES schema_drift_event(event_id),
  channel           VARCHAR(32) NOT NULL DEFAULT 'in_app',
  severity          VARCHAR(16) NOT NULL,
  title             VARCHAR(256) NOT NULL,
  body              TEXT,
  read              BOOLEAN NOT NULL DEFAULT FALSE,
  created_at_utc    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_ws_unread
  ON schema_notification (workspace_id, created_at_utc DESC)
  WHERE read = FALSE;
```

### 5.2 Notification Creation

After inserting a drift event:

```clojure
(defn- create-drift-notification!
  [conn event-id {:keys [endpoint-name severity new-count missing-count type-change-count]}]
  (let [title (case severity
                "breaking" (format "Breaking schema change on %s" endpoint-name)
                "warning"  (format "Schema warning on %s" endpoint-name)
                (format "New fields detected on %s" endpoint-name))
        body  (str (when (pos? new-count) (format "%d new field(s). " new-count))
                   (when (pos? missing-count) (format "%d missing field(s). " missing-count))
                   (when (pos? type-change-count) (format "%d type change(s)." type-change-count)))]
    (jdbc/execute!
      conn
      ["INSERT INTO schema_notification (event_id, channel, severity, title, body)
        VALUES (?, 'in_app', ?, ?, ?)"
       event-id severity title body])))
```

### 5.3 Optional Webhook Alert

For teams that want Slack/PagerDuty/email integration:

```clojure
(def ^:private severity-rank {"info" 0 "warning" 1 "breaking" 2})

(defn- fire-webhook-alert!
  [event]
  (when-let [url (some-> (get env :schema-drift-webhook-url) str str/trim not-empty)]
    (let [min-severity (or (get env :schema-drift-webhook-severity) "warning")]
      (when (>= (severity-rank (:drift_severity event) 0)
                (severity-rank min-severity 1))
        (try
          (clj-http/post url
            {:content-type :json
             :body (json/generate-string
                     {:event_type "schema_drift"
                      :severity (:drift_severity event)
                      :endpoint_name (:endpoint_name event)
                      :graph_id (:graph_id event)
                      :new_fields (:new_field_count event)
                      :missing_fields (:missing_field_count event)
                      :type_changes (:type_change_count event)
                      :detected_at (:detected_at_utc event)
                      :review_url (format "%s/schema-review?graph_id=%d&endpoint=%s"
                                          (get env :bitool-base-url "http://localhost:8080")
                                          (:graph_id event)
                                          (:endpoint_name event))})})
          (catch Exception e
            (log/warn e "Failed to send schema drift webhook")))))))
```

**Environment variables:**
- `SCHEMA_DRIFT_WEBHOOK_URL` — webhook endpoint (optional)
- `SCHEMA_DRIFT_WEBHOOK_SEVERITY` — minimum severity to fire webhook (default: `warning`)

### 5.4 Route: Notification Feed

```clojure
;; GET /schemaNotifications?unread_only=true&limit=50
(defn list-schema-notifications [request]
  (let [params (:params request)
        workspace-id (get-workspace-id request)
        unread-only (= "true" (str (:unread_only params)))
        limit (min 200 (or (parse-optional-int (:limit params) :limit) 50))]
    (http-response/ok
      (jdbc/execute!
        (db-opts db/ds)
        [(str "SELECT n.*, e.endpoint_name, e.drift_severity, e.graph_id
               FROM schema_notification n
               JOIN schema_drift_event e ON e.event_id = n.event_id
               WHERE n.workspace_id = ?"
              (when unread-only " AND n.read = FALSE")
              " ORDER BY n.created_at_utc DESC LIMIT ?")
         workspace-id limit]))))

;; POST /schemaNotifications/markRead  {notification_ids: [1,2,3]}
```

---

## 6. Human Review UI

### 6.1 Schema Review Panel

A new panel in the modeling console or ops console that shows:

```
┌─────────────────────────────────────────────────────────┐
│  Schema Review: fleet/vehicles/fuel-energy              │
│  Drift Severity: ⚠ warning                             │
│  Detected: 2026-03-22 14:30 UTC  |  Run: abc-123       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  NEW FIELDS (+2)                                        │
│  ┌─────────────────┬────────┬──────────┬──────────┐     │
│  │ Column          │ Type   │ Coverage │ Include? │     │
│  ├─────────────────┼────────┼──────────┼──────────┤     │
│  │ def_cost_gallon │ DOUBLE │ 0.87     │ [✓]      │     │
│  │ driver_id       │ STRING │ 0.93     │ [✓]      │     │
│  └─────────────────┴────────┴──────────┴──────────┘     │
│                                                         │
│  MISSING FIELDS (-1)                                    │
│  ┌──────────────────┬────────┬─────────────────────┐    │
│  │ Column           │ Type   │ Action              │    │
│  ├──────────────────┼────────┼─────────────────────┤    │
│  │ legacy_fuel_code │ STRING │ [Keep] [Drop] [Null]│    │
│  └──────────────────┴────────┴─────────────────────┘    │
│                                                         │
│  TYPE CHANGES (1)                                       │
│  ┌───────────────┬──────────┬──────────┬───────────┐    │
│  │ Column        │ Was      │ Now      │ Compat?   │    │
│  ├───────────────┼──────────┼──────────┼───────────┤    │
│  │ engine_hours  │ INT      │ BIGINT   │ ✓ widen   │    │
│  └───────────────┴──────────┴──────────┴───────────┘    │
│                                                         │
│  Notes: ____________________________________________    │
│                                                         │
│  [ Approve & Apply ]  [ Approve (no DDL) ]  [ Reject ]  │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Review Actions

| Action | What Happens |
|---|---|
| **Approve & Apply** | Sets review_state=approved, promoted=true. Triggers DDL application (Section 7). |
| **Approve (no DDL)** | Sets review_state=approved, promoted=true. Schema is promoted for future runs but no ALTER TABLE yet. Operator will apply DDL manually or in a maintenance window. |
| **Reject** | Sets review_state=rejected. Schema stays as-is. Drift event marked acknowledged. |

### 6.3 Per-Field Decisions

For new fields, the operator can:
- **Include** — column will be added to Bronze table on apply
- **Exclude** — column will not be added (marked `enabled: false` in the approved schema)

For missing fields, the operator can:
- **Keep** — column stays in the table (source stopped sending it, but it may come back)
- **Drop** — column marked for removal (not auto-dropped; flagged for manual DDL)
- **Null** — keep column but mark nullable

For type changes:
- Compatible widening (INT→BIGINT) — auto-applied if approved
- Incompatible change (STRING→INT) — requires explicit confirmation and data migration plan

### 6.4 Decision Persistence

Per-field decisions are stored as a JSONB column on the existing `endpoint_schema_approval` table:

```sql
ALTER TABLE endpoint_schema_approval
  ADD COLUMN IF NOT EXISTS field_decisions JSONB;
```

The `field_decisions` value is a map keyed by column name:

```json
{
  "def_cost_gallon": {"action": "include"},
  "driver_id":       {"action": "exclude"},
  "legacy_fuel_code": {"action": "keep"},
  "engine_hours":    {"action": "widen", "from_type": "INT", "to_type": "BIGINT"}
}
```

| Field Category | Valid Actions |
|---|---|
| New field | `include`, `exclude` |
| Missing field | `keep`, `drop`, `null_out` |
| Type change (compatible) | `widen` |
| Type change (incompatible) | `reject` (blocks approval) |

**Contract with DDL generation:** When `generate-schema-ddl` builds ALTER statements, it reads `field_decisions` from the approval record. Only fields with `action = "include"` or `action = "widen"` produce DDL. Fields with `"exclude"`, `"keep"`, or `"drop"` are omitted from the DDL plan (`"drop"` is flagged for manual DDL only). This is the canonical approved field set — `approved-fields` is the inferred schema filtered by these decisions, not the raw inferred schema.

**Review route change:** The existing `/reviewApiSchema` POST body gains an optional `field_decisions` parameter. When present, it is persisted on the approval record alongside `review_state`. The review UI sends this when the operator submits per-field choices.

---

## 7. DDL Application Pipeline

### 7.1 When Triggered

DDL application runs when:
1. Operator clicks "Approve & Apply" (immediate)
2. Operator promotes a schema via API and `auto_apply_ddl` is enabled
3. Additive mode auto-promotion (if configured)

### 7.1.1 Target Resolution

The Bronze table name, connection ID, and warehouse type are resolved from the endpoint node's configuration in the graph:

```clojure
(let [node    (get-in graph [:n api-node-id :na])
      conn-id    (:conn_id node)
      table-name (:table_name node)
      warehouse  (get-warehouse-type conn-id)]  ;; "postgres", "snowflake", "databricks"
  ...)
```

The `graph_id` and `api_node_id` on the drift event (or approval record) are used to look up the node. This resolution must happen at DDL-apply time (not at approval time) so the route always targets the current connection and table name.

### 7.2 DDL Generation

```clojure
(defn- generate-schema-ddl
  "Generate ALTER TABLE statements for an approved schema change.
   `approved-fields` is the inferred schema filtered by field_decisions from the approval record —
   only fields with action=include or action=widen are present."
  [warehouse table-name current-fields approved-fields]
  (let [current-by-name (into {} (map (juxt :column_name identity)) current-fields)
        approved-by-name (into {} (map (juxt :column_name identity)) approved-fields)
        new-columns (remove #(contains? current-by-name (:column_name %))
                            (filter :enabled approved-fields))
        type-changes (for [[col-name approved] approved-by-name
                           :let [current (get current-by-name col-name)]
                           :when (and current
                                      (not= (:type current) (:type approved))
                                      (widening-type-change? (:type current) (:type approved)))]
                       {:column_name col-name
                        :from_type (:type current)
                        :to_type (:type approved)})]
    {:add-columns (mapv (fn [col]
                          {:sql (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s"
                                        table-name
                                        (quote-ident (:column_name col))
                                        (sql-type-for-warehouse warehouse (:type col)))
                           :column col})
                        new-columns)
     :widen-columns (mapv (fn [{:keys [column_name from_type to_type]}]
                            {:sql (format "ALTER TABLE %s ALTER COLUMN %s TYPE %s"
                                          table-name
                                          (quote-ident column_name)
                                          (sql-type-for-warehouse warehouse to_type))
                             :column_name column_name
                             :from_type from_type
                             :to_type to_type})
                          type-changes)}))
```

### 7.3 Execution

```clojure
(defn apply-schema-ddl!
  "Execute approved DDL changes against the Bronze table.
   Returns a summary of applied changes."
  [conn-id table-name ddl-plan]
  (let [results (atom [])]
    (jdbc/with-transaction [tx (db/get-opts conn-id nil)]
      ;; Add columns first
      (doseq [{:keys [sql column]} (:add-columns ddl-plan)]
        (try
          (jdbc/execute! tx [sql])
          (swap! results conj {:action "add_column" :column (:column_name column) :status "applied"})
          (catch Exception e
            (swap! results conj {:action "add_column" :column (:column_name column)
                                 :status "failed" :error (.getMessage e)})
            (throw e))))

      ;; Widen types
      (doseq [{:keys [sql column_name from_type to_type]} (:widen-columns ddl-plan)]
        (try
          (jdbc/execute! tx [sql])
          (swap! results conj {:action "widen_type" :column column_name
                               :from from_type :to to_type :status "applied"})
          (catch Exception e
            (swap! results conj {:action "widen_type" :column column_name
                                 :status "failed" :error (.getMessage e)})
            (throw e)))))
    @results))
```

### 7.4 Safety Rules

1. **Never DROP COLUMN automatically.** Missing fields are flagged but never auto-removed.
2. **Never narrow types.** BIGINT→INT is always rejected.
3. **Always use IF NOT EXISTS** for ADD COLUMN to be idempotent.
4. **Wrap in transaction.** All DDL for one approval is atomic.
5. **Record every DDL execution** in a `schema_ddl_history` audit table.

### 7.5 DDL Audit Table

```sql
CREATE TABLE IF NOT EXISTS schema_ddl_history (
  ddl_id            BIGSERIAL PRIMARY KEY,
  workspace_id      INTEGER NOT NULL,
  event_id          BIGINT REFERENCES schema_drift_event(event_id),
  approval_id       BIGINT,
  graph_id          INTEGER NOT NULL,
  endpoint_name     VARCHAR(512) NOT NULL,
  table_name        VARCHAR(512) NOT NULL,
  action            VARCHAR(32) NOT NULL,    -- add_column, widen_type
  column_name       VARCHAR(256) NOT NULL,
  from_type         VARCHAR(64),
  to_type           VARCHAR(64),
  sql_executed      TEXT NOT NULL,
  status            VARCHAR(16) NOT NULL,    -- applied, failed, skipped
  error_message     TEXT,
  applied_by        VARCHAR(128),
  applied_at_utc    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ddl_history_ws
  ON schema_ddl_history (workspace_id, graph_id, endpoint_name, applied_at_utc DESC);
```

---

## 8. Enforcement Mode Behavior Matrix

Full behavior across all modes with the new pipeline:

| Mode | Drift Detected | Ingestion | Alert | Require Review | Auto-Apply DDL |
|---|---|---|---|---|---|
| `none` | Not checked | Always succeeds | No | No | No |
| `advisory` (permissive) | Recorded in snapshot + drift event | Always succeeds | Yes (in-app + webhook) | No (optional) | No |
| `additive` | Recorded; compatible changes merged in-memory | Succeeds if compatible | Yes | No (optional) | Yes for new columns; rejects incompatible |
| `strict` | Recorded | **Fails** on any drift | Yes (breaking severity) | **Yes** — must promote before next run | Only after explicit approval |

### 8.1 Additive Auto-Apply Semantics

When additive mode auto-applies new columns, the system creates a **synthetic approval record** on `endpoint_schema_approval` with:
- `reviewed_by = 'system:additive_auto'`
- `review_state = 'auto_approved'`
- `field_decisions` populated with `{"action": "include"}` for each new compatible field

This synthetic approval is then passed through the normal DDL pipeline (generate → execute → audit). The resulting `schema_ddl_history` entries reference this synthetic `approval_id`, ensuring a complete audit trail even without human review.

**Scope of auto-apply:** Only `add_column` for new fields is auto-applied. Type changes and missing fields always require human review regardless of enforcement mode. Incompatible new field types (e.g., a field whose inferred type conflicts with an existing column) are excluded from auto-apply and escalated to a drift event requiring manual review.

### 8.2 Recommended Progression

```
Development:  advisory    — see everything, block nothing
Staging:      additive    — auto-add new columns, block breaking changes
Production:   strict      — all changes require human review + promotion
```

---

## 9. Schema Timeline View

### 9.1 Consolidated History API

```
GET /schemaTimeline?graph_id=2317&endpoint_name=fleet/vehicles/fuel-energy&limit=50
```

Returns a unified timeline of:
- Schema snapshots (what was inferred)
- Drift events (what changed)
- Reviews (who approved/rejected)
- DDL applications (what was applied to the table)

```json
{
  "endpoint_name": "fleet/vehicles/fuel-energy",
  "current_field_count": 12,
  "timeline": [
    {
      "type": "snapshot",
      "time": "2026-03-22T14:30:00Z",
      "run_id": "abc-123",
      "field_count": 12,
      "schema_hash": "a1b2c3..."
    },
    {
      "type": "drift_detected",
      "time": "2026-03-23T08:15:00Z",
      "severity": "info",
      "new_fields": ["def_cost_gallon", "driver_id"],
      "missing_fields": [],
      "type_changes": []
    },
    {
      "type": "review",
      "time": "2026-03-23T09:00:00Z",
      "reviewed_by": "akulkarni",
      "decision": "approved",
      "promoted": true,
      "notes": "New cost field from Samsara Q1 release"
    },
    {
      "type": "ddl_applied",
      "time": "2026-03-23T09:00:05Z",
      "changes": [
        {"action": "add_column", "column": "def_cost_gallon", "type": "DOUBLE"},
        {"action": "add_column", "column": "driver_id", "type": "STRING"}
      ],
      "applied_by": "akulkarni"
    }
  ]
}
```

---

## 10. Route Handlers

### 10.1 New Routes

> **Tenant scoping:** All routes below scope queries by the authenticated user's `workspace_id`, derived from the request context (same pattern as existing graph routes). No cross-workspace data is ever returned.

| Route | Method | Purpose |
|---|---|---|
| `/schemaDriftEvents` | GET | List drift events (filterable by graph, endpoint, severity, acknowledged) |
| `/schemaDriftEvents/:id/acknowledge` | POST | Mark drift event as acknowledged |
| `/schemaNotifications` | GET | Notification feed (unread, severity filter) |
| `/schemaNotifications/markRead` | POST | Mark notifications as read |
| `/schemaTimeline` | GET | Consolidated schema history for an endpoint |
| `/applySchemaChange` | POST | Trigger DDL application for an approved+promoted schema |
| `/schemaPreviewDdl` | POST | Preview DDL that would be applied (dry run) |

### 10.2 Existing Routes (no changes)

| Route | Method | Purpose |
|---|---|---|
| `/apiSchemaApprovals` | GET | List approvals and snapshots |
| `/reviewApiSchema` | POST | Review a schema (approve/reject) |
| `/promoteApiSchema` | POST | Promote a schema |

---

## 11. New Tables Summary

| Table | Purpose |
|---|---|
| `schema_drift_event` | One row per detected drift. Alerting source. |
| `schema_notification` | In-app notification feed. |
| `schema_ddl_history` | Audit trail of every ALTER TABLE executed. |

Existing tables (minor schema additions):
- `endpoint_schema_snapshot` — full schema captured per run (no changes)
- `endpoint_schema_approval` — review decisions and promotions (adds `field_decisions JSONB` column)

---

## 12. Implementation Plan

### Phase 1: Drift Events + Alerting
1. Create `schema_drift_event` table via `ensure-table!`
2. Create `schema_notification` table
3. Hook `persist-schema-drift-event!` into `apply-schema-enforcement`
4. Add drift severity classification
5. Add deduplication on `(hash_before, hash_after, unacknowledged)`
6. Create in-app notifications on drift event insert
7. Add webhook alerting (optional, env-var configured)
8. Add GET `/schemaDriftEvents` and GET `/schemaNotifications` routes

### Phase 2: Review UI
9. Build schema review panel component (web component in `resources/public/`)
10. Wire to existing `/reviewApiSchema` and `/promoteApiSchema` routes
11. Add per-field include/exclude decisions for new fields
12. Add per-field keep/drop/null decisions for missing fields
13. Show type compatibility status for type changes

### Phase 3: DDL Application
14. Implement `generate-schema-ddl` (diff → ALTER TABLE statements)
15. Implement `apply-schema-ddl!` (execute within transaction)
16. Create `schema_ddl_history` audit table
17. Add POST `/applySchemaChange` route
18. Add POST `/schemaPreviewDdl` route (dry run)
19. Wire "Approve & Apply" button to review + DDL in one action

### Phase 4: Timeline + Polish
20. Implement GET `/schemaTimeline` (unified view across all tables)
21. Add notification badge to main UI toolbar
22. Add drift event count to execution monitor run results
23. Add acknowledgment flow for drift events

### Validation Criteria
- Advisory mode: drift detected, event created, notification visible, ingestion succeeds
- Strict mode: drift detected, ingestion blocked, operator approves, next run succeeds
- Additive mode: compatible new fields auto-merged, notification created, incompatible changes blocked
- DDL application: ALTER TABLE adds columns correctly, audit trail recorded
- Dedup: same drift on repeated runs creates only one unacknowledged event
- Webhook fires for warning+ severity when configured
- Timeline shows complete history for an endpoint

---

## 13. Decisions Log

| # | Decision | Rationale |
|---|---|---|
| D1 | Separate `schema_drift_event` from `endpoint_schema_snapshot` | Snapshots are full schema pictures; events are change deltas. Different query patterns, different retention. |
| D2 | Dedup unacknowledged drift events by hash pair | Prevents alert fatigue when the same drift fires every run. |
| D3 | Three severity levels (info/warning/breaking) | Maps to operator urgency. Info = nice to know. Warning = investigate. Breaking = must act. |
| D4 | Never auto-DROP columns | Column removal requires explicit manual DDL. Too dangerous to automate. |
| D5 | DDL in transaction, audited per-column | Atomic application with rollback on failure. Per-column audit for forensics. |
| D6 | Webhook is optional, in-app is always-on | Teams without Slack integration still get visibility. Webhook is additive. |
| D7 | Review UI shows per-field decisions | Operator may want to accept some new fields and reject others from the same drift event. |
| D8 | All new tables include `workspace_id` | Multi-tenant scoping. Drift events, notifications, and DDL history must never bleed across workspaces. |
| D9 | Per-field decisions stored as JSONB on `endpoint_schema_approval` | Keeps the approval record self-contained. `generate-schema-ddl` reads this as the canonical approved field set. |
| D10 | Additive auto-apply creates synthetic approval records | Ensures audit trail completeness. Every DDL execution traces back to an approval, even machine-generated ones. |
| D11 | `breaking` severity limited to incompatible type changes | Current inferrer samples API responses and cannot reliably determine field nullability. Missing-field escalation deferred until nullability metadata is available. |
