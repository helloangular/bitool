# Sheetz Implementation Tech Design

## 1. Purpose

This document defines the technical design for implementing the Sheetz telematics pipeline in BiTool using:

- Samara API as source
- BiTool as ingestion and control layer
- Databricks as storage, transformation, and governance layer
- Unity Catalog for governed object management
- A minimal custom `audit.*` schema for pipeline-control state only

This is an implementation design for this repository. It is intentionally narrower and more concrete than the higher-level architecture in [docs/sheetz.md](/Users/aaryakulkarni/bitool/docs/sheetz.md).

---

## 2. Goals

### 2.1 Functional goals

- Ingest Samara endpoints into Databricks Bronze tables.
- Support full, incremental, and sliding-window incremental extraction.
- Persist checkpoint and watermark state by endpoint.
- Standardize Bronze metadata envelope for replay and debugging.
- Trigger Silver and Gold processing in Databricks.
- Keep the custom audit footprint small and avoid duplicating Unity Catalog or Databricks-native metadata.

### 2.2 Technical goals

- Reuse the existing BiTool graph model where possible.
- Extend the current API connector instead of introducing a second ingestion framework.
- Add Databricks support with minimal disruption to the existing JDBC-centric codebase.
- Separate control-plane responsibilities from compute/storage responsibilities.

### 2.3 Non-goals for MVP

- Full Databricks Spark job authoring from the BiTool canvas.
- A complete Silver/Gold visual modeling language for every medallion transform.
- Replacing Unity Catalog lineage, permissions, or system tables with custom equivalents.
- Streaming ingestion in the first release.

---

## 3. Current State In This Repository

### 3.1 Existing capabilities

The codebase already contains:

- A graph model for ETL and service nodes in `src/clj/bitool/graph2.clj`
- Generic ETL transforms such as table, join, projection, filter, aggregation, sorter, function, mapping, and target
- An API connector with pagination, auth, and JSON extraction helpers in `src/clj/bitool/connector/api.clj`
- OpenAPI endpoint discovery and import
- Scheduler and webhook nodes at the graph-configuration level
- JDBC-based database connections and target writes in `src/clj/bitool/db.clj`

### 3.2 Key implementation gaps

The Sheetz use case is blocked by the following gaps:

1. No active batch ETL executor for `Ap -> transforms -> Tg`.
2. No Databricks connection type or Databricks-aware target writer.
3. API node does not model watermarking, extraction strategy, retry policy, or Bronze/Silver naming.
4. Scheduler exists as metadata only; it does not execute ingestion graphs.
5. There are no custom audit/control tables yet.
6. There is no explicit Bronze envelope writer or bad-record quarantine path.

---

## 4. Target Architecture

### 4.1 Responsibility split

BiTool will be responsible for:

- source-specific API extraction
- pagination and auth
- endpoint-level control configuration
- checkpoint and watermark management
- Bronze envelope creation
- endpoint-level run tracking
- dispatch of downstream Databricks processing

Databricks will be responsible for:

- Bronze table storage in Unity Catalog
- Silver canonical transformations
- Gold marts and KPI tables
- governance, permissions, and lineage
- job orchestration for medallion processing

### 4.2 Runtime flow

```text
Scheduler / manual trigger
  -> load enabled ingestion configs
  -> run Samara endpoint extraction
  -> write Bronze rows to Databricks
  -> update checkpoint and run detail
  -> trigger Databricks Silver job
  -> trigger Databricks Gold job on schedule
```

### 4.3 Recommended MVP execution mode

For the MVP, use Databricks SQL Warehouse connectivity from BiTool for:

- Bronze DDL
- Bronze inserts
- checkpoint/audit table writes
- optional control SQL such as `MERGE` into checkpoint tables

Rationale:

- It fits the existing JDBC-oriented `db.clj` design.
- It minimizes new infrastructure inside BiTool.
- It is sufficient for Bronze ingestion and control-plane persistence.

For larger scale, evolve to:

- stage raw JSON in object storage
- use Databricks Auto Loader / Lakeflow / Jobs for high-volume Bronze loading
- keep BiTool as the control and extraction plane

---

## 5. Graph and Node Design

## 5.1 Source and control nodes

The Sheetz implementation will use these logical nodes:

- `Sc` Scheduler
- `Ap` API Connection / API Extractor
- optional transform nodes: `P`, `Fi`, `Fu`, `A`
- `Tg` Target, extended for Databricks Bronze writes

The current microservice nodes such as `Au`, `Vd`, `Rl`, `Cr`, `Lg`, `Cq`, `Ci`, `Ev` are not the primary path for the batch ingestion MVP.

## 5.2 API node extension

The current `Ap` node stores spec and endpoint selection metadata. It must be extended to carry ingestion semantics.

### Proposed `Ap` data model

```clojure
{:name "samara-api"
 :btype "Ap"
 :source_system "samara"
 :api_name "samara-fleet"
 :specification_url "https://..."
 :base_url "https://api.samsara.com"
 :auth_ref {:type "bearer" :secret_ref "samara/api-token"}
 :endpoint_configs
 [{:endpoint_name "trips"
   :endpoint_url "/fleet/trips"
   :http_method "GET"
   :selected_nodes ["$.data[].id" "$.data[].vehicleId" "$.data[].startTime"]
   :load_type "incremental"
   :pagination_strategy "cursor"
   :cursor_field "endCursor"
   :watermark_column "startTime"
   :watermark_overlap_minutes 1440
   :primary_key_fields ["id"]
   :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"
   :silver_table_name "sheetz_telematics.silver.fact_trip"
   :retry_policy {:max_retries 5 :base_backoff_ms 1000}
   :json_explode_rules [{:path "$.data[]" :mode "records"}]
   :enabled true}]}
```

### Backend changes

- Extend `save-api` in `graph2.clj` to persist endpoint ingestion config, not just `endpoint_url`, `table_name`, and `selected_nodes`.
- Add `get-api-item` or extend the current item retrieval shape for full `Ap` configuration.
- Add validation for:
  - supported `load_type`
  - supported `pagination_strategy`
  - required `watermark_column` for incremental modes
  - required `primary_key_fields` for dedupe-safe ingestion

### Frontend changes

Update the API node editor to capture:

- source system
- base URL
- auth reference / secret reference
- per-endpoint load type
- pagination strategy
- checkpoint field
- overlap window
- primary keys
- Bronze and Silver target names
- retry settings
- enabled flag

The existing endpoint-selection UI can be retained and extended rather than replaced.

## 5.3 Databricks target design

Do not create a separate Databricks node for MVP unless the UI demands it. Extend `Tg` to support target kinds.

### Proposed `Tg` data model

```clojure
{:name "bronze-trips-target"
 :btype "Tg"
 :target_kind "databricks"
 :connection_id 42
 :catalog "sheetz_telematics"
 :schema "bronze"
 :table_name "samara_trips_raw"
 :write_mode "append"
 :create_table true
 :table_format "delta"
 :partition_columns ["partition_date"]
 :merge_keys []
 :cluster_by []
 :options {:raw_payload true}}
```

### MVP write modes

- `append` for Bronze
- `merge` reserved for checkpoint or future Silver support

### Backend changes

- Extend `db.clj` connection support with Databricks SQL Warehouse JDBC.
- Extend target write logic to understand fully-qualified `catalog.schema.table`.
- Add Databricks-aware DDL generation for Bronze tables.

### Why extend `Tg` instead of adding `Dbx`

- Existing graph semantics already treat `Tg` as the terminal write node.
- Bronze ingestion is still a target write.
- This avoids multiplying terminal node types before the runtime exists.

---

## 6. Databricks Connectivity Design

## 6.1 Connection type

Add a new connection `dbtype`:

- `databricks`

### Proposed connection fields

```clojure
{:connection_name "sheetz-dbx"
 :dbtype "databricks"
 :host "<server-hostname>"
 :port 443
 :http_path "/sql/1.0/warehouses/<warehouse-id>"
 :token "<pat-or-service-principal-token>"
 :catalog "sheetz_telematics"
 :schema "bronze"}
```

### Implementation note

BiTool currently builds DB specs in `create-dbspec-from-id`. Add a Databricks branch that returns a JDBC spec compatible with the Databricks SQL driver used by the deployment.

The exact driver class and connection string should be injected via configuration instead of hardcoding them in graph state.

## 6.2 DDL strategy

Bronze DDL should create Unity Catalog managed Delta tables with explicit columns.

Example target DDL shape:

```sql
CREATE TABLE IF NOT EXISTS sheetz_telematics.bronze.samara_trips_raw (
  ingestion_id STRING,
  run_id STRING,
  source_system STRING,
  endpoint_name STRING,
  extracted_at_utc TIMESTAMP,
  ingested_at_utc TIMESTAMP,
  api_request_url STRING,
  api_page_number INT,
  api_cursor STRING,
  http_status_code INT,
  record_hash STRING,
  source_record_id STRING,
  event_time_utc TIMESTAMP,
  partition_date DATE,
  load_date DATE,
  payload_json STRING
)
USING DELTA
PARTITIONED BY (partition_date)
```

## 6.3 Write strategy

For MVP:

- write one Bronze row per extracted record
- keep `payload_json` as the raw canonical representation
- map selected JSON fields into promoted Bronze columns only when stable and useful

Later:

- stage raw pages to object storage
- bulk load with Databricks-native tools

---

## 7. Minimal Custom Audit Schema

Custom `audit.*` tables exist only for pipeline state and endpoint-level observability that Unity Catalog does not provide.

## 7.1 Required tables

### `audit.ingestion_config`

Purpose:

- source-of-truth for endpoint extraction settings

Recommended columns:

- `source_system`
- `endpoint_name`
- `endpoint_path`
- `http_method`
- `load_type`
- `pagination_strategy`
- `cursor_field`
- `watermark_column`
- `watermark_overlap_minutes`
- `primary_key_fields`
- `bronze_table_name`
- `silver_table_name`
- `enabled_flag`
- `retry_policy_json`
- `json_explode_rules_json`
- `updated_at_utc`

### `audit.ingestion_checkpoint`

Purpose:

- track last successful watermark or cursor by endpoint

Recommended columns:

- `source_system`
- `endpoint_name`
- `last_successful_watermark`
- `last_attempted_watermark`
- `last_successful_cursor`
- `last_attempted_cursor`
- `last_successful_run_id`
- `last_status`
- `rows_ingested`
- `updated_at_utc`

### `audit.endpoint_run_detail`

Purpose:

- endpoint-level run history and operational status

Recommended columns:

- `run_id`
- `source_system`
- `endpoint_name`
- `started_at_utc`
- `finished_at_utc`
- `status`
- `http_status_code`
- `pages_fetched`
- `rows_extracted`
- `rows_written`
- `retry_count`
- `error_summary`

### `audit.data_quality_results`

Purpose:

- persist DQ checks and outcomes for Bronze/Silver/Gold

### `audit.bad_records`

Purpose:

- quarantine malformed or unparseable records for replay/debugging

## 7.2 Deferred tables

Add only when justified:

- `audit.schema_drift_log`
- `audit.pipeline_run`
- `audit.api_error_log`
- `audit.reconciliation_results`

---

## 8. Bronze Data Model

Bronze should be append-only and replayable.

## 8.1 Standard Bronze envelope

Every Bronze table should include:

- `ingestion_id`
- `run_id`
- `source_system`
- `endpoint_name`
- `extracted_at_utc`
- `ingested_at_utc`
- `api_request_url`
- `api_page_number`
- `api_cursor`
- `http_status_code`
- `record_hash`
- `source_record_id`
- `event_time_utc`
- `partition_date`
- `load_date`
- `payload_json`

## 8.2 Promoted fields

Promote a small number of stable fields into typed Bronze columns where useful:

- business keys such as `trip_id`, `vehicle_id`, `driver_id`
- event timestamps
- low-cardinality fields needed in early Silver logic

Avoid flattening the full payload in Bronze.

---

## 9. Silver and Gold Execution Boundary

## 9.1 MVP boundary

BiTool will own Bronze ingestion only.

Databricks jobs will own:

- Silver flattening and canonicalization
- dedupe and merge logic
- Gold aggregations

This keeps BiTool aligned with its current strength as ingestion/control layer while avoiding an oversized first implementation.

## 9.2 Trigger contract

After a successful Bronze endpoint run, BiTool should either:

- call a Databricks job endpoint for the relevant entity group, or
- insert a control row consumed by a Databricks job, or
- execute a lightweight orchestration SQL/procedure if the platform already uses one

The recommended MVP is direct Databricks Jobs API invocation from BiTool with entity-level parameters.

---

## 10. Runtime Components To Add

## 10.1 Batch graph executor

Add a batch execution namespace, for example:

- `src/clj/bitool/ingest/runtime.clj`

Responsibilities:

- read graph and identify `Sc -> Ap -> ... -> Tg` chain
- load endpoint configs from graph or `audit.ingestion_config`
- execute endpoints independently
- write audit rows
- update checkpoints
- dispatch Silver/Gold triggers

## 10.2 Checkpoint manager

Add:

- `src/clj/bitool/ingest/checkpoint.clj`

Responsibilities:

- load current checkpoint by endpoint
- compute next extraction window
- persist success or failure updates

## 10.3 Bronze writer

Add:

- `src/clj/bitool/ingest/bronze.clj`

Responsibilities:

- transform extracted items into Bronze envelope rows
- compute `record_hash`
- serialize `payload_json`
- insert rows into Databricks
- quarantine malformed records

## 10.4 Scheduler runtime

Add:

- `src/clj/bitool/ingest/scheduler.clj`

Responsibilities:

- evaluate scheduler definitions
- enqueue or directly invoke ingestion runs

If external orchestration already exists, this module can be kept thin and only expose a manual trigger plus cron metadata.

---

## 11. API Extraction Design

## 11.1 Extractor contract

Each endpoint execution should produce:

```clojure
{:run-id "..."
 :endpoint-name "trips"
 :stop-reason :eof
 :pages-fetched 42
 :items-extracted 12000
 :next-cursor nil
 :max-watermark "2026-03-13T09:00:00Z"
 :rows [{... bronze row ...}]}
```

## 11.2 Retry policy

Extend the current connector behavior to support:

- retry on `429`
- retry on transient `5xx`
- exponential backoff
- maximum retry cap
- endpoint isolation so one failing endpoint does not block others

## 11.3 Incremental window calculation

Rules:

- `full`: no checkpoint used
- `incremental`: start from last successful watermark minus overlap window
- `snapshot`: full pull, but checkpoint latest successful run

Overlap window should be configurable per endpoint.

---

## 12. Databricks Job Integration

## 12.1 Silver job trigger

Trigger payload should include:

- `source_system`
- `entity_group`
- `bronze_table`
- `run_id`
- `changed_partition_dates`

## 12.2 Gold job trigger

Gold should usually be triggered on:

- hourly cadence for operational marts
- daily cadence for executive marts

BiTool does not need to compute Gold SQL. It needs only to trigger the relevant Databricks jobs reliably.

---

## 13. Proposed Code Changes

## 13.1 `src/clj/bitool/graph2.clj`

- extend `Ap` node schema and persistence
- extend `Tg` node schema for Databricks targets
- add graph helpers for batch extraction config retrieval

## 13.2 `src/clj/bitool/db.clj`

- add Databricks connection support
- add Databricks DDL generation
- add fully-qualified table-name handling
- add optional merge helper for checkpoint tables

## 13.3 `src/clj/bitool/connector/api.clj`

- add retry loop for rate-limit and transient server errors
- expose endpoint execution result contract
- allow watermark and cursor injection from checkpoint manager

## 13.4 `src/clj/bitool/routes/home.clj`

- add endpoints for manual ingestion run
- add endpoints for loading and saving richer API ingestion config
- add endpoints for scheduler control if needed

## 13.5 New namespaces

- `bitool.ingest.runtime`
- `bitool.ingest.checkpoint`
- `bitool.ingest.bronze`
- `bitool.ingest.scheduler`
- optionally `bitool.databricks.jobs`

## 13.6 Frontend

Update:

- `resources/public/apiComponent.js`
- `resources/public/apiConnectionComponent.js`
- `resources/public/targetComponent.js`

Add fields required for:

- ingestion mode
- pagination mode
- checkpoint column
- overlap window
- Bronze/Silver names
- Databricks target fields

---

## 14. Delivery Plan

## Phase 1: Bronze MVP

- add Databricks connection type
- extend `Ap` config model
- extend `Tg` for Databricks Bronze
- add required `audit.*` tables
- implement batch executor for `Sc -> Ap -> Tg`
- support full and incremental loads

Success criteria:

- one Samara endpoint ingests into a Bronze Delta table
- checkpoint updates correctly
- endpoint run detail is queryable

## Phase 2: Multi-endpoint and hardening

- support endpoint fan-out from one API node
- add retry/backoff
- add bad-record quarantine
- support sliding-window incremental loads
- trigger Databricks Silver jobs

Success criteria:

- multiple Samara endpoints run independently
- failures are isolated and auditable

## Phase 3: Production readiness

- schema drift logging
- richer DQ recording
- Silver/Gold trigger routing by entity group
- scale tuning and optional object-storage staging

---

## 15. Risks and Tradeoffs

## 15.1 JDBC to Databricks for Bronze

Pros:

- fastest fit to current codebase
- smallest implementation surface

Cons:

- not ideal for very large raw payload volume
- may need later migration to staged bulk loading

Decision:

- use JDBC/SQL Warehouse for MVP Bronze
- evolve to staged file ingestion if volume or cost requires it

## 15.2 Extending `Tg` instead of creating a Databricks-specific node

Pros:

- keeps graph model simple
- reuses existing target semantics

Cons:

- target node becomes more polymorphic

Decision:

- extend `Tg` now
- split later only if target-specific behaviors become too divergent

## 15.3 Keeping Silver/Gold in Databricks

Pros:

- aligns with medallion responsibilities
- avoids overbuilding BiTool runtime

Cons:

- less visual modeling of downstream transforms in BiTool for MVP

Decision:

- keep Silver/Gold in Databricks for the first implementation

---

## 16. MVP Acceptance Criteria

- BiTool can store a Samara ingestion graph with scheduler, API extraction config, and Databricks target config.
- BiTool can connect to Databricks SQL Warehouse.
- A scheduled or manual run can ingest at least one Samara endpoint into `sheetz_telematics.bronze.*`.
- `audit.ingestion_checkpoint` is updated after success.
- `audit.endpoint_run_detail` records run status and counts.
- Failed records can be written to `audit.bad_records`.
- A successful Bronze load can trigger a Databricks Silver job.

---

## 17. Recommended First Implementation Slice

Implement this slice first:

1. Databricks connection support in `db.clj`
2. `Tg` Databricks append writer for Bronze
3. `Ap` endpoint config extension with watermarking fields
4. `audit.ingestion_checkpoint`
5. `audit.endpoint_run_detail`
6. manual trigger endpoint for one Samara entity such as `trips`

That slice proves the architecture with the least amount of new surface area.
