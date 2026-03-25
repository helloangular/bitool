# Intent-Based Bidirectional Medallion Pipeline Creation — Technical Design

**Status**: Draft
**Author**: System
**Date**: 2026-03-23
**Depends on**: GIL v1.0 (normalize/validate/compile/apply pipeline), graph2.clj node types, ingest/runtime.clj, modeling/automation.clj

---

## 1. Problem Statement

Today, creating a full API-to-Bronze-to-Silver-to-Gold pipeline in Bitool requires:

1. Manually creating an API source node and configuring each endpoint
2. Manually adding a Target node and connecting to a warehouse
3. Running Preview Schema and configuring watermark/explode rules
4. Manually creating Silver proposals and releases
5. Manually creating Gold proposals and releases

A user should be able to say:

> "Pull Samsara fleet/vehicles and fleet/drivers every hour into Bronze on Databricks, create cleaned vehicle and driver Silver tables, and build a daily fleet utilization Gold model."

And the system should generate the entire pipeline graph, configuration, and execution plan.

The design must support two equally important entry modes:

1. **Top-down / BRD-first**
   - the user starts from business intent or analytics requirements
   - example: "Build revenue analytics from Stripe"
2. **Bottom-up / source-first**
   - the user starts from an API, OpenAPI spec, or known source system
   - example: "Connect Samsara and suggest useful models"

The planner is therefore not only a natural-language-to-graph system. It is a **bidirectional semantic planner** that aligns:

- business demand from Gold and BRD intent,
- semantic contracts in Silver,
- source supply from Bronze and API/OpenAPI discovery.

Silver is the canonical alignment layer between the two directions.

---

## 2. Architecture Overview

```
User Intent (natural language)
  |
  v
[Stage 1] Intent Parser (LLM)
  |  Output: Structured PipelineIntent
  v
[Stage 2] Source Discovery + Semantic Planning (deterministic)
  |  Output: AvailableSourceCatalog + required semantic entities
  v
[Stage 3] Coverage Reconciliation Engine (deterministic)
  |  Output: coverage report + ingestion recommendations
  v
[Stage 4] Pipeline Planner (deterministic)
  |  Output: Canonical PipelineSpec
  v
[Stage 5] Graph Compiler (deterministic)
  |  Output: BronzeBuildPlan + modeled Silver/Gold mutation plan
  v
[Stage 6] Bronze Graph Validation
  |  Output: validated Bronze graph plan / extended Bronze GIL
  v
[Stage 7] Preview & Assumptions (user-facing)
  |  Output: human-readable plan + assumptions
  v
[Stage 8] Apply
  |  Output: persisted Bronze graph + Silver proposals + Gold proposals
  v
[Stage 9] Artifact Generator (new)
  |  Output: optional Databricks SDP SQL artifacts
  v
[Stage 10] Deploy (future)
```

Existing Bitool pieces already cover part of the path:

- Bronze apply/runtime in `bitool.gil.*`, `graph2.clj`, and `ingest/runtime.clj`
- Silver/Gold proposal lifecycle in `modeling/automation.clj`
- OpenAPI-assisted source understanding through the existing schema import and inference work

This design adds the missing intent, source-discovery, reconciliation, preview, and artifact-generation layers around those existing contracts.

### 2.1 Core Semantic Concept

The system should be understood as:

```text
        BRD / Intent
             ↓
           GOLD
             ↓
           SILVER   ← canonical semantic contract
             ↑
           BRONZE
             ↑
      API / OpenAPI / DB
```

Gold expresses business demand. Bronze expresses source supply. Silver is the stable semantic layer that reconciles the two.

### 2.2 Supported Planning Modes

| Mode | User Starts With | Planner Output |
|---|---|---|
| BRD-first | business intent, metrics, domain | Gold-first pipeline plan, then required Silver/Bronze |
| Source-first | connector, API, OpenAPI spec, tables | Bronze-first plan, then suggested Silver/Gold |
| Hybrid | both intent and source hints | reconciled pipeline with explicit coverage status |

---

## 3. Stage 1: Intent Parser

### 3.1 Intent Classes

The parser recognizes four intent classes, which may appear together in a single prompt:

| Class | Examples | Output Fields |
|---|---|---|
| **Ingestion** | "Pull Stripe charges every hour" | source_system, objects, cadence, auth_method, incremental_field |
| **Transformation** | "Clean and deduplicate payments" | entities, dedup_keys, normalization, joins |
| **Analytics** | "Build daily revenue and MRR" | metric_packages, grain, dimensions |
| **Operational** | "Run daily at 6am, retry 3 times" | schedule, retry_policy, alerts |

### 3.2 PipelineIntent Schema

```clojure
{:intent-type    :pipeline            ;; :pipeline | :ingestion-only | :transform-only | :analytics-only
 :source         {:system       "samsara"
                  :objects      ["fleet/vehicles" "fleet/drivers"]
                  :auth-method  "bearer"
                  :load-type    "incremental"
                  :cadence      "hourly"}
 :target         {:platform     "databricks"
                  :catalog      "main"
                  :bronze-schema "bronze"
                  :silver-schema "silver"
                  :gold-schema   "gold"}
 :bronze         [{:object       "fleet/vehicles"
                   :table        "samsara_fleet_vehicles_raw"
                   :watermark    "updatedAtTime"
                   :primary-key  "id"
                   :explode-path "data"}
                  {:object       "fleet/drivers"
                   :table        "samsara_fleet_drivers_raw"
                   :watermark    "updatedAtTime"
                   :primary-key  "id"
                   :explode-path "data"}]
 :silver         [{:entity       "vehicle"
                   :table        "dim_vehicle"
                   :source       "samsara_fleet_vehicles_raw"
                   :dedup-key    "id"
                   :columns      [{:source "id" :target "vehicle_id" :type "STRING"}
                                  {:source "name" :target "vehicle_name" :type "STRING"}
                                  {:source "make" :target "make" :type "STRING"}
                                  {:source "model" :target "model" :type "STRING"}
                                  {:source "year" :target "year" :type "INT"}
                                  {:source "vin" :target "vin" :type "STRING"}]}
                  {:entity       "driver"
                   :table        "dim_driver"
                   :source       "samsara_fleet_drivers_raw"
                   :dedup-key    "id"
                   :columns      [...]}]
 :gold           [{:model        "fleet_utilization_daily"
                   :grain        "day"
                   :depends-on   ["dim_vehicle" "dim_driver"]
                   :measures     ["active_vehicles" "total_miles" "utilization_pct"]
                   :dimensions   ["date" "vehicle_make" "region"]}]
 :ops            {:schedule      "0 * * * *"
                  :retries       3
                  :timeout-min   30
                  :alerts        ["failure" "drift"]}}
```

The intent schema should also support business-first inputs that may not initially name concrete Bronze objects:

```clojure
{:intent-type :pipeline
 :business    {:domain "revenue"
               :metrics ["gross_revenue" "mrr_monthly"]
               :grain "day"
               :dimensions ["customer" "plan" "region"]}
 :source      {:system "stripe"}
 :target      {:platform "databricks"}}
```

That form is resolved later through the metric registry, entity registry, and connector knowledge during planning.

### 3.3 LLM Prompt Design

The intent parser uses a single LLM call with constrained tool_use output:

```
System Prompt:
  You are a data pipeline architect. Convert natural language descriptions
  into structured PipelineIntent specs.

  ## Supported Source Systems
  {from connector registry: samsara, stripe, hubspot, salesforce, ...}

  ## Connector Knowledge
  {per-system: known endpoints, common entities, pagination patterns,
   typical watermark fields, auth methods}

  ## Canonical Business Ontology
  {shared entity types: customer, order, payment, vehicle, driver, ...}

  ## Metric Packages
  {known gold patterns: revenue_analytics, fleet_analytics, subscription_analytics, ...}

  ## Rules
  1. Always set load_type to "incremental" unless user says "full"
  2. Always identify watermark field from connector knowledge
  3. Default primary_key to "id" unless system uses different convention
  4. Default explode_path from connector knowledge (e.g. "data" for Samsara)
  5. Silver entities should be dimension or fact tables, not raw mirrors
  6. Gold should use known metric packages, not freeform
  7. If ambiguous, make assumptions and list them

  ## Few-Shot Examples
  {3-5 examples covering different source systems and pipeline shapes}

Tool Schema:
  {
    "name": "create_pipeline_intent",
    "input_schema": { ... PipelineIntent JSON Schema ... }
  }
```

### 3.4 Connector Knowledge Registry

Stored as EDN in `resources/connector-knowledge/`:

```clojure
;; resources/connector-knowledge/samsara.edn
{:system       "samsara"
 :display-name "Samsara"
 :auth-method  "bearer"
 :base-url     "https://api.samsara.com"
 :pagination   {:strategy "cursor"
                :cursor-field "pagination.endCursor"
                :cursor-param "after"
                :has-next-field "pagination.hasNextPage"}
 :endpoints    [{:path     "fleet/vehicles"
                 :method   "GET"
                 :entity   "vehicle"
                 :grain    "entity"
                 :explode  "data"
                 :key      "id"
                 :watermark "updatedAtTime"
                 :fields   [{:name "id" :type "STRING" :role "key"}
                            {:name "name" :type "STRING" :role "attribute"}
                            {:name "make" :type "STRING" :role "attribute"}
                            {:name "model" :type "STRING" :role "attribute"}
                            {:name "year" :type "INT" :role "attribute"}
                            {:name "vin" :type "STRING" :role "attribute"}
                            {:name "updatedAtTime" :type "TIMESTAMP" :role "watermark"}
                            {:name "createdAtTime" :type "TIMESTAMP" :role "timestamp"}]}
                {:path     "fleet/drivers"
                 :method   "GET"
                 :entity   "driver"
                 :grain    "entity"
                 :explode  "data"
                 :key      "id"
                 :watermark "updatedAtTime"
                 :fields   [...]}
                {:path     "fleet/vehicles/stats"
                 :method   "GET"
                 :entity   "vehicle_stat"
                 :grain    "event"
                 :explode  "data"
                 :key      "id"
                 :watermark "time"}]
 :metric-packages [{:name "fleet_analytics"
                    :entities ["vehicle" "driver" "vehicle_stat"]
                    :gold-models [{:table "fleet_utilization_daily"
                                   :grain "day"
                                   :measures ["active_count" "total_miles"]}
                                  {:table "driver_safety_daily"
                                   :grain "day"
                                   :measures ["events" "score"]}]}]}
```

### 3.5 Additional Registries

The bidirectional planner depends on three other deterministic registries:

#### Metric Registry

Maps business metrics/packages to required semantic entities.

```clojure
{:gross_revenue {:requires-entities ["payment"]
                 :grain "day"}
 :mrr_monthly   {:requires-entities ["subscription" "customer"]
                 :grain "month"}}
```

#### Entity Registry

Defines canonical Silver entities and their minimum semantic contract.

```clojure
{:payment {:keys ["payment_id"]
           :attributes ["amount" "currency" "status" "event_time"]}
 :vehicle {:keys ["vehicle_id"]
           :attributes ["vehicle_name" "make" "model" "updated_at"]}}
```

#### Transform Registry

Defines the allowed deterministic transform primitives used by the planner/compiler.

```clojure
{:field_map        {:primitive "projection"}
 :type_cast        {:primitive "projection"}
 :filter_rows      {:primitive "filter"}
 :deduplicate      {:primitive "aggregation"}
 :join_entities    {:primitive "join"}
 :aggregate_by_day {:primitive "aggregation"}}
```

---

## 4. Stage 2: Source Discovery + Semantic Planning

This stage is deterministic. It resolves:

1. what semantic entities the user needs,
2. what sources Bitool can already see,
3. whether coverage is ready, partial, or blocked.

### 4.1 Required Semantic Entities

For BRD-first prompts:

- metric packages determine the required Gold models
- Gold models determine required Silver entities
- Silver entities determine required Bronze source coverage

Examples:

- `revenue_analytics` -> `payment`, `refund`, `subscription`
- `fleet_analytics` -> `vehicle`, `driver`, `vehicle_stat`

### 4.2 Available Source Catalog

The planner should discover available source supply from:

- connector knowledge registry for known systems
- imported OpenAPI specs
- approved spec-derived field descriptors already stored on endpoints
- existing graph endpoint configs
- future DB/schema discovery adapters

The output is an `AvailableSourceCatalog`, not yet a full pipeline:

```clojure
{:system "samsara"
 :available-endpoints [{:path "fleet/vehicles"
                        :maps-to-entity "vehicle"
                        :key "id"
                        :watermark "updatedAtTime"}
                       {:path "fleet/drivers"
                        :maps-to-entity "driver"
                        :key "id"
                        :watermark "updatedAtTime"}]}
```

### 4.3 Coverage Reconciliation

This is the key new stage from the bidirectional design.

It compares:

- **required entities** from business/Gold intent
- **available entities** from source discovery

Example output:

```clojure
{:gold-model "revenue_daily"
 :status "partial"
 :required-entities ["payment" "refund"]
 :available-entities ["payment"]
 :missing ["refund"]
 :recommendation "Add Stripe refunds endpoint"}
```

Coverage states:

| State | Meaning |
|---|---|
| `ready` | all required entities available |
| `partial` | some entities available, some missing |
| `blocked` | core required entities unavailable |
| `suggested_ingestion` | system can suggest new Bronze ingestion to close the gap |

This stage does not mutate anything. It drives preview, assumptions, and endpoint recommendations.

---

## 5. Stage 3: Pipeline Planner

The planner is **deterministic** (no LLM). It takes a PipelineIntent and produces a PipelineSpec by:

1. Resolving connector knowledge for the source system
2. Matching requested objects and required entities to known endpoints
3. Selecting watermark/key/explode from connector knowledge
4. Mapping Silver entities to transform primitives
5. Instantiating Gold metric packages
6. Incorporating coverage results and missing-source recommendations
7. Computing table names using naming conventions
8. Building dependency graph

### 5.1 PipelineSpec Schema

```clojure
{:pipeline-id    "samsara_fleet_pipeline"
 :pipeline-name  "Samsara Fleet Pipeline"
 :target-platform "databricks"
 :catalog         "main"

 ;; Bronze layer — one API source node group + target
 :bronze-nodes
 [{:node-ref    "api1"
   :node-type   "api-connection"
   :config      {:api_name "samsara_fleet"
                 :source_system "samsara"
                 :base_url "https://api.samsara.com"
                 :auth_ref {:type "bearer" :secret_ref "SAMSARA_API_TOKEN"}
                 :endpoint_configs
                 [{:endpoint_name "fleet/vehicles"
                   :endpoint_url "fleet/vehicles"
                   :http_method "GET"
                   :load_type "incremental"
                   :pagination_strategy "cursor"
                   :cursor_field "pagination.endCursor"
                   :cursor_param "after"
                   :json_explode_rules [{:path "data"}]
                   :watermark_column "data_items_updatedAtTime"
                   :primary_key_fields ["id"]
                   :schema_mode "infer"
                   :inferred_fields [...]}
                  {:endpoint_name "fleet/drivers"
                   ...}]}}
  {:node-ref    "tg1"
   :node-type   "target"
   :config      {:target_kind "databricks"
                 :connection_binding {:mode "selected-connection"
                                      :connection_id 42}
                 :catalog "main"
                 :schema "bronze"
                 :write_mode "append"
                 :table_format "delta"
                 :partition_columns ["partition_date"]}}]

 :bronze-edges [["api1" "tg1"] ["tg1" "o1"]]

 ;; Silver layer — one proposal per entity
 :silver-proposals
 [{:target-model "dim_vehicle"
   :layer "silver"
   :source-bronze "samsara_fleet_vehicles_raw"
   :entity-kind "dimension"
   :business-keys ["vehicle_id"]
   :columns [{:source "data_items_id" :target "vehicle_id" :type "STRING"}
             {:source "data_items_name" :target "vehicle_name" :type "STRING"}
             {:source "data_items_make" :target "make" :type "STRING"}
             {:source "data_items_model" :target "model" :type "STRING"}
             {:source "data_items_year" :target "year" :type "INT"}
             {:source "data_items_vin" :target "vin" :type "STRING"}]
   :processing-policy {:ordering-strategy "latest_event_time_wins"
                       :event-time-column "updated_at"
                       :late-data-mode "merge"}}]

 ;; Gold layer — one model per metric
 :gold-models
 [{:target-model "fleet_utilization_daily"
   :layer "gold"
   :grain "day"
   :depends-on ["dim_vehicle"]
   :sql-template "SELECT
                    CAST(v.updated_at AS DATE) AS report_date,
                    v.make,
                    COUNT(DISTINCT v.vehicle_id) AS active_vehicles
                  FROM silver.dim_vehicle v
                  GROUP BY 1, 2"}]

 ;; Assumptions made during planning
 :assumptions
 ["Revenue calculated from successful charges only"
  "Watermark field 'updatedAtTime' used for incremental load"
  "Primary key is 'id' for all endpoints"
  "Bronze tables partitioned by partition_date"
  "Silver uses latest-event-time-wins dedup strategy"]

 ;; Operational config
 :ops {:schedule "0 * * * *"
       :retries 3}}
```

The PipelineSpec should also preserve reconciliation output so preview and apply stay honest:

```clojure
:coverage {:status "ready"
           :required-entities ["vehicle" "driver"]
           :available-entities ["vehicle" "driver"]
           :missing []
           :recommendations []}
```

### 5.2 Naming Conventions

| Layer | Pattern | Example |
|---|---|---|
| Bronze table | `{catalog}.bronze.{source}_{object}_raw` | `main.bronze.samsara_fleet_vehicles_raw` |
| Silver table | `{catalog}.silver.dim_{entity}` or `fct_{entity}` | `main.silver.dim_vehicle` |
| Gold table | `{catalog}.gold.{model}` | `main.gold.fleet_utilization_daily` |
| Graph name | `{source}_{pipeline_name}` | `samsara_fleet_pipeline` |

### 5.3 Transform Primitives

The planner selects from a constrained set of transform primitives:

| Primitive | GIL Node Type | Silver/Gold Use |
|---|---|---|
| `raw_api_extract` | Ap (api-connection) | Bronze ingestion |
| `json_flatten` | (implicit in inferred_fields) | Bronze promoted columns |
| `field_map` | P (projection) | Silver column rename/select |
| `type_cast` | P (projection) | Silver type standardization |
| `filter_rows` | Fi (filter) | Silver data quality filters |
| `deduplicate` | A (aggregation) | Silver dedup by business key |
| `join_entities` | J (join) | Silver/Gold cross-entity joins |
| `aggregate_by_grain` | A (aggregation) | Gold metric computation |
| `date_bucket` | Fu (function) | Gold time grain |

---

## 6. Stage 4: Graph Compiler

Converts PipelineSpec into:

1. a Bronze graph build plan, and
2. deterministic Silver/Gold proposal mutations layered on top of existing modeling automation.

This stage must respect current Bitool contracts rather than inventing new direct proposal APIs.

### 6.1 Bronze Graph Generation

Bronze generation has one explicit prerequisite:

- extend `bitool.gil.schema` so `api-connection` accepts the same config keys already supported by `graph2.clj`
- extend `bitool.gil.schema` so `target` accepts the full current target contract (`target_kind`, `connection_id`, `catalog`, `schema`, `table_format`, partitioning, Databricks job ids, etc.)

Until that extension is landed, the compiler should emit a BronzeBuildPlan that is structurally equivalent to GIL and apply it through a thin adapter that uses the same deterministic create/save sequence as `bitool.gil.compiler/apply-gil`.

For each Bronze endpoint group, generate:

```clojure
{:intent :build
 :graph-name "samsara_fleet_pipeline"
 :nodes [{:node-ref "api1"
          :type "api-connection"
          :config {... full Ap config from PipelineSpec ...}}
         {:node-ref "tg1"
          :type "target"
          :config {... full Tg config from PipelineSpec ...}}
         {:node-ref "o1"
          :type "output"}]
 :edges [["api1" "tg1"] ["tg1" "o1"]]}
```

Connection binding is not deferred implicitly at apply time. By the time the user presses `Apply`, the preview must already contain a concrete connection selection. The compiler therefore requires:

- `connection_binding.mode = "selected-connection"`
- `connection_binding.connection_id = <existing saved connection>`

That concrete `connection_id` is what gets persisted into the Target node config.

### 6.2 Silver Proposal Generation

Silver generation is a two-step flow because the current automation API derives a default proposal from Bronze metadata and endpoint schema before proposal edits are applied.

For each Silver entity:

1. call `modeling-automation/propose-silver-schema!` with the actual Bronze graph, source node, and endpoint
2. take the returned default proposal
3. apply deterministic planner edits through `update-silver-proposal!`
4. optionally compile immediately for preview confidence, but leave review/publish to the user

Example:

```clojure
;; Step 1: create baseline proposal from actual Bronze graph state
(def silver-base
  (modeling-automation/propose-silver-schema!
    {:graph-id bronze-graph-id
     :api-node-id api-node-id
     :endpoint-name "fleet/vehicles"
     :created-by created-by}))

;; Step 2: replace the returned proposal JSON with the deterministic
;; planner-shaped proposal JSON
(modeling-automation/update-silver-proposal!
  (:proposal_id silver-base)
  {:proposal planned-silver-proposal-json
   :created_by created-by})
```

The planner does not bypass the default proposal builder. It edits the returned proposal into the intended modeled shape.

### 6.3 Gold Proposal Generation

Gold generation follows the same rule. The current API creates Gold proposals from a Silver proposal id, not from freeform model inputs.

For each Gold model:

1. identify the Silver proposal it depends on
2. call `modeling-automation/propose-gold-schema!` with that `silver_proposal_id`
3. update the returned Gold proposal through `update-gold-proposal!`

Example:

```clojure
(def gold-base
  (modeling-automation/propose-gold-schema!
    {:silver_proposal_id silver-proposal-id
     :created_by created-by}))

(modeling-automation/update-gold-proposal!
  (:proposal_id gold-base)
  {:proposal planned-gold-proposal-json
   :created_by created-by})
```

### 6.4 SDP Artifact Generation (Stage 9)

SDP generation is an optional export artifact for Databricks-native deployment. It is not the primary Bitool runtime in MVP.

Current runtime model:

- Bronze runs through `ingest/runtime.clj`
- if the Bronze target is Databricks, Bronze writes directly to Databricks tables over the configured warehouse connection
- Silver and Gold continue to use existing proposal/release execution
- SDP is generated as an exportable artifact for future Databricks-native deployment, not as the default execution path

For Silver and Gold models targeted to Databricks, generate SDP SQL from the finalized PipelineSpec:

**Silver SDP Example:**
```sql
CREATE OR REFRESH STREAMING TABLE silver.dim_vehicle AS
SELECT
  data_items_id AS vehicle_id,
  data_items_name AS vehicle_name,
  data_items_make AS make,
  data_items_model AS model,
  data_items_year AS year,
  data_items_vin AS vin,
  data_items_updatedAtTime AS updated_at,
  ingested_at_utc,
  run_id
FROM STREAM(bronze.samsara_fleet_vehicles_raw)
WHERE data_items_id IS NOT NULL;
```

**Gold SDP Example:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.fleet_utilization_daily AS
SELECT
  CAST(updated_at AS DATE) AS report_date,
  make,
  COUNT(DISTINCT vehicle_id) AS active_vehicles,
  COUNT(*) AS total_records
FROM silver.dim_vehicle
GROUP BY 1, 2;
```

---

## 7. Stage 5: Preview & Assumptions

Before applying, the user sees a preview:

```
Pipeline: Samsara Fleet Pipeline
Target: Databricks connection #42 (main catalog)

Bronze Layer:
  - samsara / fleet/vehicles -> main.bronze.samsara_fleet_vehicles_raw
    10 fields, watermark: updatedAtTime, key: id
  - samsara / fleet/drivers -> main.bronze.samsara_fleet_drivers_raw
    8 fields, watermark: updatedAtTime, key: id

Silver Layer:
  - dim_vehicle (dimension, 6 columns from samsara_fleet_vehicles_raw)
    Dedup: latest by updatedAtTime, key: vehicle_id
  - dim_driver (dimension, 5 columns from samsara_fleet_drivers_raw)
    Dedup: latest by updatedAtTime, key: driver_id

Gold Layer:
  - fleet_utilization_daily (daily grain)
    Measures: active_vehicles, total_records
    Dimensions: report_date, make

Schedule: Hourly (0 * * * *)
Retries: 3

Assumptions:
  -> Watermark field 'updatedAtTime' used for incremental load
  -> Primary key is 'id' for all endpoints
  -> Bronze tables partitioned by partition_date
  -> Silver uses latest-event-time-wins dedup strategy
  -> Gold aggregation is daily by default
  -> Bronze target connection is Databricks connection #42

Coverage:
  -> Status: ready
  -> Required entities: vehicle, driver
  -> Available entities: vehicle, driver
  -> Missing entities: none

[Apply] [Edit] [Cancel]
```

If coverage is partial or blocked, preview must say so explicitly before apply:

- which required Silver entities are missing
- which Bronze endpoints could close the gap
- whether apply can continue as Bronze-only, Bronze+Silver, or full Bronze+Silver+Gold

---

## 8. API Routes

| Method | Path | Description |
|---|---|---|
| POST | `/pipeline/from-nl` | NL text -> PipelineIntent -> PipelineSpec -> Preview |
| POST | `/pipeline/preview` | PipelineSpec -> Preview (no LLM, deterministic) |
| POST | `/pipeline/apply` | PipelineSpec -> Create graph + proposals + releases |
| GET | `/pipeline/connectors` | List known connector systems |
| GET | `/pipeline/connectors/:system` | Connector knowledge for a system |
| GET | `/pipeline/metric-packages` | List known metric packages |
| GET | `/pipeline/entities` | List canonical semantic entities |
| POST | `/pipeline/generate-sdp` | PipelineSpec -> SDP SQL artifacts |

---

## 9. File Structure

```
src/clj/bitool/pipeline/
  intent.clj          ;; Stage 1: NL -> PipelineIntent (LLM)
  discovery.clj       ;; Stage 2: source discovery + OpenAPI/entity matching
  reconcile.clj       ;; Stage 3: coverage engine
  planner.clj         ;; Stage 4: PipelineIntent -> PipelineSpec (deterministic)
  compiler.clj        ;; Stage 5: PipelineSpec -> GIL + proposals (deterministic)
  preview.clj         ;; Stage 7: PipelineSpec -> human-readable plan
  sdp.clj             ;; Stage 9: PipelineSpec -> Databricks SDP SQL
  routes.clj          ;; API route handlers

resources/connector-knowledge/
  samsara.edn
  stripe.edn
  hubspot.edn
  salesforce.edn

resources/metric-packages/
  revenue_analytics.edn
  fleet_analytics.edn
  subscription_analytics.edn
  support_operations.edn

resources/entity-registry/
  canonical_entities.edn
```

---

## 10. Constrained Generation Rules

The LLM is a **proposal engine**, not the source of truth. All generation is constrained:

### What the LLM decides:
- Which API objects to pull
- Which endpoints matter
- Business entity names for Silver
- Which metric package to use for Gold
- Assumptions when intent is ambiguous

### What the system decides (deterministic):
- Table naming conventions
- Canonical entity requirements
- Coverage status and missing-source recommendations
- Watermark/key/explode from connector knowledge
- Pagination implementation
- Bronze column structure
- Retry behavior
- Partitioning strategy
- Whether to emit optional SDP artifacts for Databricks export

### What the user approves:
- Assumptions list
- Silver column mappings
- Gold metric definitions
- Schedule and alerting

---

## 11. UX Modes

### 11.1 BRD-first

User:

```text
Build revenue analytics
```

System:

- identifies Gold package candidates
- derives required Silver entities
- suggests Bronze endpoints needed to support them

### 11.2 Source-first

User:

```text
Connect Stripe
```

System:

- builds Bronze plan from connector/OpenAPI knowledge
- suggests Silver canonical entities that can be formed
- suggests Gold models that are now possible

### 11.3 Hybrid

User:

```text
Connect Stripe and build revenue analytics
```

System:

- performs both flows
- reconciles required vs available entities
- shows missing coverage if any

---

## 12. Ambiguity Resolution

When the user says something vague, the system resolves with defaults and lists assumptions:

| Vague Intent | Resolution | Assumption Listed |
|---|---|---|
| "Build fleet analytics" | fleet_analytics metric package | "Using fleet_analytics package: utilization + safety" |
| "Ingest Samsara" | Top 5 endpoints from connector knowledge | "Selected: vehicles, drivers, stats, locations, trips" |
| "Create vehicle model" | dim_vehicle with standard fields | "Dimension table with latest-event-time dedup" |
| "Daily revenue" | revenue_analytics at day grain | "Revenue = successful charges, daily grain" |
| "Run hourly" | Cron: `0 * * * *` | "Schedule: top of every hour UTC" |

---

## 13. Relationship to Existing GIL

This design **extends** GIL, it does not replace it:

| Concern | GIL v1.0 (existing) | Pipeline Intent (new) |
|---|---|---|
| Scope | Single graph (API endpoints, transforms) | Full medallion (Bronze + Silver + Gold) |
| Input | Structured GIL JSON | Natural language |
| LLM role | Generates GIL nodes/edges | Generates PipelineIntent |
| Compilation | GIL -> graph nodes | PipelineSpec -> Bronze graph plan + modeled proposal mutations |
| Apply | Creates nodes in one graph | Creates Bronze graph, then creates/updates Silver and Gold proposals |
| SDP | Not applicable | Optional export artifact for Databricks-native deployment |

The pipeline compiler uses:

- Bronze: extended GIL apply, or a thin Bronze apply adapter with identical deterministic semantics until the GIL schema extension lands
- Silver: `propose-silver-schema!` followed by `update-silver-proposal!`
- Gold: `propose-gold-schema!` followed by `update-gold-proposal!`

---

## 14. MVP Scope

### Phase 1: Samsara vertical (4-6 weeks)
- Connector knowledge for Samsara (10 endpoints)
- fleet_analytics metric package
- Intent parser with Samsara few-shot examples
- Deterministic planner + graph compiler
- GIL schema extension for `api-connection` and `target`
- Preview UI
- Bronze graph generation + apply
- Silver proposal generation (manual review)

### Phase 2: Multi-source (4-6 weeks)
- Connector knowledge for Stripe, HubSpot, Salesforce
- revenue_analytics, subscription_analytics metric packages
- Gold proposal generation
- SDP artifact generation
- Deploy to Databricks

### Phase 3: Self-learning (future)
- Learn connector knowledge from OpenAPI specs
- Learn metric packages from user-created Gold models
- Automatic assumption refinement from user feedback

---

## 15. Key Design Decisions

1. **LLM parses intent, does not generate SQL** — reliability over flexibility
2. **Connector knowledge is pre-built, not hallucinated** — correct pagination, watermarks, keys
3. **Silver is the canonical semantic contract** — Gold demand and Bronze supply align through stable Silver entities
4. **Coverage reconciliation is first-class** — the system must explicitly say ready/partial/blocked before apply
5. **Silver/Gold use existing modeling automation** — baseline proposals are generated first, then deterministically updated
6. **SDP is generated, not hand-written** — deterministic from PipelineSpec
7. **Bronze runtime remains existing ingest/runtime** — Databricks targets are written directly over the configured warehouse connection
8. **Preview before apply** — user sees assumptions, coverage, connection binding, and table names before persistence
9. **Selective Bronze promotion** — promote key operational fields, not unconstrained source mirrors
