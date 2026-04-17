# Semantic Layer Tech Design

> Bitool already computes most of a semantic layer during Silver/Gold proposal automation. This doc covers surfacing what exists, closing gaps vs SAP Datasphere / Looker LookML, and building a first-class Semantic Model object.
>
> **Last updated**: 2026-04-10

---

## 1. Context & Motivation

SAP Datasphere, Looker LookML, and dbt Semantic Layer all provide a **named, browsable, governed business layer** between raw tables and reports. Bitool computes the same metadata — entity kinds, column roles, grain, measures, dimensions, business descriptions, join relationships — but buries it inside proposal JSON and schema_context rows. The reporting layer (ISL) stitches it together at query time but users never see it.

**Result**: Users can't browse their semantic layer, can't define calculated measures, can't set drill hierarchies. The intelligence is there but invisible.

**Goal**: Surface a first-class Semantic Model that makes Bitool's inferred intelligence browsable, editable, and queryable — without requiring users to learn YAML or SQL.

---

## 2. What We Already Have (Code Inventory)

### 2.1 Entity Classification

**File**: `modeling/automation.clj` — `infer-entity-kind` (line ~969)

```clojure
(cond
  (and has-measure? has-time?) "fact"
  (seq key-columns)            "dimension"
  :else                        "entity")
```

Also supports: `reference`, `snapshot`, `mart` in Gold context.

### 2.2 Column Roles

**File**: `modeling/automation.clj` — `field-role` (line ~961)

```clojure
(cond
  (contains? key-columns col) "business_key"
  (timestamp-column? field)   "timestamp"
  (#{"INT" "BIGINT" "DOUBLE"} type) "measure_candidate"
  :else                       "attribute")
```

Gold adds: `"dimension"`, `"time_dimension"`, `"measure"` with expressions like `SUM(...)`, `COUNT(*)`.

### 2.3 Grain

Silver proposals carry `{:grain {:keys ["customer_id" "order_id"]}}`. Gold proposals carry group-by columns (dimension keys + event_date). Semantic grain labels like `"vehicle_day"` computed at execution time.

### 2.4 Measures (Base + Aggregated)

Gold `build-gold-proposal` (line ~1237) generates:
- `SUM(column)` measures from `aggregatable-measure?` columns
- `COUNT(*)` fallback when no numeric measures found
- Each measure has: `target_column`, `type`, `expression`, `confidence`, `rule_source`

**Missing**: Calculated measures (query-time formulas), restricted measures (filtered aggregates).

### 2.5 Business Descriptions

**Table**: `schema_context` — per-table and per-column descriptions, sample values, source provenance.

**Populated via**: Auto-train (LLM), fingerprint (curated), manual entry.

**Consumed by**: `build-isl-prompt` injects as `BUSINESS CONTEXT` section into ISL prompt. The LLM reads these when translating NL → query.

### 2.6 Join Relationships

**File**: `db.clj` — `discover-joins` (line ~665)

Queries `information_schema` foreign keys. Returns `{:from_table, :from_column, :to_table, :to_column}`. Cached 10 minutes.

**Coverage by warehouse**:

| Warehouse | FK Introspection | Notes |
|---|---|---|
| PostgreSQL | Full | `information_schema.table_constraints` + `key_column_usage` |
| SQL Server | Full | Same `information_schema` path |
| MySQL | Full | `information_schema.key_column_usage` + `referenced_table_name` |
| Snowflake | Returns `[]` | No portable FK metadata query |
| Databricks | Returns `[]` | No FK metadata support |
| BigQuery | Returns `[]` | No FK metadata support |

**Impact on auto-generation**: On Snowflake/Databricks/BigQuery, `propose-semantic-model!` will produce entities with no auto-discovered relationships. Relationships must be added manually or inferred from naming conventions (future work). This is a material gap for Phase 1 — auto-generated models on these warehouses will have entities but no joins until the user adds them.

**Consumed by**: `build-isl-prompt` injects as join docs. ISL validation (`validate-join-spec`) checks join references.

### 2.7 Confidence Scoring

Every column and proposal carries a confidence score (0–1). Business key matches: 0.95, schema passthrough: 0.85, aggregated measures: 0.88, count fallback: 0.82. Unique to Bitool — neither Datasphere nor Looker have this.

### 2.8 Lineage

`source_paths`, `source_columns` tracked end-to-end from Bronze through Silver to Gold. `control_plane.clj` provides `lineage-view` for graph-level dependency visualization.

---

## 3. Gap Analysis vs SAP Datasphere

| Datasphere Concept | Bitool Status | Gap |
|---|---|---|
| **Analytic Model** (named consumption artifact) | Not present | ISL stitches registry + joins + context at ask-time; no persistent named model |
| **Fact / Dimension classification** | Done | `infer-entity-kind` → fact / dimension / entity |
| **Measures** (base) | Done | Gold `SUM(...)`, `COUNT(*)` |
| **Calculated measures** (query-time formulas) | Missing | No live formula layer |
| **Restricted measures** (filtered aggregates) | Missing | No "Revenue WHERE region = EMEA" |
| **Associations** (lazy joins) | Partial | FK discovery exists; joins resolved at compile-time not query-time |
| **Hierarchies** (level-based, parent-child) | Missing | timestamp→date derivation only |
| **Business glossary** | Done | `schema_context` — but not browsable as glossary UI |
| **Column roles** | Done | business_key, timestamp, measure_candidate, attribute, dimension, time_dimension, measure |
| **Grain** | Done | Keys + temporal resolution |
| **Confidence scoring** | Done (unique) | Datasphere doesn't have this |
| **Lineage** | Done | source_paths end-to-end |
| **Perspectives** (audience-scoped views) | Missing | No scoped model subsets |
| **Row-level security** | Missing | No RLS |
| **Transport / promotion** (dev→prod) | Partial | Graph versioning + workspaces, no formal promotion |

---

## 4. Design: The Semantic Model

### 4.1 Core Object

A **Semantic Model** is a named, versioned, first-class object that bundles everything ISL currently assembles on-the-fly.

```clojure
{:model_id       "sm-12345"
 :name           "Fleet Operations"
 :version        3
 :conn_id        42
 :schema         "analytics"
 :status         "published"           ;; draft | published | archived
 :created_by     "user@company.com"
 :created_at     "2026-04-10T..."

 ;; --- Entities (facts + dimensions) ---
 :entities
 {"trips"
  {:kind        "fact"
   :table       "gold_trips"
   :grain       {:keys ["trip_id"] :temporal "event_date"}
   :description "One row per completed trip"
   :columns     [{:name "trip_id"    :role "dimension" :type "BIGINT"  :description "Unique trip identifier"}
                 {:name "driver_id"  :role "dimension" :type "BIGINT"  :description "FK to drivers"}
                 {:name "event_date" :role "time_dimension" :type "DATE" :description "Trip completion date"}
                 {:name "miles"      :role "measure"   :type "DOUBLE" :description "Distance driven"}
                 {:name "fuel_cost"  :role "measure"   :type "DOUBLE" :description "Fuel expense USD"}]}

  "drivers"
  {:kind        "dimension"
   :table       "silver_drivers"
   :grain       {:keys ["driver_id"]}
   :description "One row per active driver"
   :columns     [{:name "driver_id"   :role "business_key" :type "BIGINT"  :description "PK"}
                 {:name "driver_name" :role "attribute"     :type "VARCHAR" :description "Full name"}
                 {:name "region"      :role "attribute"     :type "VARCHAR" :description "Operating region"}
                 {:name "hire_date"   :role "timestamp"     :type "DATE"    :description "Start date"}]}}

 ;; --- Relationships (lazy associations) ---
 :relationships
 [{:from "trips" :from_column "driver_id"
   :to   "drivers" :to_column "driver_id"
   :type "many_to_one"
   :join "LEFT"}]

 ;; --- Calculated Measures ---
 :calculated_measures
 [{:name        "cost_per_mile"
   :expression  "fuel_cost / NULLIF(miles, 0)"
   :type        "DOUBLE"
   :entity      "trips"
   :description "Fuel efficiency metric"}
  {:name        "total_trips"
   :expression  "COUNT(*)"
   :type        "BIGINT"
   :entity      "trips"
   :description "Trip count"}]

 ;; --- Restricted Measures ---
 ;; CONSTRAINT: Restricted measures may only filter on columns that belong
 ;; to the SAME entity as the base measure, or on columns reachable via
 ;; exactly ONE declared relationship from that entity. Cross-entity filters
 ;; must specify the join path explicitly via :via_relationship so the ISL
 ;; compiler can deterministically resolve which JOIN to add.
 :restricted_measures
 [{:name             "emea_fuel_cost"
   :base_measure     "fuel_cost"
   :entity           "trips"
   :filter           {:column "region" :op "=" :value "EMEA"}
   :via_relationship "trips->drivers"     ;; required when filter column is on a different entity
   :description      "Fuel cost for EMEA region only"}]

 ;; --- Hierarchies ---
 :hierarchies
 [{:name    "geography"
   :entity  "drivers"
   :levels  ["region" "driver_name"]
   :type    "level"}
  {:name    "time"
   :entity  "trips"
   :levels  ["event_date"]
   :type    "level"
   :granularities ["year" "quarter" "month" "week" "day"]}]

 ;; --- Governance ---
 :confidence    0.91
 :source        "auto"                  ;; auto | manual | hybrid
 :lineage       {:bronze ["raw_trips" "raw_drivers"]
                 :silver ["silver_trips" "silver_drivers"]
                 :gold   ["gold_trips"]}}
```

### 4.2 Storage

New PostgreSQL table:

```sql
CREATE TABLE semantic_model (
  model_id       TEXT PRIMARY KEY DEFAULT 'sm-' || gen_random_uuid()::text,
  conn_id        INTEGER NOT NULL REFERENCES connection(id),
  schema_name    TEXT NOT NULL,
  name           TEXT NOT NULL,
  version        INTEGER NOT NULL DEFAULT 1,
  status         TEXT NOT NULL DEFAULT 'draft',   -- draft | published | archived
  model_json     JSONB NOT NULL,                  -- full model object above
  created_by     TEXT NOT NULL DEFAULT 'system',
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (conn_id, schema_name, name)
);

CREATE TABLE semantic_model_version (
  version_id     BIGSERIAL PRIMARY KEY,
  model_id       TEXT NOT NULL REFERENCES semantic_model(model_id),
  version        INTEGER NOT NULL,
  model_json     JSONB NOT NULL,
  change_summary TEXT,
  created_by     TEXT NOT NULL DEFAULT 'system',
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (model_id, version)
);
```

### 4.3 Auto-Generation from Existing Data

The key insight: **we already compute 80% of this during Silver/Gold proposal automation.** Auto-generation assembles the model from existing artifacts:

```
Inputs:
  1. Silver proposals → entities (fact/dimension), columns with roles, grain, business keys
  2. Gold proposals   → aggregated measures, time dimensions, group-by columns
  3. schema_context   → business descriptions (table + column level)
  4. discover-joins   → FK relationships → associations
  5. fingerprints     → curated descriptions for known SaaS schemas

Assembly:
  propose-semantic-model!(conn-id, schema)
    → fetch all Silver + Gold proposals for this connection
    → fetch schema_context descriptions
    → fetch FK joins
    → merge into Semantic Model object
    → set source = "auto", confidence from proposal scores
    → persist as draft
```

**File**: New `semantic/model.clj`

```clojure
(defn propose-semantic-model!
  "Auto-generate a Semantic Model from existing Silver/Gold proposals + schema context + joins."
  [{:keys [conn-id schema created-by]}]
  (let [silver-proposals  (list-proposals {:conn-id conn-id :layer "silver" :status "approved"})
        gold-proposals    (list-proposals {:conn-id conn-id :layer "gold"})
        schema-ctx        (db/get-schema-context conn-id :schema schema)
        joins             (db/discover-joins conn-id :schema schema)
        entities          (build-entities-from-proposals silver-proposals gold-proposals schema-ctx)
        relationships     (build-relationships-from-joins joins entities)
        calc-measures     (infer-calculated-measures gold-proposals)
        hierarchies       (infer-hierarchies entities)
        model             {:name (str schema "_model")
                           :conn_id conn-id
                           :schema schema
                           :status "draft"
                           :source "auto"
                           :entities entities
                           :relationships relationships
                           :calculated_measures calc-measures
                           :restricted_measures []
                           :hierarchies hierarchies
                           :confidence (avg-confidence silver-proposals gold-proposals)}]
    (persist-semantic-model! model created-by)))
```

---

## 5. ISL Integration — Semantic Model as Query Source

### 5.1 Current Flow (registry-based)

```
build-isl-prompt
  → registry (raw table→columns from warehouse introspection)
  → joins (FK discovery)
  → schema-context (business descriptions)
  → stitched into prompt text
```

### 5.2 New Flow (model-based)

```
build-isl-prompt-from-model
  → Semantic Model (entities + relationships + measures + hierarchies + descriptions)
  → richer prompt:
      - entity kind hints ("trips is a fact table, drivers is a dimension")
      - calculated measures available ("cost_per_mile = fuel_cost / miles")
      - restricted measures available ("emea_fuel_cost = fuel_cost WHERE region = EMEA")
      - hierarchy hints ("geography: region → driver_name")
      - join semantics ("trips.driver_id → drivers.driver_id, many-to-one LEFT JOIN")
      - grain hints ("trips grain: one row per trip_id per event_date")
```

The ISL tool schema gains new capabilities:

```clojure
;; Existing: table enum-locked, columns validated server-side
;; New additions to tool schema:
"calculated_measure"
{:type "string"
 :enum ["cost_per_mile" "total_trips"]  ;; enum-locked from model
 :description "Pre-defined calculated measures"}

"drill_hierarchy"
{:type "string"
 :enum ["geography" "time"]
 :description "Hierarchy to drill into"}
```

### 5.3 Lazy Associations

Relationships in the model are **lazy** — they're defined once and only resolved when the ISL query references a column from the associated entity. This matches Datasphere's association pattern.

#### Join-Path Resolution Rules

The resolver must handle ambiguity deterministically. Rules, applied in order:

1. **Direct relationship**: If there is exactly one relationship connecting the base entity to the referenced entity, use it. This is the common case (fact→dimension).
2. **Shortest path**: If multiple paths exist (e.g. `trips→drivers` and `trips→vehicles→drivers`), choose the shortest (fewest hops).
3. **Explicit preference**: If multiple paths of equal length exist, the relationship with `:preferred true` wins. If no preference is set, reject as ambiguous and return a validation error listing the candidates.
4. **Cycle prevention**: A resolved entity may not appear twice in the join chain. If resolving entity B requires entity C which requires entity B, reject with a cycle error.
5. **Chained joins**: If entity C is only reachable via entity B (e.g. `trips→drivers→regions`), both joins are added in dependency order. The resolver builds a BFS tree from the base entity over declared relationships.

```clojure
(defn resolve-lazy-joins
  "Given an ISL document and a Semantic Model, add JOINs only for entities
   whose columns are actually referenced. Uses BFS shortest-path from the
   base entity over declared relationships. Rejects ambiguous equal-length
   paths unless one relationship is marked :preferred."
  [isl-doc model]
  (let [referenced-entities (extract-referenced-entities isl-doc model)
        base-entity         (get-in isl-doc ["table"])
        relationship-graph  (build-relationship-graph (:relationships model))
        join-plan           (bfs-join-paths base-entity referenced-entities
                                           relationship-graph)]
    (when-let [ambiguous (:ambiguous join-plan)]
      (throw (ex-info "Ambiguous join paths — mark one relationship as :preferred or remove duplicates"
                      {:base base-entity :ambiguous ambiguous})))
    (when-let [cycles (:cycles join-plan)]
      (throw (ex-info "Cyclic join path detected"
                      {:base base-entity :cycles cycles})))
    (assoc isl-doc "joins"
           (mapv relationship->join-spec (:resolved-joins join-plan)))))
```

**Validation contract**: `validate-semantic-model` checks at model-save time that no two relationships between the same entity pair exist unless one is marked `:preferred`. This prevents ambiguity from reaching query time.

---

## 6. UI: Semantic Layer Browser

### 6.1 New Screen: Model Explorer

Add to `modelingConsole.js` (or new `semanticLayerComponent.js`):

```
+------------------------------------------+
|  Semantic Model: Fleet Operations  v3    |
|  Status: Published  Confidence: 91%      |
+------------------------------------------+
|                                          |
|  ENTITIES           RELATIONSHIPS        |
|  +--------+         trips.driver_id      |
|  | trips  |-------->| drivers            |
|  | (fact) |  N:1    | (dimension)        |
|  +--------+         +-------------------+|
|                                          |
|  MEASURES                                |
|  Base: miles, fuel_cost                  |
|  Calculated: cost_per_mile               |
|  Restricted: emea_fuel_cost              |
|                                          |
|  HIERARCHIES                             |
|  geography: region > driver_name         |
|  time: year > quarter > month > day      |
|                                          |
|  GLOSSARY                                |
|  trips — One row per completed trip      |
|    trip_id: Unique trip identifier        |
|    miles: Distance driven                 |
|    fuel_cost: Fuel expense USD            |
+------------------------------------------+
```

### 6.2 Interactions

| Action | What Happens |
|---|---|
| Click "Auto-generate" | Calls `propose-semantic-model!` → assembles from proposals + context + joins |
| Edit entity/column | Updates `model_json`, bumps version |
| Add calculated measure | User writes expression, validated against model columns |
| Add hierarchy | User picks entity + column ordering |
| Publish | Changes status draft → published, ISL uses this model for queries |
| View lineage | Shows Bronze → Silver → Gold → Semantic Model chain |

---

## 7. API Routes

| Method | Path | Handler |
|---|---|---|
| POST | `/api/semantic/generate` | `generate-semantic-model` — auto-generate from proposals |
| GET | `/api/semantic/models` | `list-semantic-models` — list all models for connection |
| GET | `/api/semantic/models/:model_id` | `get-semantic-model` — full model object |
| PUT | `/api/semantic/models/:model_id` | `update-semantic-model` — edit model |
| POST | `/api/semantic/models/:model_id/publish` | `publish-semantic-model` — draft → published |
| DELETE | `/api/semantic/models/:model_id` | `delete-semantic-model` |
| GET | `/api/semantic/models/:model_id/versions` | `list-model-versions` — version history |
| GET | `/api/semantic/models/:model_id/versions/:version` | `get-model-version` — specific version |
| POST | `/api/semantic/models/:model_id/measures` | `add-calculated-measure` — add formula |
| POST | `/api/semantic/models/:model_id/hierarchies` | `add-hierarchy` — add drill path |
| POST | `/api/semantic/models/:model_id/validate` | `validate-semantic-model` — check consistency |

---

## 8. Implementation Phases

### Phase 1 — Surface (2-3 weeks)

**Goal**: Make existing intelligence visible. No new inference logic.

1. **`semantic/model.clj`** — `propose-semantic-model!` assembles from existing Silver/Gold proposals + schema_context + discover-joins
2. **`semantic_model` + `semantic_model_version` tables** — DDL + CRUD
3. **API routes**: generate, list, get, update, publish, delete
4. **Model Explorer UI** — browsable entity/column/relationship view in modeling console
5. **Glossary view** — schema_context entries rendered as browsable business glossary

**What users see**: "Auto-generate Semantic Model" button → star schema diagram with facts, dimensions, measures, relationships, descriptions, confidence scores.

### Phase 2 — Calculated Measures + Hierarchies (3-4 weeks)

**Goal**: Close the two biggest Datasphere gaps.

**NOTE**: This is the hardest phase. The current ISL validation pipeline (`validate-isl` in `home.clj`) validates raw column refs and aggregate columns against the warehouse registry. Calculated measures introduce a new class of reference — semantic formulas that don't exist as physical columns. This requires a defined expression language, aggregation semantics, and dependency ordering.

#### 2a. Calculated Measure Expression Contract

**Expression language** (subset of SQL scalar expressions):
- Arithmetic: `+`, `-`, `*`, `/`
- Functions: `NULLIF`, `COALESCE`, `ABS`, `ROUND`, `CAST`
- Aggregates: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX` (only in aggregate-context measures)
- References: bare column names that MUST resolve to columns on the measure's `:entity`
- No subqueries, no window functions, no cross-entity column refs

**Dependency ordering**: Calculated measures may reference base columns or other calculated measures on the same entity. `validate-semantic-model` must topologically sort measures and reject cycles. A measure that references another calculated measure is expanded recursively at compile time.

**Aggregation semantics**: A calculated measure is either:
- **Row-level** (e.g. `fuel_cost / NULLIF(miles, 0)`) — applied per-row, then aggregated by the query's GROUP BY
- **Post-aggregate** (e.g. `SUM(fuel_cost) / SUM(miles)`) — applied after GROUP BY

The `:aggregation` field on each calculated measure specifies this: `"row"` (default) or `"post"`. Row-level measures become SELECT-list column aliases. Post-aggregate measures become expressions over already-aggregated columns.

**ISL integration**: `validate-isl` gains a new validation pass that resolves calculated measure refs against the model before checking physical columns. The tool schema exposes them as enum-locked options (LLM picks from a list, not free-form).

#### 2b. Implementation Steps

1. **Expression parser + validator** — parse calculated measure expressions, resolve column refs against entity columns + other measures, detect cycles, classify row-level vs post-aggregate
2. **Restricted measures** — base measure + filter predicate + explicit `:via_relationship` for cross-entity filters, stored in model_json
3. **Hierarchies** — level-based drill paths on dimension entities
4. **ISL integration** — `build-isl-prompt-from-model` uses model instead of raw registry. Calculated measures become enum-locked tool options. Hierarchy hints in prompt.
5. **Query-time resolution** — row-level measures injected as column aliases in SELECT; post-aggregate measures injected as expressions over aggregated columns. Dependency order enforced.

### Phase 3 — Model as Query Source (2-3 weeks)

**Goal**: ISL queries reference the Semantic Model, not raw tables.

1. **Lazy associations** — `resolve-lazy-joins` adds JOINs only when referenced columns require them
2. **Model-scoped ISL** — tool schema generated from model (entity enums, measure enums, hierarchy enums) instead of raw registry
3. **Perspectives** — scoped views of a model for different audiences (subset of entities/measures)
4. **NL model editing** — "add a cost_per_mile metric to Fleet Operations model" via chat

### Phase 4 — Governance (2-3 weeks)

**Goal**: Production-grade lifecycle.

1. **Approval workflow** — draft → review → published (reuse Silver proposal review pattern)
2. **Promotion** — workspace-scoped models, promote between workspaces
3. **Row-level security** — dimension-based access filters, enforced at ISL query time
4. **Impact analysis** — show which saved reports depend on a model before editing

#### Phase 4 Storage

```sql
-- Approval / review state per model version
CREATE TABLE semantic_model_review (
  review_id      BIGSERIAL PRIMARY KEY,
  model_id       TEXT NOT NULL REFERENCES semantic_model(model_id),
  version        INTEGER NOT NULL,
  reviewer       TEXT NOT NULL,
  decision       TEXT NOT NULL,           -- approved | rejected | needs_changes
  comment        TEXT,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Row-level security policies attached to dimensions
CREATE TABLE semantic_rls_policy (
  policy_id      BIGSERIAL PRIMARY KEY,
  model_id       TEXT NOT NULL REFERENCES semantic_model(model_id),
  entity         TEXT NOT NULL,           -- dimension entity name
  column_name    TEXT NOT NULL,           -- e.g. "region"
  allowed_values_json TEXT,               -- JSON array of allowed values, or null = unrestricted
  user_field     TEXT NOT NULL,           -- session field to match (e.g. "workspace_key", "roles")
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### Phase 4 API Routes

| Method | Path | Handler |
|---|---|---|
| POST | `/api/semantic/models/:model_id/review` | `review-semantic-model` — submit approval decision |
| GET | `/api/semantic/models/:model_id/reviews` | `list-model-reviews` — review history |
| POST | `/api/semantic/models/:model_id/promote` | `promote-semantic-model` — copy to target workspace |
| POST | `/api/semantic/models/:model_id/rls` | `add-rls-policy` — attach dimension filter |
| GET | `/api/semantic/models/:model_id/rls` | `list-rls-policies` — current policies |
| DELETE | `/api/semantic/models/:model_id/rls/:policy_id` | `delete-rls-policy` |
| GET | `/api/semantic/models/:model_id/impact` | `model-impact-analysis` — saved reports + queries that reference this model |

---

## 9. Positioning

> "Other tools make you build the semantic layer by hand — in YAML (dbt), in LookML (Looker), in a point-and-click modeler (Datasphere). Bitool infers it from your data and lets you refine it. The semantic layer writes itself."

| Tool | Semantic Layer Authoring | Bitool |
|---|---|---|
| dbt | Write YAML metrics manually | Auto-inferred from proposals, edit if you want |
| Looker | Write LookML files manually | Auto-inferred, visual editor |
| Datasphere | Point-and-click modeler | Auto-inferred + NL editing |
| Cube | Write schema.js manually | Auto-inferred, zero config |

The confidence score is the differentiator: users see **how sure** the system is about each classification, measure, and relationship. Low confidence = human review needed. High confidence = trust the automation.

---

## 10. Files to Create / Modify

| File | Action | Purpose |
|---|---|---|
| `src/clj/bitool/semantic/model.clj` | Create | Core: propose, CRUD, validate, version |
| `src/clj/bitool/semantic/routes.clj` | Create | API route handlers |
| `src/clj/bitool/semantic/measures.clj` | Create (Phase 2) | Calculated + restricted measure engine |
| `src/clj/bitool/handler.clj` | Modify | Mount `(semantic-routes)` |
| `src/clj/bitool/routes/home.clj` | Modify | `build-isl-prompt-from-model` (Phase 3) |
| `resources/public/semanticLayerComponent.js` | Create | Model Explorer UI |
| `resources/public/modelingConsole.js` | Modify | Add "Semantic Model" tab/button |
| `test/clj/bitool/semantic_model_test.clj` | Create | Auto-generation, CRUD, validation tests |
| `test/clj/bitool/semantic_measures_test.clj` | Create (Phase 2) | Measure expression tests |

---

*This design builds on what exists. Phase 1 is pure surfacing — no new ML, no new inference. The intelligence is already computed; we just need to name it, store it, and show it.*
