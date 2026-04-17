# Zero-Config AI Reporting — Technical Design

> **Goal**: Connect any database. Ask any question. Get a validated, correct answer in 3 seconds. No setup. No analyst. No dashboard to build.

## The Problem

Current ISL pipeline is hardcoded to Sheetz fleet data:
- `gold-table-registry` — 17 tables, 90+ columns, all Sheetz-specific (`home.clj:3197`)
- `build_fleet_query` — tool name, enum values, all Sheetz (`home.clj:3317`)
- `isl-system-prompt` — Sheetz vehicle IDs, date ranges, sensor counts (`home.clj:3377`)
- `reporting-data` — 7 canned SQL queries for fleet KPIs (`home.clj:2948`)

A new customer connecting their Postgres/Snowflake/Databricks gets nothing. We need to make this work for any schema in any database, with the same hallucination-prevention guarantees.

**Design principle**: The ISL pattern prevents hallucinated SQL by constraining LLM output to enum-locked tool schemas. It does NOT guarantee the LLM will always pick the right column for the right table — `validate-isl` catches those errors deterministically after the LLM call. The guarantee is: **no invalid SQL ever reaches the database**. The LLM may need a validation retry, but it can never corrupt data or bypass schema checks.

---

## Architecture: Before & After

```
BEFORE (Sheetz-only)
┌──────────┐    ┌──────────────────┐    ┌──────────┐    ┌─────────┐    ┌─────────┐
│  User NL  │───>│  Hardcoded Tool   │───>│ Validate  │───>│ Compile  │───>│ Execute │
│  Question │    │  (17 Sheetz tbls) │    │ vs const  │    │ ISL→SQL  │    │ on ds   │
└──────────┘    └──────────────────┘    └──────────┘    └─────────┘    └─────────┘

AFTER (any database)
┌──────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────┐    ┌─────────┐    ┌─────────┐
│  User NL  │───>│  Dynamic Tool     │───>│  Dynamic Prompt   │───>│ Validate  │───>│ Compile  │───>│ Execute │
│  Question │    │  (from registry)  │    │  (from registry)  │    │ vs reg.   │    │ ISL→SQL  │    │ on conn │
└──────────┘    └──────────────────┘    └──────────────────┘    └──────────┘    └─────────┘    └─────────┘
                         ▲                        ▲                     ▲                            ▲
                         │                        │                     │                            │
                    ┌────┴────────────────────────┴─────────────────────┴────────────────────────────┴───┐
                    │                        Schema Registry (runtime, cached)                            │
                    │                 introspect(conn-id) → {table → [{col, type}]}                      │
                    └────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Dynamic Schema Registry + Generic ISL

**This is 80% of the value.** Everything else builds on it.

### 1.1 Schema Introspection

New function in `db.clj` — reuse existing `get-ds` / `get-opts` infrastructure:

```clojure
(defn introspect-schema
  "Query information_schema to build a runtime schema registry.
   Returns {\"table_name\" [{:col \"col_name\" :type \"varchar\" :quoted? bool} ...]}.
   Respects optional table-filter set to limit scope."
  [conn-id & {:keys [schema table-filter]}]
  (let [db-spec  (get-dbspec conn-id false)
        dbtype   (:dbtype db-spec)
        db-opts  (get-opts conn-id false)
        raw-cols (case dbtype
                   "postgresql"
                   (jdbc/execute! db-opts
                     [(str "SELECT table_name, column_name, data_type, is_nullable
                            FROM information_schema.columns
                            WHERE table_schema = ?"
                          " ORDER BY table_name, ordinal_position")
                      (or schema "public")])

                   "snowflake"
                   (jdbc/execute! db-opts
                     ["SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name,
                             DATA_TYPE as data_type, IS_NULLABLE as is_nullable
                      FROM INFORMATION_SCHEMA.COLUMNS
                      WHERE TABLE_SCHEMA = ?
                      ORDER BY TABLE_NAME, ORDINAL_POSITION"
                      (or schema "PUBLIC")])

                   "databricks"
                   (jdbc/execute! db-opts
                     ["SELECT table_name, column_name, data_type, is_nullable
                      FROM information_schema.columns
                      WHERE table_schema = ?
                      ORDER BY table_name, ordinal_position"
                      (or schema "default")])

                   ;; Fallback: standard information_schema
                   (jdbc/execute! db-opts
                     ["SELECT table_name, column_name, data_type, is_nullable
                      FROM information_schema.columns
                      WHERE table_schema = ?
                      ORDER BY table_name, ordinal_position"
                      (or schema "public")]))]
    (->> raw-cols
         (filter (fn [row]
                   (or (nil? table-filter)
                       (contains? table-filter (:table_name row)))))
         (group-by :table_name)
         (reduce-kv
           (fn [acc tbl cols]
             (assoc acc tbl
               (mapv (fn [c]
                       (cond-> {:col  (:column_name c)
                                :type (normalize-pg-type (:data_type c))}
                         (re-find #"[A-Z]" (:column_name c))
                         (assoc :quoted? true)))
                     cols)))
           {}))))
```

Type normalization helper (Postgres `character varying` → `varchar`, etc.):

```clojure
(defn- normalize-pg-type [raw]
  (let [t (str/lower-case (or raw "text"))]
    (cond
      (str/includes? t "int")     "int8"
      (str/includes? t "float")   "float8"
      (str/includes? t "double")  "float8"
      (str/includes? t "numeric") "float8"
      (str/includes? t "decimal") "float8"
      (str/includes? t "bool")    "boolean"
      (str/includes? t "date")    "date"
      (str/includes? t "time")    "timestamp"
      :else                       "text")))
```

### 1.2 Schema Cache

Introspection is expensive. Cache the **full** schema per connection/schema pair with 10-minute TTL. Scope filtering is applied *after* cache lookup so different reporting scopes don't poison each other's cache entries.

```clojure
;; In db.clj
(def ^:private schema-cache (atom {}))

(defn get-schema-registry
  "Return schema registry for a connection, optionally filtered to a table subset.
   Caches the FULL schema per [conn-id, schema]. Scope filtering (table-filter)
   is applied post-cache so different scopes share the same introspection result."
  [conn-id & {:keys [schema table-filter force-refresh]}]
  (let [cache-key [conn-id (or schema "public")]
        cached    (get @schema-cache cache-key)
        ttl-ms    600000  ;; 10 minutes
        stale?    (or force-refresh
                      (nil? cached)
                      (> (- (System/currentTimeMillis) (:ts cached 0)) ttl-ms))
        ;; Always cache the full (unfiltered) registry
        full-registry
        (if stale?
          (let [registry (introspect-schema conn-id :schema schema)]
            (swap! schema-cache assoc cache-key {:registry registry
                                                  :ts (System/currentTimeMillis)})
            registry)
          (:registry cached))]
    ;; Apply scope filter AFTER cache lookup
    (if (seq table-filter)
      (select-keys full-registry table-filter)
      full-registry)))
```

**Why this design**: The cache key is `[conn-id, schema]` — it stores the full introspection result once. When Phase 3 introduces reporting scopes, `table-filter` is a `#{set}` passed in by the caller. The filter runs against the cached full registry, not against the introspection query. This means:
- Scope A (tables `#{orders customers}`) and Scope B (tables `#{products inventory}`) both hit the same cache entry
- No scope can poison another scope's view
- Cache invalidation is simple: one key per connection/schema pair

### 1.3 Dynamic ISL Tool Builder

Replace the hardcoded `isl-tool` def with a function. The key design decision: **column enums are per-table, not global**. A single flat `columns` enum across all tables allows the LLM to produce structurally valid JSON that still references columns from the wrong table (e.g., `table=orders` with `columns=["customer_email"]`). Instead, we document per-table columns in the tool description and rely on the system prompt + validation to enforce correctness. This keeps the tool schema smaller and the LLM more accurate.

```clojure
;; In home.clj — replaces (def ^:private isl-tool ...)
(defn- build-isl-tool
  "Generate an Anthropic tool definition from a runtime schema registry.
   Table enum prevents hallucinated table names. Column validation happens
   in validate-isl (not in the tool schema) to avoid cross-table column confusion."
  [registry]
  (let [tables      (vec (sort (keys registry)))
        ;; Build per-table column documentation for the tool description
        table-col-doc (str/join "; "
                        (map (fn [[tbl cols]]
                               (str tbl " → " (str/join ", " (map :col cols))))
                             (sort-by key registry)))]
    ;; Guard: if schema is too large, the tool becomes unwieldy
    (when (> (count tables) 50)
      (throw (ex-info "Schema too large for single-tool ISL. Use reporting scope to narrow."
                      {:table-count (count tables)})))
    {:name "build_query"
     :description (str "Build a structured query against the connected database. "
                       "Output an ISL document — never raw SQL. "
                       "IMPORTANT: Only use columns that belong to the chosen table. "
                       "Available columns per table: " table-col-doc)
     :input_schema
     {:type "object"
      :required ["table" "intent"]
      :properties
      {"table"
       {:type "string"
        :enum tables
        :description "Table to query"}

       "columns"
       {:type "array"
        :items {:type "string"}    ;; No global enum — per-table enforcement via validate-isl
        :description "Columns to SELECT. Must belong to the chosen table. Omit for all columns."}

       "aggregates"
       {:type "array"
        :items {:type "object"
                :required ["fn" "column"]
                :properties {"fn"     {:type "string" :enum ["SUM" "AVG" "MIN" "MAX" "COUNT"]}
                             "column" {:type "string"}  ;; validated per-table
                             "alias"  {:type "string"}}}
        :description "Aggregate functions to apply. Column must belong to the chosen table."}

       "filters"
       {:type "array"
        :items {:type "object"
                :required ["column" "op" "value"]
                :properties {"column" {:type "string"}  ;; validated per-table
                             "op"     {:type "string" :enum ["=" "!=" ">" "<" ">=" "<=" "LIKE" "IN" "BETWEEN"]}
                             "value"  {:oneOf [{:type "string"} {:type "number"}
                                               {:type "array" :items {:oneOf [{:type "string"} {:type "number"}]}}]}}}
        :description "WHERE conditions (ANDed together)"}

       "group_by"
       {:type "array"
        :items {:type "string"}  ;; validated per-table
        :description "GROUP BY columns"}

       "order_by"
       {:type "array"
        :items {:type "object"
                :required ["column"]
                :properties {"column"    {:type "string"}
                             "direction" {:type "string" :enum ["ASC" "DESC"]}}}
        :description "ORDER BY clauses"}

       "date_trunc"
       {:type "string"
        :enum ["day" "week" "month" "quarter" "year"]
        :description "Truncate date column to this granularity"}

       "date_column"
       {:type "string"
        :description "Which date column to truncate (must belong to chosen table)"}

       "limit"
       {:type "integer" :minimum 1 :maximum 500
        :description "Max rows to return (default 100)"}

       "intent"
       {:type "string"
        :description "One-line description of what the user is asking for"}}}}))
```

**Key design decisions:**
- **Table enum**: hard-locked via `enum` — LLM literally cannot invent a table name
- **Column fields**: NO global enum. Columns are free-text in the tool schema, but the tool description documents per-table columns, and `validate-isl` rejects any column that doesn't belong to the chosen table. This prevents cross-table column confusion that a flat global enum would allow.
- **Two-layer defense**: Tool description guides the LLM to pick correct columns. `validate-isl` catches anything that slips through. No invalid SQL ever reaches the database.
- `date_column` field added (not every table uses `event_date`)
- `date_trunc` supports `quarter` and `year`
- Limit raised to 500

### 1.4 Dynamic System Prompt Builder

Replace hardcoded `isl-system-prompt` with a function:

```clojure
(defn- build-isl-prompt
  "Generate a system prompt from a runtime schema registry.
   Includes per-table column listing so the LLM knows what belongs where."
  [registry & {:keys [description sample-values]}]
  (let [table-docs (str/join "\n"
                     (map (fn [[tbl cols]]
                            (str "  " tbl ": "
                                 (str/join ", " (map :col cols))))
                          (sort-by key registry)))
        date-cols  (->> (vals registry)
                        (mapcat identity)
                        (filter #(= "date" (:type %)))
                        (map :col)
                        distinct
                        vec)]
    (str
      "You are an ISL (Intelligent Structured Language) translator.\n"
      "The user asks natural-language questions about their data.\n"
      "You MUST respond by calling the build_query tool with a valid ISL document.\n"
      "NEVER write SQL. NEVER respond with plain text. ALWAYS use the tool.\n\n"
      (when description (str description "\n\n"))
      "IMPORTANT — Each table has SPECIFIC columns. Only reference columns that belong to the chosen table:\n"
      table-docs "\n\n"
      "Rules:\n"
      "- Use BETWEEN for date ranges.\n"
      "- Use date_trunc for time-series trends. Specify date_column if the table's date column is not obvious.\n"
      "- For top-N questions use order_by + limit.\n"
      "- For totals/averages use aggregates + group_by.\n"
      "- If a column name contains uppercase letters, it will be auto-quoted.\n"
      (when (seq date-cols)
        (str "- Date columns detected: " (str/join ", " date-cols) "\n"))
      (when sample-values
        (str "\nSample values for context:\n"
             (str/join "\n" (map (fn [[col vals]]
                                   (str "  " col ": " (str/join ", " (take 5 vals))))
                                 sample-values))
             "\n")))))
```

### 1.5 Parameterize validate-isl and compile-isl

Minimal changes — pass registry as argument instead of reading the global:

```clojure
;; validate-isl: change (let [... registry gold-table-registry ...])
;; to accept registry as parameter:
(defn- validate-isl
  [isl registry]       ;; <-- add registry param
  (let [errors   (atom [])
        table    (get isl :table (get isl "table"))
        ;; ... rest unchanged, just uses `registry` instead of `gold-table-registry`
        tbl-cols (get registry table)]
    ;; ... same validation logic ...
    ))

;; compile-isl: change the col-type-map line
(defn- compile-isl
  [isl registry]       ;; <-- add registry param
  (let [;; ... same bindings ...
        col-type-map (into {} (map (juxt :col :type)
                                   (get registry table)))  ;; was gold-table-registry
        ;; Also: date_column support for date_trunc
        date-col (or (get isl :date_column (get isl "date_column")) "event_date")
        ;; Replace all hardcoded "event_date" references with date-col
        ;; ... rest of function uses date-col instead of "event_date" ...
        ]))
```

### 1.6 Updated Pipeline: reporting-ask

```clojure
(defn reporting-ask [request]
  (let [params     (:params request)
        question   (or (:question params) (:message params))
        conn-id    (or (:conn_id params)
                       (:conn-id (:session request))
                       :default)  ;; fallback to default connection
        ;; Resolve schema registry
        registry   (if (= :default conn-id)
                     gold-table-registry            ;; backward compat for Sheetz demo
                     (db/get-schema-registry conn-id
                       :schema (:schema params)))
        tool       (build-isl-tool registry)
        prompt     (build-isl-prompt registry
                     :description (:description params))]
    (if (str/blank? question)
      {:status 400 :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error "question is required"})}
      (try
        (let [isl-doc    (nl->isl question prompt tool)  ;; pass dynamic prompt+tool
              validation (validate-isl isl-doc registry)]
          (if-not (:valid validation)
            {:status 400 :headers {"Content-Type" "application/json"}
             :body (json/generate-string {:error "ISL validation failed"
                                          :details (:errors validation)
                                          :isl isl-doc})}
            (let [{:keys [sql params]} (compile-isl isl-doc registry)
                  ;; Execute against the right connection
                  ds       (if (= :default conn-id)
                             db/ds
                             (db/get-ds conn-id false))
                  rows     (jdbc/execute! ds (into [sql] params)
                             {:builder-fn rs/as-unqualified-lower-maps})
                  columns  (if (seq rows) (mapv name (keys (first rows))) [])]
              {:status 200 :headers {"Content-Type" "application/json"}
               :body (json/generate-string
                       {:sql sql :columns columns :rows rows
                        :count (count rows) :isl isl-doc
                        :pipeline "NL → ISL → Validate → SQL → Execute"
                        :connection conn-id})})))
        (catch Exception e
          {:status 500 :headers {"Content-Type" "application/json"}
           :body (json/generate-string {:error (.getMessage e)})})))))

;; Updated nl->isl to accept dynamic prompt and tool:
(defn- nl->isl [question system-prompt tool]
  (let [call-llm (requiring-resolve 'bitool.ai.llm/call-llm)
        result   (call-llm system-prompt question
                   :anthropic-model "claude-sonnet-4-6"
                   :openai-model    "gpt-4.1"
                   :max-tokens      1024
                   :temperature     0
                   :tools           [tool]
                   :tool-choice     {:type "tool" :name (:name tool)})]
    (when-not (map? result)
      (throw (ex-info "LLM failed to produce ISL" {:question question :result result})))
    result))
```

### 1.7 Backward Compatibility

Keep `gold-table-registry` as a constant. When no `conn_id` is provided, fall back to it. The Sheetz demo keeps working with zero changes to the frontend.

---

## Phase 2: Connection-Scoped Reporting UI

### 2.1 Connection Selector

Add a dropdown to `reporting.html` above the AI chat:

```html
<div class="rpt-conn-selector" id="rptConnSelector" style="display:none">
  <label style="font-size:12px;color:#6b7280;font-weight:600">DATA SOURCE</label>
  <select id="rptConnSelect">
    <option value="">Default (demo)</option>
  </select>
  <button id="rptRefreshSchema" title="Refresh schema">↻</button>
</div>
```

### 2.2 Frontend: Pass conn_id with Every Ask

In `reportingDashboard.js`, modify the AI ask call:

```javascript
// Current:
fetch('/api/reporting/ask', { body: JSON.stringify({ question: text }) })

// After:
var connId = el('rptConnSelect')?.value || '';
fetch('/api/reporting/ask', {
  body: JSON.stringify({ question: text, conn_id: connId || undefined })
})
```

### 2.3 Schema Explorer Endpoint

New route to let the UI show available tables before the user asks anything:

```
GET /api/reporting/schema?conn_id=5
→ { tables: ["orders", "customers", ...],
    columns: { orders: [{col: "id", type: "int8"}, ...] },
    table_count: 12,
    column_count: 47 }
```

This powers:
- Autocomplete suggestions ("Try: what are total orders by month?")
- Table/column browser in the sidebar
- Smart suggestion chips based on actual schema

### 2.4 Auto-Generated Landing KPIs

Replace the hardcoded `reporting-data` with a **metadata-first overview**. Running `COUNT(*)` on every table at page load is too expensive for warehouse-scale schemas (Snowflake, Databricks, even medium Postgres). Instead:

**Strategy**: Show schema metadata immediately (table names, column counts, date columns). Compute row counts lazily per-table in the background, capped at 10 tables.

```clojure
(defn reporting-overview [conn-id registry]
  "Metadata-first overview. Returns immediately with schema info.
   Row counts are computed lazily via /api/reporting/table-stats."
  (mapv (fn [[tbl cols]]
          (let [date-col (first (filter #(= "date" (:type %)) cols))]
            {:table       tbl
             :columns     (count cols)
             :column_names (mapv :col cols)
             :date_column (:col date-col)
             :has_date    (boolean date-col)}))
        (sort-by key registry)))

(defn reporting-table-stats [request]
  "Lazy per-table stats. Called by frontend for individual tables on expand/hover.
   Has a 5-second timeout to avoid warehouse hangs."
  (let [params  (:params request)
        conn-id (:conn_id params)
        table   (:table params)
        registry (db/get-schema-registry (or conn-id :default))
        tbl-cols (get registry table)]
    (if-not tbl-cols
      {:status 404 :body {:error (str "Table not found: " table)}}
      (let [ds       (if conn-id (db/get-ds conn-id false) db/ds)
            date-col (first (filter #(= "date" (:type %)) tbl-cols))
            table-id (isl-quote-ident table)
            ;; Use a future with timeout to avoid blocking on slow warehouses
            result   (deref
                       (future
                         (jdbc/execute-one! ds
                           [(str "SELECT COUNT(*) as cnt"
                                 (when date-col
                                   (str ", MIN(" (isl-quote-col (:col date-col)) ") as min_date"
                                        ", MAX(" (isl-quote-col (:col date-col)) ") as max_date"))
                                 " FROM " table-id)]
                           {:builder-fn rs/as-unqualified-lower-maps}))
                       5000 nil)]  ;; 5s timeout
        (if result
          {:status 200 :body {:table table
                              :row_count (:cnt result)
                              :date_range (when date-col
                                            {:from (:min_date result)
                                             :to   (:max_date result)})}}
          {:status 200 :body {:table table
                              :row_count nil
                              :timeout true}})))))
```

**Frontend behavior**:
1. Page load → `GET /api/reporting/schema` → render table list with column counts (instant)
2. For each visible table card → `GET /api/reporting/table-stats?table=X` → fill in row count (lazy, parallel, max 10 concurrent)
3. If a table times out, show "---" instead of a count — user can still query it

---

## Phase 3: Reporting Scope (Guardrails for Large Schemas)

Problem: a Snowflake warehouse with 500 tables and 5000 columns blows up the tool enum.

### 3.1 Reporting Scope Model

New DB table:

```sql
CREATE TABLE reporting_scope (
  id          SERIAL PRIMARY KEY,
  conn_id     INTEGER REFERENCES connection(id),
  scope_name  TEXT NOT NULL,
  tables      TEXT[] NOT NULL,         -- subset of tables to include
  description TEXT,                    -- optional context for the LLM
  created_at  TIMESTAMP DEFAULT now()
);
```

### 3.2 Scope Selection Flow

1. User opens reporting → selects connection
2. If connection has >30 tables, prompt: "Select tables to include in reporting" (checkbox UI)
3. Save as a reporting scope
4. `get-schema-registry` filters to only scoped tables
5. Tool enums stay small, LLM stays accurate

### 3.3 Smart Scope Suggestions

Optional: when user connects, auto-suggest a scope:

```clojure
(defn suggest-scope [registry table-stats]
  "Pick the most reportable tables: those with a date/timestamp column and
   enough rows to be useful. table-stats is optional lazy metadata from
   /api/reporting/table-stats or warehouse catalog statistics."
  (->> registry
       (filter (fn [[tbl cols]]
                 (and (some #(contains? #{"date" "timestamp"} (:type %)) cols)
                      (> (or (get-in table-stats [tbl :row_count]) 0) 100))))
       (sort-by (fn [[tbl _]]
                  [(- (or (get-in table-stats [tbl :row_count]) 0)) tbl]))
       (map first)
       (take 30)
       vec))
```

If table counts are not available yet, fall back to "has date/timestamp column"
only and surface the suggestion as low-confidence in the UI rather than claiming
the `>100 rows` rule was applied.

---

## Phase 4: Conversational Memory

Problem: "Show me fuel data by month" → great. "Now filter to vehicle 100005" → starts from scratch.

**Critical design constraint**: Memory must be scoped to `[conn-id, scope-id]`. If the user switches connections or reporting scopes, history from the previous context must NOT carry over — otherwise "filter that" could silently apply filters from a completely different dataset.

### 4.1 Connection-Scoped ISL History

Store the last N ISL documents + results in the session, keyed by connection+scope:

```clojure
(defn- history-key
  "Session history is partitioned by connection and scope so follow-ups
   never leak across datasets."
  [conn-id scope-id]
  (keyword (str "isl-history-" (or conn-id "default") "-" (or scope-id "all"))))

;; In reporting-ask, after successful execution:
(let [hkey (history-key conn-id scope-id)]
  (update-in response [:session hkey]
    (fn [h] (take 5 (conj (or h [])
                           {:isl isl-doc :sql sql :intent intent
                            :conn-id conn-id :scope-id scope-id
                            :ts (System/currentTimeMillis)})))))
```

### 4.2 Context-Aware Prompt

Append recent history to the system prompt — **only from the same connection+scope**:

```clojure
(defn- append-history-context [base-prompt session conn-id scope-id]
  (let [hkey    (history-key conn-id scope-id)
        history (get session hkey)]
    (if (empty? history)
      base-prompt
      (str base-prompt "\n\n"
           "Recent queries on THIS connection for context (user may reference these):\n"
           (str/join "\n"
             (map-indexed (fn [i h]
                            (str (inc i) ". " (:intent (:isl h))
                                 " → table: " (get-in h [:isl :table])
                                 (when-let [f (get-in h [:isl :filters])]
                                   (str " filters: " (pr-str f)))))
                          (reverse history)))
           "\n\nIf the user says 'now filter that' or 'same but for X', "
           "reuse the most recent query's table and modify accordingly.\n"))))
```

### 4.3 Follow-Up Detection

No LLM call needed — simple heuristic:

```clojure
(defn- follow-up? [question]
  (boolean (re-find #"(?i)^(now |also |same |but |and |filter|add|remove|change|show me that|drill)" question)))
```

When detected, inject the last ISL as context so the LLM modifies rather than starts fresh.

### 4.4 Connection Switch Guard

When the user switches connections or scopes in the UI, the frontend should:
1. Clear the chat history display (visual reset)
2. Send the new `conn_id` / `scope_id` with the next ask
3. Backend automatically reads from the correct history partition

No explicit "clear history" API needed — the partitioning handles it. The user can switch back and their old history for that connection is still there.

---

## Phase 5: Saved Reports

### 5.1 Data Model

```sql
CREATE TABLE saved_report (
  id          SERIAL PRIMARY KEY,
  conn_id     INTEGER REFERENCES connection(id),
  name        TEXT NOT NULL,
  description TEXT,
  isl         JSONB NOT NULL,           -- the ISL document (stable, re-executable)
  scope_id    INTEGER REFERENCES reporting_scope(id),
  created_by  TEXT,
  created_at  TIMESTAMP DEFAULT now(),
  updated_at  TIMESTAMP DEFAULT now()
);
```

### 5.2 API

```
POST /api/reporting/save    {name, isl, conn_id}  → {id}
GET  /api/reporting/saved   ?conn_id=5             → [{id, name, description, created_at}]
POST /api/reporting/run     {report_id}            → same response as /ask
DELETE /api/reporting/saved/:id                     → {ok: true}
```

### 5.3 Re-Execution

Saved reports re-validate against the current schema before executing. If the schema changed (table dropped, column renamed), the user sees a clear error instead of a broken query:

```clojure
(defn run-saved-report [report-id]
  (let [report   (db/get-saved-report report-id)
        registry (db/get-schema-registry (:conn_id report))
        valid    (validate-isl (:isl report) registry)]
    (if-not (:valid valid)
      {:status 400 :body {:error "Schema changed since report was saved"
                          :details (:errors valid)}}
      ;; Re-compile and execute
      (let [{:keys [sql params]} (compile-isl (:isl report) registry)
            ;; ... execute ...
            ]))))
```

---

## Phase 6: Multi-Table Joins (ISL v2)

This is the hardest extension but required for real-world use. It should not be
marketed as "automatic on any warehouse" in v2 unless we add a second explicit
join metadata path beyond foreign keys.

### 6.1 Extended ISL Schema

Add a `join` field to the ISL document:

```json
{
  "table": "orders",
  "join": [
    {
      "table": "customers",
      "on": {"orders.customer_id": "customers.id"},
      "type": "LEFT"
    }
  ],
  "columns": ["orders.order_date", "customers.name", "orders.total"],
  "intent": "Show orders with customer names"
}
```

### 6.2 Join Discovery

First pass: auto-detect joinable relationships via foreign keys. This works
well for OLTP-style PostgreSQL schemas and some governed warehouses, but it is
not enough for Snowflake/Databricks analytics schemas where FKs are often
absent or unenforced.

Production contract:
- Try FK discovery first
- If no FK metadata exists, allow curated/manual join metadata
- Optionally add heuristic suggestions later (`customer_id -> customers.id`),
  but never compile those joins without validation/confirmation

V2 ship rule: support joins only when there is explicit metadata (FK or curated
join map). Do not pretend inference alone is reliable enough.

FK-backed discovery example:

```clojure
(defn discover-joins [conn-id schema]
  "Query information_schema to find FK relationships between tables."
  (let [ds (db/get-ds conn-id false)]
    (jdbc/execute! ds
      ["SELECT
         tc.table_name AS from_table,
         kcu.column_name AS from_column,
         ccu.table_name AS to_table,
         ccu.column_name AS to_column
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
       JOIN information_schema.constraint_column_usage ccu
         ON ccu.constraint_name = tc.constraint_name
       WHERE tc.constraint_type = 'FOREIGN KEY'
         AND tc.table_schema = ?"
       (or schema "public")]
      {:builder-fn rs/as-unqualified-lower-maps})))
```

Curated metadata fallback:

```sql
CREATE TABLE reporting_join_map (
  id           SERIAL PRIMARY KEY,
  conn_id      INTEGER REFERENCES connection(id),
  left_table   TEXT NOT NULL,
  left_column  TEXT NOT NULL,
  right_table  TEXT NOT NULL,
  right_column TEXT NOT NULL,
  join_type    TEXT DEFAULT 'LEFT',
  created_at   TIMESTAMP DEFAULT now()
);
```

### 6.3 Join Compilation

```clojure
;; In compile-isl, add join clause generation:
(defn- compile-joins [joins]
  (when (seq joins)
    (str/join " "
      (map (fn [j]
             (let [jtype (or (get j :type (get j "type")) "LEFT")
                   jtbl  (get j :table (get j "table"))
                   on    (get j :on (get j "on"))]
               (str jtype " JOIN " jtbl " ON "
                    (str/join " AND "
                      (map (fn [[l r]] (str l " = " r)) on)))))
           joins))))
```

### 6.4 Tool Extension

Add join info to the tool schema only when foreign keys exist:

```clojure
(defn- build-isl-tool [registry & {:keys [joins]}]
  (let [base-tool (build-base-isl-tool registry)]
    (if (empty? joins)
      base-tool
      (assoc-in base-tool [:input_schema :properties "join"]
        {:type "array"
         :items {:type "object"
                 :required ["table" "on"]
                 :properties
                 {"table" {:type "string" :enum (vec (keys registry))}
                  "on"    {:type "object" :description "Column mappings: {\"left.col\": \"right.col\"}"}
                  "type"  {:type "string" :enum ["LEFT" "INNER" "RIGHT"]}}}}))))
```

---

## Implementation Order

| Phase | What | Files Changed | Risk | Value |
|-------|------|---------------|------|-------|
| **1** | Dynamic schema registry + generic ISL | `db.clj` (+80 LOC), `home.clj` (~150 LOC changed) | Low — additive, backward compatible | **Highest** — unlocks any database |
| **2** | Connection selector UI | `reporting.html` (+30), `reportingDashboard.js` (+40) | Low | Medium — makes Phase 1 usable |
| **3** | Reporting scope | `db.clj` (+40), `home.clj` (+60), new SQL migration | Low | Medium — handles large schemas |
| **4** | Conversational memory | `home.clj` (+50) | Low | High — "filter that" is table stakes |
| **5** | Saved reports | `db.clj` (+30), `home.clj` (+80), `reportingDashboard.js` (+100) | Low | High — makes it sticky |
| **6** | Multi-table joins | `home.clj` (+120), `db.clj` (+40) | **Medium** — SQL compilation complexity | High — real-world necessity |

**Ship order: 1 → 2 → 4 → 3 → 5 → 6**

Phase 4 (memory) before Phase 3 (scope) because conversational context is more immediately valuable to users than admin guardrails.

---

## Guardrails & Safety

### SQL Injection Prevention
Already handled — `compile-isl` uses parameterized queries (`?` placeholders). The LLM never writes SQL. This is the entire point of ISL.

### Schema Size Limits
- Hard cap: 50 tables / 500 columns per tool invocation
- Beyond that: require a Reporting Scope (Phase 3)
- Error message: "Schema too large — select tables to include in reporting"

### Query Cost Protection
- `LIMIT` capped at 500 rows (tool schema enforces this)
- Engine-aware query timeout — `SET statement_timeout` is Postgres-specific. Each engine needs its own cancellation strategy:

  ```clojure
  (defn- execute-with-timeout
    "Run a query with engine-appropriate timeout. Returns rows or throws on timeout."
    [conn-id ds sql-vec timeout-seconds]
    (let [dbtype (if conn-id
                   (name (db/get-dbtype conn-id))
                   "postgresql")]
      (case dbtype
        ;; PostgreSQL: session-level statement_timeout
        "postgresql"
        (do (jdbc/execute! ds [(str "SET statement_timeout = '" (* timeout-seconds 1000) "'")])
            (let [result (jdbc/execute! ds sql-vec {:builder-fn rs/as-unqualified-lower-maps})]
              (jdbc/execute! ds ["SET statement_timeout = '0'"])  ;; reset
              result))

        ;; Snowflake: STATEMENT_TIMEOUT_IN_SECONDS session parameter
        "snowflake"
        (do (jdbc/execute! ds [(str "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = " timeout-seconds)])
            (jdbc/execute! ds sql-vec {:builder-fn rs/as-unqualified-lower-maps}))

        ;; Databricks: uses JDBC queryTimeout (set on HikariCP or statement level)
        ;; Fall back to Future-based timeout
        (let [result (deref
                       (future (jdbc/execute! ds sql-vec {:builder-fn rs/as-unqualified-lower-maps}))
                       (* timeout-seconds 1000)
                       ::timeout)]
          (when (= result ::timeout)
            (throw (ex-info "Query timed out" {:timeout-seconds timeout-seconds :engine dbtype})))
          result))))
  ```

  Default timeout: **30 seconds**. Configurable per connection via `reporting_scope.timeout_seconds` in Phase 3.

### Credential Isolation
`get-ds` already manages per-connection pools via `ds-pool-cache`. Reporting queries execute against user-provided connections, never against `db/ds` (the admin pool) — unless explicitly falling back to the Sheetz demo.

---

## What NOT to Build

- **Semantic layer / business glossary** — the LLM is good enough at inferring meaning from column names. A semantic layer adds config burden that defeats "zero-config." Revisit only if accuracy drops below 90% on real schemas.
- **Cross-database joins** — federated queries are a different product. Stay single-connection.
- **Real-time / streaming reports** — ISL is batch/query. Streaming is a different architecture.
- **PDF/Excel export** — nice to have but not core. Defer to Phase 7+.
- **Role-based access control on reports** — important eventually, but not for MVP. All reports visible to all users of a connection.

---

## Success Metrics

| Metric | Current (Sheetz) | Phase 1 Target | iPhone Target |
|--------|-----------------|----------------|---------------|
| Setup time to first query | N/A (hardcoded) | < 30 seconds | < 10 seconds |
| Schema coverage | 17 tables | Any Postgres | Postgres, Snowflake, Databricks |
| Query accuracy (valid SQL) | ~95% | ~90% | ~95% |
| Follow-up support | None | None | 3-turn memory |
| Saved reports | None | None | Unlimited |

---

## Test Plan

### Phase 1 Tests (add to `home_test.clj`)

```clojure
(deftest build-isl-tool-generates-valid-schema
  (let [registry {"orders" [{:col "id" :type "int8"}
                            {:col "total" :type "float8"}
                            {:col "order_date" :type "date"}]
                  "customers" [{:col "id" :type "int8"}
                               {:col "name" :type "text"}]}
        tool (build-isl-tool registry)]
    (is (= "build_query" (:name tool)))
    (is (= ["customers" "orders"] (sort (get-in tool [:input_schema :properties "table" :enum]))))
    ;; Column fields should NOT have enum (per-table enforcement via validate-isl)
    (is (nil? (get-in tool [:input_schema :properties "columns" :items :enum])))
    ;; Tool description should include per-table column docs
    (is (str/includes? (:description tool) "orders → id, total, order_date"))
    (is (str/includes? (:description tool) "customers → id, name"))))

(deftest validate-isl-uses-dynamic-registry
  (let [registry {"orders" [{:col "id" :type "int8"} {:col "total" :type "float8"}]}
        good-isl {:table "orders" :columns ["id" "total"] :intent "test"}
        bad-isl  {:table "orders" :columns ["nonexistent"] :intent "test"}]
    (is (:valid (validate-isl good-isl registry)))
    (is (false? (:valid (validate-isl bad-isl registry))))))

(deftest compile-isl-with-dynamic-date-column
  (let [registry {"events" [{:col "happened_at" :type "date"} {:col "value" :type "float8"}]}
        isl {:table "events" :date_trunc "month" :date_column "happened_at"
             :aggregates [{"fn" "SUM" "column" "value" "alias" "total"}]
             :intent "monthly totals"}
        {:keys [sql]} (compile-isl isl registry)]
    (is (str/includes? sql "date_trunc('month', happened_at)"))
    (is (not (str/includes? sql "event_date")))))

(deftest schema-cache-respects-ttl
  (let [call-count (atom 0)]
    (with-redefs [db/introspect-schema (fn [& _] (swap! call-count inc)
                                         {"t1" [{:col "a" :type "text"}]
                                          "t2" [{:col "b" :type "int8"}]})]
      (db/get-schema-registry 1)
      (db/get-schema-registry 1)
      (is (= 1 @call-count) "Second call should use cache"))))

(deftest schema-cache-scope-filter-does-not-poison-cache
  "Different reporting scopes on the same connection must not interfere."
  (let [call-count (atom 0)]
    (with-redefs [db/introspect-schema (fn [& _] (swap! call-count inc)
                                         {"orders"    [{:col "id" :type "int8"}]
                                          "customers" [{:col "id" :type "int8"}]
                                          "products"  [{:col "id" :type "int8"}]})]
      ;; Scope A: only orders
      (let [scope-a (db/get-schema-registry 1 :table-filter #{"orders"})]
        (is (= #{"orders"} (set (keys scope-a)))))
      ;; Scope B: only customers+products — should NOT see scope A's filter
      (let [scope-b (db/get-schema-registry 1 :table-filter #{"customers" "products"})]
        (is (= #{"customers" "products"} (set (keys scope-b)))))
      ;; Full registry: all 3 tables
      (let [full (db/get-schema-registry 1)]
        (is (= 3 (count full))))
      ;; Only 1 introspection call — all served from same cache entry
      (is (= 1 @call-count))))

(deftest validate-isl-rejects-cross-table-columns
  "Column from table B used in query against table A must fail validation."
  (let [registry {"orders"    [{:col "id" :type "int8"} {:col "total" :type "float8"}]
                  "customers" [{:col "id" :type "int8"} {:col "email" :type "text"}]}
        ;; Try to use customers.email in an orders query
        bad-isl {:table "orders" :columns ["id" "email"] :intent "test"}]
    (is (false? (:valid (validate-isl bad-isl registry))))))
```

### Integration Smoke Test

```clojure
(deftest generic-reporting-round-trip
  ;; Requires a test Postgres with at least one table
  (let [conn-id  (test-conn-id)
        registry (db/get-schema-registry conn-id)
        tool     (build-isl-tool registry)
        prompt   (build-isl-prompt registry)]
    (is (pos? (count registry)) "Should discover at least one table")
    (is (contains? (set (get-in tool [:input_schema :properties "table" :enum]))
                   (first (keys registry))))
    ;; Validate a hand-crafted ISL against the live schema
    (let [tbl  (first (keys registry))
          cols (map :col (get registry tbl))
          isl  {:table tbl :columns [(first cols)] :limit 5 :intent "smoke test"}
          v    (validate-isl isl registry)]
      (is (:valid v)))))
```
