# GIL (Graph Intent Language) — Technical Design v1.0

**BiTool Intermediate Language for AI-Driven Graph Construction**

> Adapted from the IRL/ISL architecture in `medtronic-cicd`. Reuses the
> normalize → validate → compile → execute pipeline, registry-driven schemas,
> constrained LLM tool_use, and retry-with-feedback patterns.

---

## 0. Design Principles

1. **LLM is a PROPOSAL engine** — It never mutates the graph directly. It produces GIL, which is validated and compiled deterministically.
2. **GIL is the contract** — The intermediate representation is the single source of truth between NL and graph. If GIL is valid, the graph is guaranteed to be buildable.
3. **Compiler wraps existing graph ops** — The compiler calls `add-single-node`, `connect-single-node`, `save-endpoint`, `save-rl`, etc. using their **actual** function names and parameter shapes. The GIL config schema is a **thin mapping layer** over these existing contracts, not a new API.
4. **Incremental by default** — GIL supports both full-graph construction (`intent: :build`) and incremental mutations (`intent: :patch`). Users can say "build an API" or "add auth to endpoint-1".
5. **Deterministic compilation** — Same GIL always produces the same graph. No randomness in the compiler.
6. **Fail fast, fail clear** — Validation errors are human-readable and fed back to the LLM for self-correction.
7. **Dry-run is truly read-only** — The `/gil/compile` endpoint validates and plans mutations without touching the database. Only `/gil/apply` persists.

---

## 1. Architecture Overview

```
User NL command
  │
  ▼
┌─────────────────────────────────┐
│  bitool.gil.nl                  │   LLM (Anthropic tool_use)
│  - system prompt + examples     │   constrained JSON output
│  - tool schema from registry    │   retry with temp escalation
│  - validation error feedback    │
└──────────────┬──────────────────┘
               │ raw GIL (JSON)
               ▼
┌─────────────────────────────────┐
│  bitool.gil.normalize           │   Canonicalize keys, defaults,
│  - keywordize nested maps       │   resolve aliases, desugar
│  - default values               │   shorthand notations
│  - alias resolution             │
└──────────────┬──────────────────┘
               │ normalized GIL (EDN)
               ▼
┌─────────────────────────────────┐
│  bitool.gil.validator           │   Pure function → {:valid :errors :warnings}
│  - structural checks            │   No side effects
│  - node-type registry           │   Returns all errors, not just first
│  - edge legality (rectangles)   │
│  - required config per btype    │
│  - duplicate/cycle detection    │
└──────────────┬──────────────────┘
               │ validated GIL
               ▼
┌─────────────────────────────────┐
│  bitool.gil.compiler            │   Two modes:
│  PLAN MODE (dry-run):           │   - Topo-sort, validate edges,
│  - produces mutation plan       │     compute layout
│  - no DB writes                 │   - Returns plan for preview
│  APPLY MODE:                    │   - Calls graph2 functions
│  - executes plan against DB     │   - Persists graph, returns result
│  - topological save order       │
└──────────────┬──────────────────┘
               │ compiled graph (apply) or plan (dry-run)
               ▼
┌─────────────────────────────────┐
│  bitool.gil.api                 │   HTTP endpoints
│  - POST /gil/validate           │   Validate only
│  - POST /gil/compile            │   Validate + plan (NO persist)
│  - POST /gil/apply              │   Validate + plan + persist
│  - POST /gil/from-nl            │   NL → GIL → validate → plan → persist
└─────────────────────────────────┘
```

---

## 2. GIL Grammar

### 2.1 Top-Level Structure

```clojure
{:gil-version    "1.0"
 :intent         :build              ;; :build | :patch
 :graph-name     "user-orders-api"   ;; required for :build, optional for :patch
 :description    "REST API for..."   ;; optional, human-readable
 ;; For :build intent:
 :nodes          [<node-spec> ...]   ;; required for :build, omitted for :patch
 :edges          [<edge-spec> ...]   ;; required for :build, omitted for :patch
 ;; For :patch intent:
 :patches        [<patch-op> ...]}   ;; required for :patch, omitted for :build
```

### 2.2 Node Specification

```clojure
{:node-ref    "ep1"                  ;; local reference ID (unique within GIL doc)
 :type        "endpoint"             ;; human-readable type name (maps to btype via btype-codes)
 :alias       "get-users"            ;; display name on canvas (optional, defaults to type)
 :config      {<type-specific>}      ;; validated against node-keys registry (see §2.3)
 :position    {:x 100 :y 200}}       ;; optional, auto-layout if omitted
```

**Type names** use human-readable form (`"endpoint"`, `"filter"`, `"auth"`) not btype codes (`"Ep"`, `"Fi"`, `"Au"`). The compiler resolves via `btype-codes`.

**Node identity**: The `:node-ref` is persisted into the graph node as `:gil_ref` (a new attribute on `:na`). This enables patches to reference nodes durably across sessions. See §2.6 for details.

### 2.3 Type-Specific Config — Exact graph2 Contracts

Each config shape maps **directly** to the params expected by the corresponding `graph2/save-*` function. The GIL config IS the params map — the compiler passes it through with minimal transformation.

```clojure
;; ── Endpoint (Ep) ── save-endpoint [g id params]
;; Keys: :http_method :route_path :path_params :query_params :body_schema
;;       :response_format :description
{:http_method      "GET"                    ;; GET|POST|PUT|DELETE|PATCH (validated, defaults "GET")
 :route_path       "/users/:id/orders"      ;; colon-prefixed path params
 :path_params      [{:param_name "id" :data_type "integer" :description "User ID"}]
 :query_params     [{:param_name "limit" :data_type "integer" :required false :default_value "10"}]
 :body_schema      []                       ;; for POST/PUT/PATCH: [{:field_name :data_type :required}]
 :response_format  "json"                   ;; json|csv|edn (defaults "json")
 :description      "Fetch orders for user"}

;; ── Filter (Fi) ── save-filter [g id params]
;; Keys: :sql
{:sql "status = 'active'"}

;; ── Projection (P) ── handled via update-node, keys vary
{:columns ["order_id" "amount" "date"]}

;; ── Join (J) ── save via update-node
;; Keys: :join_type :left_key :right_key
{:join_type  "inner"                        ;; inner|left|right|full
 :left_key   "user_id"
 :right_key  "id"}

;; ── Aggregation (A) ── save via update-node
;; Keys: :groupby :having
{:groupby  ["status"]
 :having   "count(*) > 1"}

;; ── Sorter (S) ── save via update-node
;; Keys: :sort_by
{:sort_by [{:column "created_at" :direction "desc"}]}

;; ── Auth (Au) ── save-auth [g id params]
;; Keys: :auth_type :token_header :secret :claims_to_cols
{:auth_type      "bearer"                   ;; api-key|bearer|basic|oauth2
 :token_header   "Authorization"            ;; header name
 :secret         "my-jwt-secret"            ;; signing secret (preserved if blank on update)
 :claims_to_cols [{:column "user_id" :data_type "integer"}]}  ;; JWT claims → columns

;; ── Validator (Vd) ── save-validator [g id params]
;; Keys: :rules
{:rules [{:field "id" :rule "required"}
         {:field "id" :rule "type" :value "integer"}
         {:field "limit" :rule "min" :value 1}
         {:field "limit" :rule "max" :value 100}]}
;; rule must be: required|min|max|min-length|max-length|regex|one-of|type

;; ── Rate Limiter (Rl) ── save-rl [g id params]
;; Keys: :max_requests :window_seconds :key_type :burst
{:max_requests    100                       ;; integer (defaults 100)
 :window_seconds  60                        ;; integer (defaults 60)
 :key_type        "ip"                      ;; ip|header:X-Client-ID|param:api_key (defaults "ip")
 :burst           0}                        ;; integer (defaults 0)

;; ── CORS (Cr) ── save-cr [g id params]
;; Keys: :allowed_origins :allowed_methods :allowed_headers :allow_credentials :max_age
{:allowed_origins    ["*"]                  ;; vector or comma-separated string
 :allowed_methods    ["GET" "POST"]
 :allowed_headers    ["Content-Type" "Authorization"]
 :allow_credentials  false                  ;; boolean (string "true" accepted)
 :max_age            3600}                  ;; integer (defaults 86400)

;; ── Logger (Lg) ── save-lg [g id params]
;; Keys: :log_level :fields_to_log :destination :format :external_url
{:log_level      "info"                     ;; debug|info|warn|error (defaults "INFO")
 :fields_to_log  []                         ;; vector of field names
 :destination    "console"                  ;; console|file|external (defaults "console")
 :format         "json"                     ;; json|text (defaults "json")
 :external_url   nil}                       ;; optional URL for external destination

;; ── DB Execute (Dx) ── save-dx [g id params]
;; Keys: :connection_id :operation :sql_template :result_mode
{:connection_id  "default"                  ;; connection identifier
 :operation      "SELECT"                   ;; SELECT|INSERT|UPDATE|DELETE (defaults "SELECT")
 :sql_template   "SELECT * FROM orders WHERE user_id = :id"
 :result_mode    "single"}                  ;; single|multi (defaults "single")
;; NOTE: DDL (CREATE, DROP, ALTER) is rejected by save-dx validation

;; ── Response Builder (Rb) ── save-response-builder [g id params]
;; Keys: :status_code :response_type :headers :template
{:status_code    200                        ;; integer or string (defaults "200")
 :response_type  "json"                     ;; json|xml|text (defaults "json")
 :headers        "{\"X-Custom\":\"value\"}" ;; JSON string or empty string (parsed by parse-rb-headers)
 :template       [{:output_key "id" :source_column "orders.order_id"}
                  {:output_key "total" :source_column "orders.amount"}]}
;; template is a vector of maps, each with :output_key and :source_column (both required, non-blank).
;; Empty rows (both blank) are silently dropped. Partially blank rows are rejected.

;; ── Cache (Cq) ── save-cq [g id params]
;; Keys: :cache_key :ttl_seconds :strategy
{:cache_key    "orders::user_id"            ;; string (defaults "")
 :ttl_seconds  300                          ;; integer (defaults 300)
 :strategy     "read-through"}              ;; read-through|write-through (defaults "read-through")

;; ── Circuit Breaker (Ci) ── save-ci [g id params]
;; Keys: :failure_threshold :reset_timeout :fallback_response
{:failure_threshold  5                      ;; integer (defaults 5)
 :reset_timeout      30                     ;; integer seconds (defaults 30)
 :fallback_response  "{}"}                  ;; string JSON (defaults "{}")

;; ── Event Emitter (Ev) ── save-ev [g id params]
;; Keys: :topic :broker_url :key_template :format
{:topic         "order.fetched"             ;; string (defaults "")
 :broker_url    ""                          ;; string (defaults "")
 :key_template  ""                          ;; string (defaults "")
 :format        "json"}                     ;; string (defaults "json")

;; ── Scheduler (Sc) ── save-sc [g id params]
;; Keys: :cron_expression :timezone :params
{:cron_expression  "0 */5 * * *"            ;; validated 5-part cron
 :timezone         "UTC"                    ;; validated timezone
 :params           [{:name "batch_size" :data_type "integer"}]}
;; NOTE: sc-params->tcols reads :name (NOT :param_name) for column_name.
;; Using :param_name would produce blank column names.

;; ── Webhook (Wh) ── save-wh [g id params]
;; Keys: :webhook_path :secret_header :secret_value :payload_format
{:webhook_path    "/hooks/order-created"
 :secret_header   "X-Webhook-Secret"
 :secret_value    "whsec_123"               ;; preserved if blank on update
 :payload_format  "json"}                   ;; defaults "json"
;; Fixed tcols: event_type(varchar), payload(json), received_at(varchar)

;; ── Conditionals (C) ── save-conditional [g id params]
;; Keys: :cond_type :branches :default_branch
{:cond_type       "if-else"                 ;; if-else|if-elif-else|multi-if|case|cond|pattern-match
 :branches        [{:condition "amount > 100" :group "high-value"}
                   {:condition "amount > 0" :group "normal"}]
 :default_branch  "unknown"}

;; ── Function / Logic (Fu) ── save-logic [g id params]
;; Keys: :fn_name :fn_params :fn_lets :fn_return :fn_outputs
{:fn_name    "calculateDiscount"
 :fn_params  [{:param_name "amount" :param_type "double" :source_column "orders.amount"}
              {:param_name "tier" :param_type "string" :source_column "users.tier"}]
 :fn_lets    [{:variable "rate" :expression "(if (= tier \"gold\") 0.2 0.1)"}
              {:variable "discount" :expression "(* amount rate)"}]
 :fn_return  "discount"
 :fn_outputs [{:output_name "discount_amount" :data_type "double"}]}
;; fn_outputs → tcols via logic-outputs->tcols

;; ── Union (U), Mapping (Mp), Target (Tg), Output (O) ──
;; Union: no required config
;; Mapping: {:mapping [{:source "old_col" :target "new_col" :transform "uppercase"}]}
;; Target: {:target_type "table" :target_name "output_orders" :write_mode "append"}
;; Output: {} (terminal node, always btype "O")
```

### 2.4 Edge Specification

```clojure
;; Simple edge: [from-ref, to-ref]
["ep1" "au1"]

;; Join edges: two edges into the same join node.
;; Order matters: the FIRST edge connected to a join becomes the LEFT table,
;; the SECOND becomes the RIGHT table. This matches how graph2 resolves
;; join parents positionally (first parent = left, second = right).
;;
;; Example: two tables joining
["table-users" "j1"]      ;; first → left
["table-orders" "j1"]     ;; second → right
```

**No `:port` metadata.** The current graph model stores edges as parent→child links without port annotations. Join side assignment is positional (order of edge creation). The compiler creates edges in the order they appear in `:edges`, and this determines left/right identity. The validator enforces that join nodes have exactly two incoming edges.

### 2.5 Patch Operations (for `intent: :patch`)

```clojure
;; Add a node to existing graph
{:op       :add-node
 :node     {<node-spec>}              ;; same as §2.2
 :after    "ep1"                      ;; existing node alias or :gil_ref or numeric id
 :before   "rb1"}                     ;; optional, connect before this node

;; Remove a node
{:op       :remove-node
 :ref      "au1"}                     ;; node alias, :gil_ref, or numeric id

;; Update node config
{:op       :update-config
 :ref      "ep1"
 :config   {:route_path "/v2/users/:id/orders"}}  ;; partial merge

;; Add edge
{:op       :add-edge
 :from     "fi1"
 :to       "p1"}

;; Remove edge
{:op       :remove-edge
 :from     "ep1"
 :to       "au1"}

;; Reorder (move node in chain)
{:op       :move-node
 :ref      "vd1"
 :after    "au1"
 :before   "dx1"}
```

### 2.6 Persisted Node Identity (`:gil_ref`)

**Problem**: GIL `:node-ref` is a document-local ID. After compilation, the graph stores nodes by numeric ID. A later `:patch` intent needs to reference nodes created by a previous `:build`, but numeric IDs are opaque.

**Solution**: The compiler persists `:node-ref` as a new attribute `:gil_ref` on each node's `:na` map:

```clojure
;; After compilation, node 5's :na contains:
{:name "get-users"
 :btype "Ep"
 :gil_ref "ep1"          ;; ← persisted from GIL :node-ref
 :http_method "GET"
 :route_path "/users/:id/orders"
 ...}
```

**Patch resolution order** (how `:ref` is resolved):
1. Match by `:gil_ref` (exact match on persisted GIL identity)
2. Match by `:name` / alias (fallback for manually-created nodes)
3. Match by numeric ID (explicit `"42"` string → integer)

**For NL→patch context**: When building the system prompt for a patch, the current graph's nodes are listed with their `:gil_ref` (if present) or `:name` + numeric ID, so the LLM knows what identities to reference.

---

## 3. Schema Registry (`bitool.gil.schema`)

The schema registry **wraps existing `graph2.clj` definitions** — it does not duplicate them.

```clojure
(ns bitool.gil.schema
  (:require [bitool.graph2 :as g2]))

;; ── Node Type Registry ──────────────────────────────────────────────

(def node-types
  "Map of human-readable type name → metadata.
   :btype        — graph2 btype code
   :source?      — true if this is a graph entry point (no parents needed)
   :terminal?    — true if this is a graph exit (Output, Target)
   :save-fn      — symbol of the graph2 save function (or nil for no-config nodes)
   :config-keys  — vector of keys the save function accepts
   :required-config — keys that MUST be present for the node to be functional"
  {"endpoint"         {:btype "Ep"  :source? true   :terminal? false
                       :save-fn 'g2/save-endpoint
                       :config-keys [:http_method :route_path :path_params :query_params
                                     :body_schema :response_format :description]
                       :required-config [:http_method :route_path]}
   "filter"           {:btype "Fi"  :source? false  :terminal? false
                       :save-fn nil  ;; save-filter or update-node with :sql
                       :config-keys [:sql]
                       :required-config []}
   "join"             {:btype "J"   :source? false  :terminal? false
                       :save-fn nil
                       :config-keys [:join_type :left_key :right_key]
                       :required-config [:join_type]}
   "aggregation"      {:btype "A"   :source? false  :terminal? false
                       :save-fn nil
                       :config-keys [:groupby :having]
                       :required-config [:groupby]}
   "sorter"           {:btype "S"   :source? false  :terminal? false
                       :save-fn nil
                       :config-keys [:sort_by]
                       :required-config [:sort_by]}
   "projection"       {:btype "P"   :source? false  :terminal? false
                       :save-fn nil
                       :config-keys [:columns]
                       :required-config []}
   "union"            {:btype "U"   :source? false  :terminal? false
                       :save-fn nil  :config-keys []  :required-config []}
   "mapping"          {:btype "Mp"  :source? false  :terminal? false
                       :save-fn nil
                       :config-keys [:mapping]
                       :required-config [:mapping]}
   "function"         {:btype "Fu"  :source? false  :terminal? false
                       :save-fn 'g2/save-logic
                       :config-keys [:fn_name :fn_params :fn_lets :fn_return :fn_outputs]
                       :required-config [:fn_name]}
   "conditionals"     {:btype "C"   :source? false  :terminal? false
                       :save-fn 'g2/save-conditional
                       :config-keys [:cond_type :branches :default_branch]
                       :required-config [:cond_type]}
   "table"            {:btype "T"   :source? true   :terminal? false
                       :save-fn nil  :config-keys []  :required-config []}
   "api-connection"   {:btype "Ap"  :source? true   :terminal? false
                       :save-fn nil  :config-keys []  :required-config []}
   "target"           {:btype "Tg"  :source? false  :terminal? false  ;; NOT terminal — Tg connects to O
                       :save-fn nil
                       :config-keys [:target_type :target_name :write_mode]
                       :required-config []}
   "output"           {:btype "O"   :source? false  :terminal? true
                       :save-fn nil  :config-keys []  :required-config []}

   ;; HTTP middleware nodes
   "auth"             {:btype "Au"  :source? false  :terminal? false
                       :save-fn 'g2/save-auth
                       :config-keys [:auth_type :token_header :secret :claims_to_cols]
                       :required-config [:auth_type]}
   "validator"        {:btype "Vd"  :source? false  :terminal? false
                       :save-fn 'g2/save-validator
                       :config-keys [:rules]
                       :required-config [:rules]}
   "rate-limiter"     {:btype "Rl"  :source? false  :terminal? false
                       :save-fn 'g2/save-rl
                       :config-keys [:max_requests :window_seconds :key_type :burst]
                       :required-config [:max_requests :window_seconds]}
   "cors"             {:btype "Cr"  :source? false  :terminal? false
                       :save-fn 'g2/save-cr
                       :config-keys [:allowed_origins :allowed_methods :allowed_headers
                                     :allow_credentials :max_age]
                       :required-config [:allowed_origins]}
   "logger"           {:btype "Lg"  :source? false  :terminal? false
                       :save-fn 'g2/save-lg
                       :config-keys [:log_level :fields_to_log :destination :format :external_url]
                       :required-config []}
   "cache"            {:btype "Cq"  :source? false  :terminal? false
                       :save-fn 'g2/save-cq
                       :config-keys [:cache_key :ttl_seconds :strategy]
                       :required-config [:ttl_seconds]}
   "circuit-breaker"  {:btype "Ci"  :source? false  :terminal? false
                       :save-fn 'g2/save-ci
                       :config-keys [:failure_threshold :reset_timeout :fallback_response]
                       :required-config [:failure_threshold]}
   "event-emitter"    {:btype "Ev"  :source? false  :terminal? false
                       :save-fn 'g2/save-ev
                       :config-keys [:topic :broker_url :key_template :format]
                       :required-config [:topic]}
   "response-builder" {:btype "Rb"  :source? false  :terminal? false
                       :save-fn 'g2/save-response-builder
                       :config-keys [:status_code :response_type :headers :template]
                       :required-config []}
   "db-execute"       {:btype "Dx"  :source? false  :terminal? false
                       :save-fn 'g2/save-dx
                       :config-keys [:connection_id :operation :sql_template :result_mode]
                       :required-config [:sql_template]}
   "scheduler"        {:btype "Sc"  :source? true   :terminal? false
                       :save-fn 'g2/save-sc
                       :config-keys [:cron_expression :timezone :params]
                       :required-config [:cron_expression]}
   "webhook"          {:btype "Wh"  :source? true   :terminal? false
                       :save-fn 'g2/save-wh
                       :config-keys [:webhook_path :secret_header :secret_value :payload_format]
                       :required-config [:webhook_path]}})

;; ── Edge Registry ───────────────────────────────────────────────────

(defn valid-edge?
  "Check if parent-type → child-type is a legal connection.
   Delegates to graph2/rectangles."
  [parent-type child-type]
  (let [parent-btype (get-in node-types [parent-type :btype])
        child-btype  (get-in node-types [child-type :btype])]
    (when (and parent-btype child-btype)
      (some #{child-btype} (get g2/rectangles parent-btype)))))

;; ── Enums (for LLM tool schema) ────────────────────────────────────

(def http-methods  ["GET" "POST" "PUT" "DELETE" "PATCH"])
(def auth-types    ["api-key" "bearer" "basic" "oauth2"])
(def join-types    ["inner" "left" "right" "full"])
(def cond-types    ["if-else" "if-elif-else" "multi-if" "case" "cond" "pattern-match"])
(def data-types    ["varchar" "integer" "double" "boolean" "date" "timestamp" "text" "json"])
(def vd-rule-types ["required" "min" "max" "min-length" "max-length" "regex" "one-of" "type"])
(def dx-operations ["SELECT" "INSERT" "UPDATE" "DELETE"])
(def resp-formats  ["json" "csv" "edn"])

(def schema-version "1.0.0")
(def gil-version "1.0")
```

---

## 4. Normalizer (`bitool.gil.normalize`)

Canonicalizes raw LLM output into consistent EDN.

```clojure
(ns bitool.gil.normalize
  (:require [bitool.gil.schema :as schema]
            [clojure.walk :as walk]))

(defn normalize [raw-gil]
  "Normalize raw GIL (JSON-parsed map with string keys) into canonical EDN.")
```

**Normalizations performed:**

| Input | Normalized |
|---|---|
| String keys everywhere | Keywordized recursively via `walk/keywordize-keys` |
| `"intent": "build"` | `:intent :build` |
| `"type": "ep"` or `"Ep"` | `:type "endpoint"` (via alias map) |
| Missing `:config` | `:config {}` |
| Missing `:position` | `:position nil` (auto-layout) |
| Missing `:alias` | `:alias` = `:type` + index |
| Missing `:gil-version` | `:gil-version "1.0"` |
| Missing `:intent` | `:intent :build` |
| `"nodes"` present + `"patches"` absent | `:intent :build` forced |
| `"patches"` present + `"nodes"` absent | `:intent :patch` forced |
| Nested config string keys | Keywordized (e.g., `"http_method"` → `:http_method`) |

**Type alias map** (accepts btype codes and abbreviations):

```clojure
(def type-aliases
  {"ep" "endpoint", "Ep" "endpoint",
   "fi" "filter",   "Fi" "filter",
   "au" "auth",     "Au" "auth",
   "vd" "validator","Vd" "validator",
   "rb" "response-builder", "Rb" "response-builder",
   "dx" "db-execute", "Dx" "db-execute",
   "rl" "rate-limiter", "Rl" "rate-limiter",
   "cr" "cors",     "Cr" "cors",
   "lg" "logger",   "Lg" "logger",
   "cq" "cache",    "Cq" "cache",
   "ci" "circuit-breaker", "Ci" "circuit-breaker",
   "ev" "event-emitter", "Ev" "event-emitter",
   "sc" "scheduler","Sc" "scheduler",
   "wh" "webhook",  "Wh" "webhook",
   "fu" "function", "Fu" "function",
   "p"  "projection","P" "projection",
   "j"  "join",     "J"  "join",
   "a"  "aggregation","A" "aggregation",
   "s"  "sorter",   "S"  "sorter",
   "u"  "union",    "U"  "union",
   "mp" "mapping",  "Mp" "mapping",
   "c"  "conditionals", "C" "conditionals",
   "t"  "table",    "T"  "table",
   "tg" "target",   "Tg" "target",
   "o"  "output",   "O"  "output"})
```

---

## 5. Validator (`bitool.gil.validator`)

Pure function. Returns `{:valid true/false :errors [...] :warnings [...]}`. Collects ALL errors.

```clojure
(ns bitool.gil.validator
  (:require [bitool.gil.schema :as schema]))

(defn validate [gil]
  "Validate normalized GIL. Returns {:valid bool :errors [...] :warnings [...]}.")
```

### 5.1 Validation Rules

**Structural checks:**
- `:gil-version` must be `"1.0"`
- `:intent` must be `:build` or `:patch`
- `:build` requires `:nodes` (non-empty) and `:edges`
- `:patch` requires `:patches` (non-empty)
- `:graph-name` required for `:build`
- `:build` must NOT have `:patches`; `:patch` must NOT have `:nodes`/`:edges`

**Node checks:**
- Every node has `:node-ref` (unique, non-empty string)
- Every node has `:type` that exists in `schema/node-types`
- No duplicate `:node-ref` values
- `:config` keys are subset of `:config-keys` for that type
- All `:required-config` keys present in `:config`
- Type-specific value validation:
  - `:http_method` ∈ `schema/http-methods`
  - `:auth_type` ∈ `schema/auth-types`
  - `:join_type` ∈ `schema/join-types`
  - `:cond_type` ∈ `schema/cond-types`
  - `:operation` ∈ `schema/dx-operations`
  - Validator `:rule` values ∈ `schema/vd-rule-types`
  - `:max_requests` > 0, `:window_seconds` > 0
  - `:ttl_seconds` > 0
  - `:route_path` starts with `/`
  - `:cron_expression` is valid 5-part expression

**Edge checks:**
- Every edge references valid `:node-ref`s
- Edge connection is legal per `schema/valid-edge?` (delegates to `graph2/rectangles`)
- No self-edges
- No duplicate edges
- Join nodes have exactly two incoming edges
- No cycles (topological sort succeeds)
- Every source node (`:source? true`) has at least one outgoing edge
- Every non-source, non-terminal node has at least one incoming edge
- Graph has exactly one Output node (btype "O") — BiTool always requires this

**Terminal node rule**: Every `:build` GIL must include exactly one node of type `"output"`. This matches BiTool's invariant that `createGraph` always starts with an Output node (id=1). Target nodes ("Tg") are intermediate — they write data but do not replace Output.

**Warnings (non-fatal):**
- Node with empty config (may work but likely unintended)
- Endpoint without auth or rate-limiter
- DB-execute without validator upstream
- Filter with empty SQL expression

**Patch-specific checks:**
- `:op` is valid
- `:ref` / `:after` / `:before` resolve to existing nodes (by `:gil_ref`, `:name`, or numeric ID)
- `:add-edge` creates a legal connection per rectangles
- `:update-config` keys valid for node type
- `:remove-node` warns if it orphans part of the graph

### 5.2 Error Format

```clojure
{:valid false
 :errors [{:code    :unknown-node-type
           :ref     "foo1"
           :message "Node 'foo1' has unknown type 'foo'. Valid types: endpoint, filter, ..."}
          {:code    :illegal-edge
           :from    "rb1"
           :to      "ep1"
           :message "Cannot connect response-builder → endpoint. response-builder can only connect to: output."}
          {:code    :missing-required-config
           :ref     "ep1"
           :key     :http_method
           :message "Endpoint node 'ep1' requires config key :http_method."}
          {:code    :join-needs-two-parents
           :ref     "j1"
           :message "Join node 'j1' has 1 incoming edge(s), needs exactly 2."}]
 :warnings [{:code    :no-auth
             :ref     "ep1"
             :message "Endpoint 'ep1' has no auth node. Consider adding auth for security."}]}
```

---

## 6. Compiler (`bitool.gil.compiler`)

Two modes: **plan** (dry-run, no DB) and **apply** (persists).

```clojure
(ns bitool.gil.compiler
  (:require [bitool.graph2 :as g2]
            [bitool.db :as db]
            [bitool.gil.schema :as schema]
            [bitool.endpoint :as endpoint]))
```

### 6.1 Plan Mode (dry-run)

Used by `POST /gil/compile`. Produces a mutation plan without touching the DB.

```clojure
(defn plan-gil [gil]
  "Produce an execution plan from validated GIL.
   Returns {:steps [...] :layout {ref → {:x :y}} :node-order [...]}
   No side effects."
  ...)
```

**Plan steps:**
1. Topological sort of nodes (sources first, Output last)
2. Auto-layout computation (for nodes without `:position`)
3. Ordered list of mutations:

```clojure
{:steps
 [{:action :create-graph :name "user-orders-api"}
  {:action :create-node :ref "ep1" :type "endpoint" :btype "Ep" :x 100 :y 100}
  {:action :create-node :ref "au1" :type "auth" :btype "Au" :x 300 :y 100}
  {:action :create-node :ref "rb1" :type "response-builder" :btype "Rb" :x 500 :y 100}
  ;; Output node already exists (id=1 from createGraph)
  {:action :create-edge :from "ep1" :to "au1"}
  {:action :create-edge :from "au1" :to "rb1"}
  {:action :create-edge :from "rb1" :to "o1"}
  ;; Configs saved in TOPOLOGICAL ORDER (sources first → downstream last)
  {:action :save-config :ref "ep1" :save-fn "save-endpoint" :config {...}}
  {:action :save-config :ref "au1" :save-fn "save-auth" :config {...}}
  {:action :save-config :ref "rb1" :save-fn "save-response-builder" :config {...}}]
 :layout {"ep1" {:x 100 :y 100} "au1" {:x 300 :y 100} ...}
 :node-order ["ep1" "au1" "rb1" "o1"]}
```

This plan is returned to the caller for preview. No DB state is modified.

### 6.2 Apply Mode (persist)

Used by `POST /gil/apply` and `POST /gil/from-nl`. Executes the plan.

```clojure
(defn apply-gil [gil session]
  "Execute validated GIL against the database.
   Returns {:graph-id :version :node-map {ref → id} :panel [...]}."
  ...)
```

**Execution steps for `:build`:**

**Step 1: Create graph shell**
```clojure
;; createGraph calls db/insertGraph internally — do NOT call insertGraph again.
(let [g   (g2/createGraph graph-name)   ;; returns persisted graph with :a {:id N :v 1}
      gid (get-in g [:a :id])]
  ;; Output node (id=1) already exists from createGraph.
  ;; gid is the persisted graph ID, used for all subsequent add-single-node calls.
  ...)
```

**Step 2: Create nodes** (topological order, skip "output" — already exists)

`add-single-node` calls `db/insertGraph` internally — each call persists.
After all nodes are created, we do ONE pass to stamp `:gil_ref` on every node
and persist that via `db/insertGraph`. This ensures `:gil_ref` survives even for
nodes with no save function (U, P, T, etc.).

```clojure
;; 2a: Create nodes (each add-single-node persists automatically)
(doseq [node-spec topo-sorted-nodes
        :when (not= (:type node-spec) "output")]
  (let [g-before (db/getGraph gid)
        g-after  (g2/add-single-node gid nil alias btype x y)
        new-id   (find-new-node-id g-before g-after)]
    ;; Store in node-map: {"ep1" → 5, "au1" → 6, ...}
    (swap! node-map-atom assoc (:node-ref node-spec) new-id)))

;; 2b: Stamp :gil_ref on ALL nodes (including output) in one pass.
;;     The output node's ref comes from the GIL spec (e.g. "out-main", "o1", etc.)
;;     — NOT hardcoded. We find it by looking for the node with :type "output".
(let [output-ref  (->> (:nodes gil) (filter #(= "output" (:type %))) first :node-ref)
      full-map    (merge @node-map-atom {output-ref 1})  ;; output is always graph node 1
      g           (db/getGraph gid)
      g           (reduce (fn [acc [ref id]]
                            (update-in acc [:n id :na] assoc :gil_ref ref))
                          g full-map)
      g           (db/insertGraph g)]
  ;; :gil_ref is now durably persisted for all nodes
  ...)
```

**Step 3: Create edges** (in declaration order — order matters for joins)
```clojure
(let [from-id (node-map from-ref)
      to-id   (node-map to-ref)]
  (g2/connect-single-node gid from-id to-id))
```

**Step 4: Save configs in topological order**

This is critical: save functions derive tcols from upstream state at save time. Saving in topological order ensures parents have their tcols populated before children read them.

```clojure
;; For each node in topological order (sources first → output last):
(let [save-fn (get-in schema/node-types [type :save-fn])
      g       (db/getGraph gid)]  ;; re-read for latest state
  (when (and save-fn (seq config))
    (let [updated (save-fn g node-id config)]
      (db/insertGraph updated))))
```

The save dispatch table uses **actual function references**:

```clojure
(def save-dispatch
  {"Ep" g2/save-endpoint
   "Au" g2/save-auth
   "Vd" g2/save-validator
   "Rl" g2/save-rl
   "Cr" g2/save-cr
   "Lg" g2/save-lg
   "Dx" g2/save-dx
   "Rb" g2/save-response-builder
   "Cq" g2/save-cq
   "Ci" g2/save-ci
   "Ev" g2/save-ev
   "Sc" g2/save-sc
   "Wh" g2/save-wh
   "C"  g2/save-conditional
   "Fu" g2/save-logic})
```

Nodes without entries in `save-dispatch` (T, O, U, P, J, A, S, Mp, Tg) are configured
via `update-node` + explicit `db/insertGraph` when they have non-empty config.
`update-node` is a pure in-memory merge — it does NOT persist by itself.

```clojure
;; For non-dispatched nodes with config:
(when (and (nil? save-fn) (seq config))
  (let [g (db/getGraph gid)
        g (update-node g node-id config)]
    (db/insertGraph g)))
```

**Step 5: Register endpoints** (if graph contains Ep/Wh nodes)

`register-endpoint!` reads `:btype` from its config arg to distinguish Ep from Wh,
so we must pass the full persisted node data (which includes `:btype`), not just
the raw GIL config.

```clojure
(let [g (db/getGraph gid)]
  (doseq [[ref id] @node-map-atom
          :let [btype (get-in g [:n id :na :btype])]
          :when (#{"Ep" "Wh"} btype)]
    ;; Pass the full :na map — it contains :btype, :route_path/:webhook_path, :http_method
    (endpoint/register-endpoint! gid id (get-in g [:n id :na]))))
```

### 6.3 Patch Apply

Operates on existing graph from session:

```clojure
(defn apply-patch [session patches]
  "Apply patch operations to existing graph.
   Returns {:graph-id :version :node-map :panel}."
  (let [gid (:gid session)
        g   (db/getGraph gid)]
    (reduce apply-single-patch g patches)))
```

**Node resolution** for `:ref`, `:after`, `:before`:
```clojure
(defn resolve-node-ref [g ref]
  "Resolve a patch ref to a numeric node-id.
   Tries: 1) :gil_ref match, 2) :name match, 3) numeric parse."
  (or (first (for [[id {:keys [na]}] (:n g)
                   :when (= ref (:gil_ref na))] id))
      (first (for [[id {:keys [na]}] (:n g)
                   :when (= ref (:name na))] id))
      (try (Integer/parseInt ref) (catch Exception _ nil))))
```

**Patch ops map to existing functions:**

| Patch op | Graph function(s) |
|---|---|
| `:add-node` | `g2/add-single-node` + `g2/connect-single-node` + optional save |
| `:remove-node` | Existing `/removeNode` logic (removes edges + node) |
| `:update-config` | `save-dispatch` by btype |
| `:add-edge` | `g2/connect-single-node` |
| `:remove-edge` | Remove from `:e` map directly |
| `:move-node` | Remove old edges + add new edges |

### 6.4 Auto-Layout Algorithm

For nodes without `:position`:

```
Source nodes:       x = 100, y = 100 + (i * 200)
Chain successors:   x = parent.x + 200, y = parent.y
Branch merge:       x = max(parent.x) + 200, y = avg(parent.y)
Output:             x = max(all.x) + 200, y = avg(all.y)
```

### 6.5 Compiler Output

```clojure
;; Plan mode (dry-run):
{:steps    [...]        ;; ordered mutation list
 :layout   {...}        ;; {ref → {:x :y}}
 :warnings [...]}       ;; from validator

;; Apply mode:
{:graph-id   42
 :version    3
 :node-map   {"ep1" 5, "au1" 6, "rb1" 8, "o1" 1}
 :panel      [...]      ;; panel items for UI
 :registered-endpoints [{:node-id 5 :method "GET" :path "/users/:id/orders"}]}
```

---

## 7. NL Translation (`bitool.gil.nl`)

### 7.1 LLM Integration

Ported from `medtronic-cicd/src/clj/bny/nl/llm.clj`:

```clojure
(ns bitool.gil.nl
  (:require [bitool.gil.schema :as schema]
            [bitool.gil.normalize :as normalize]
            [bitool.gil.validator :as validator]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(def temperatures [0 0.3 0.7])
(def max-retries 2)

(defn translate [nl-prompt & {:keys [existing-graph session]}]
  "Translate natural language to validated GIL.
   Returns {:gil <normalized> :validation {:valid true} :attempts N}
   Throws on persistent validation failure."
  (loop [attempt 0 errors []]
    (let [temp        (nth temperatures (min attempt (dec (count temperatures))))
          tool-schema (build-tool-schema existing-graph)
          messages    (build-messages nl-prompt errors existing-graph)
          response    (call-anthropic messages tool-schema temp)
          raw-gil     (extract-tool-input response)
          norm-gil    (normalize/normalize raw-gil)
          validation  (validator/validate norm-gil)]
      (if (:valid validation)
        {:gil norm-gil :validation validation :attempts (inc attempt)}
        (if (>= attempt max-retries)
          (throw (ex-info "Could not produce valid GIL after retries"
                          {:errors (:errors validation) :attempts (inc attempt)}))
          (recur (inc attempt) (:errors validation)))))))
```

### 7.2 Tool Schema

The tool schema is **dynamically generated** and uses **intent-conditional required fields**.

```clojure
(defn build-tool-schema [existing-graph]
  {:name "build_graph"
   :description "Build or modify a BiTool node graph."
   :input_schema
   {:type "object"
    :required ["intent"]    ;; only intent is always required
    :properties
    {:intent      {:type "string" :enum ["build" "patch"]}
     :graph_name  {:type "string" :description "Name for new graph (required for build)"}
     :description {:type "string"}
     :nodes {:type "array"
             :description "Node list (required for build, omit for patch)"
             :items {:type "object"
                     :required ["node_ref" "type"]
                     :properties
                     {:node_ref {:type "string"}
                      :type     {:type "string" :enum (vec (keys schema/node-types))}
                      :alias    {:type "string"}
                      :config   {:type "object"}}}}
     :edges {:type "array"
             :description "Edge list (required for build, omit for patch)"
             :items {:type "array" :items {:type "string"} :minItems 2 :maxItems 2}}
     :patches {:type "array"
               :description "Patch ops (required for patch, omit for build)"
               :items {:type "object"
                       :required ["op"]
                       :properties
                       {:op     {:type "string"
                                 :enum ["add-node" "remove-node" "update-config"
                                        "add-edge" "remove-edge" "move-node"]}
                        :ref    {:type "string"}
                        :node   {:type "object"}
                        :after  {:type "string"}
                        :before {:type "string"}
                        :config {:type "object"}
                        :from   {:type "string"}
                        :to     {:type "string"}}}}}}})
```

**Key fix**: Only `"intent"` is unconditionally required. The system prompt instructs the LLM that `nodes`+`edges` are required for `build` and `patches` is required for `patch`. The validator enforces this — not the JSON schema.

### 7.3 System Prompt

```text
You are a BiTool graph architect. You convert natural language descriptions
of APIs, data pipelines, and microservices into GIL (Graph Intent Language).

## Intents
- "build": Create a new graph. REQUIRES: graph_name, nodes, edges.
- "patch": Modify existing graph. REQUIRES: patches array.

## Node Types
{dynamically generated from schema/node-types with config-keys listed}

## Connection Rules
{dynamically generated from graph2/rectangles as readable text}
Example: endpoint can connect to → auth, validator, rate-limiter, cors, logger,
         circuit-breaker, cache, db-execute, join, union, projection, aggregation,
         sorter, filter, function, target, conditionals, response-builder, output

## Config Contracts (exact keys accepted by each node type)
endpoint:         http_method, route_path, path_params, query_params, body_schema, response_format, description
auth:             auth_type, token_header, secret, claims_to_cols
db-execute:       connection_id, operation, sql_template, result_mode
response-builder: status_code, response_type, headers, template
rate-limiter:     max_requests, window_seconds, key_type, burst
cors:             allowed_origins, allowed_methods, allowed_headers, allow_credentials, max_age
... {all types listed}

## Patterns

### REST API: endpoint → [auth] → [rate-limiter] → [validator] → [db-execute] → response-builder → output
### Data pipeline: table → [filter] → [join] → [projection] → [aggregation] → [sorter] → output
### Webhook: webhook → [auth] → [validator] → [db-execute] → [event-emitter] → response-builder → output

## Rules
1. Every graph MUST have exactly one "output" node.
2. Join nodes need exactly 2 incoming edges. First edge = left, second = right.
3. node_ref should be short: "ep1", "auth1", "fi-active", "rb1".
4. Config keys must match the exact names listed above.
5. For db-execute: use sql_template (not query), operation (not method).
6. For response-builder: use response_type (not content_type).
7. For auth: use token_header (not header), secret (not key).

{existing_graph_context — only for patch intent}
{few_shot_examples}
{previous_validation_errors — only on retry}
```

### 7.4 Few-Shot Examples

```clojure
(def examples
  [{:nl "Build a simple GET API that returns all users"
    :gil {:intent "build"
          :graph_name "get-users"
          :nodes [{:node_ref "ep1" :type "endpoint"
                   :config {:http_method "GET" :route_path "/users"
                            :response_format "json"}}
                  {:node_ref "rb1" :type "response-builder"
                   :config {:status_code 200 :response_type "json"}}
                  {:node_ref "o1" :type "output"}]
          :edges [["ep1" "rb1"] ["rb1" "o1"]]}}

   {:nl "Add bearer authentication to the API"
    :gil {:intent "patch"
          :patches [{:op "add-node"
                     :node {:node_ref "auth1" :type "auth"
                            :config {:auth_type "bearer" :token_header "Authorization"}}
                     :after "ep1"
                     :before "rb1"}]}}

   {:nl "Create an API that takes a user ID, looks up orders, filters active ones, returns JSON"
    :gil {:intent "build"
          :graph_name "user-orders-api"
          :nodes [{:node_ref "ep1" :type "endpoint"
                   :config {:http_method "GET" :route_path "/users/:id/orders"
                            :path_params [{:param_name "id" :data_type "integer"}]
                            :response_format "json"}}
                  {:node_ref "au1" :type "auth"
                   :config {:auth_type "api-key" :token_header "X-API-Key"}}
                  {:node_ref "dx1" :type "db-execute"
                   :config {:sql_template "SELECT * FROM orders WHERE user_id = :id"
                            :operation "SELECT"}}
                  {:node_ref "fi1" :type "filter"
                   :config {:sql "status = 'active'"}}
                  {:node_ref "p1" :type "projection"
                   :config {:columns ["order_id" "amount" "status" "created_at"]}}
                  {:node_ref "rb1" :type "response-builder"
                   :config {:status_code 200 :response_type "json"}}
                  {:node_ref "o1" :type "output"}]
          :edges [["ep1" "au1"] ["au1" "dx1"] ["dx1" "fi1"]
                  ["fi1" "p1"] ["p1" "rb1"] ["rb1" "o1"]]}}])
```

### 7.5 Context Injection for Patches

When a graph exists in session, the prompt includes:

```text
## Current Graph (ID: 42)
Nodes:
  - ep1 [endpoint] GET /users/:id/orders  (id=5, gil_ref="ep1")
  - au1 [auth] bearer  (id=6, gil_ref="au1")
  - dx1 [db-execute] SELECT * FROM orders...  (id=7, gil_ref="dx1")
  - rb1 [response-builder] 200 json  (id=8, gil_ref="rb1")
  - o1  [output]  (id=1)

Edges: ep1 → au1 → dx1 → rb1 → o1

Reference nodes by their gil_ref (ep1, au1, etc.) or by their name.
```

### 7.6 Validation Error Feedback (on retry)

```text
Your previous GIL had these errors:
  - [illegal-edge] Cannot connect response-builder → endpoint. response-builder can only connect to: output.
  - [missing-required-config] db-execute node 'dx1' requires config key :sql_template.

Fix these and try again. Use the exact config key names listed in the schema.
```

---

## 8. API Routes (`bitool.gil.api`)

```clojure
(ns bitool.gil.api
  (:require [bitool.gil.normalize :as normalize]
            [bitool.gil.validator :as validator]
            [bitool.gil.compiler :as compiler]
            [bitool.gil.nl :as nl]
            [bitool.db :as db]
            [ring.util.http-response :as http-response]))

;; POST /gil/validate — validate only, no side effects
(defn validate-handler [request]
  (let [raw-gil   (:params request)
        norm-gil  (normalize/normalize raw-gil)
        result    (validator/validate norm-gil)]
    (http-response/ok (assoc result :normalized norm-gil))))

;; POST /gil/compile — validate + plan, NO persistence
(defn compile-handler [request]
  (let [raw-gil    (:params request)
        norm-gil   (normalize/normalize raw-gil)
        validation (validator/validate norm-gil)]
    (if (:valid validation)
      (let [plan (compiler/plan-gil norm-gil)]  ;; pure function, no DB
        (http-response/ok {:valid true :plan plan :warnings (:warnings validation)}))
      (http-response/bad-request validation))))

;; POST /gil/apply — validate + compile + PERSIST
(defn apply-handler [request]
  (let [raw-gil    (:params request)
        norm-gil   (normalize/normalize raw-gil)
        validation (validator/validate norm-gil)]
    (if (:valid validation)
      (let [result  (compiler/apply-gil norm-gil (:session request))
            session (:session request)]
        (-> (http-response/ok result)
            (assoc :session (assoc session
                                  :gid (:graph-id result)
                                  :ver (:version result)))))
      (http-response/bad-request validation))))

;; POST /gil/from-nl — NL → GIL → validate → compile → persist (or preview)
(defn from-nl-handler [request]
  (let [{:keys [prompt apply]} (:params request)
        existing-graph (when-let [gid (get-in request [:session :gid])]
                         (db/getGraph gid))
        {:keys [gil validation]} (nl/translate prompt
                                               :existing-graph existing-graph
                                               :session (:session request))]
    (if apply
      (let [result  (compiler/apply-gil gil (:session request))
            session (:session request)]
        (-> (http-response/ok {:gil gil :validation validation :result result})
            (assoc :session (assoc session
                                  :gid (:graph-id result)
                                  :ver (:version result)))))
      ;; Preview mode: return GIL + plan without persisting
      (let [plan (compiler/plan-gil gil)]
        (http-response/ok {:gil gil :validation validation :plan plan})))))
```

### 8.1 Route Registration (in `home.clj`)

```clojure
["/gil"
 ["/validate" {:post gil-api/validate-handler}]
 ["/compile"  {:post gil-api/compile-handler}]
 ["/apply"    {:post gil-api/apply-handler}]
 ["/from-nl"  {:post gil-api/from-nl-handler}]]
```

---

## 9. Frontend Integration

### 9.1 AI Command Bar

```html
<div id="ai-command-bar" style="display:flex;gap:8px;align-items:center;">
  <input type="text" id="ai-prompt" placeholder="Describe what to build..."
         style="flex:1;padding:8px;border:1px solid #ccc;border-radius:4px;" />
  <button onclick="previewAICommand()">Preview</button>
  <button onclick="executeAICommand()">Build</button>
</div>
```

```javascript
// Preview: show what would be built without persisting
async function previewAICommand() {
  const prompt = document.getElementById('ai-prompt').value;
  if (!prompt.trim()) return;
  const result = await request('/gil/from-nl', {
    method: 'POST',
    body: { prompt, apply: false }
  });
  showGilPreview(result.gil, result.plan);  // modal with node/edge summary
}

// Build: actually create the graph
async function executeAICommand() {
  const prompt = document.getElementById('ai-prompt').value;
  if (!prompt.trim()) return;
  const result = await request('/gil/from-nl', {
    method: 'POST',
    body: { prompt, apply: true }
  });
  if (result.result?.panel) setPanelItems(result.result.panel);
  window.location.reload();
}
```

---

## 10. Testing Strategy

### 10.1 Golden Tests (Clojure)

```clojure
(ns bitool.gil.golden-test
  (:require [clojure.test :refer :all]
            [bitool.gil.normalize :as normalize]
            [bitool.gil.validator :as validator]
            [bitool.gil.compiler :as compiler]))

;; 1. Simple API: ep → rb → o
;; 2. Illegal edge rejected: rb → ep
;; 3. Full chain: ep → au → dx → fi → p → rb → o
;; 4. Data pipeline: t → fi → p → a → s → o
;; 5. Patch: add-node with :after/:before
;; 6. Patch: remove-node
;; 7. Patch: update-config
;; 8. Missing required config
;; 9. Duplicate node-ref
;; 10. Cycle detection
;; 11. Join with two parents (edge order = left/right)
;; 12. Webhook chain
;; 13. Conditional node
;; 14. Function node with fn_outputs
;; 15. Auto-layout (no positions)
;; 16. Type alias normalization ("Ep" → "endpoint")
;; 17. Output node required
;; 18. Config key mismatch (wrong key name → error)
;; 19. Topological save order (Ep saved before Rb)
;; 20. :gil_ref persisted after apply
```

### 10.2 JS Tests (existing pattern)

```javascript
section("GIL Schema");
assert(gilSchema.includes("node-types"), "defines node-types");
assert(gilSchema.includes("valid-edge?"), "delegates to rectangles");
assert(gilSchema.includes("save-rl"), "uses actual save-rl name");

section("GIL Validator");
assert(gilValidator.includes("defn validate"), "defines validate");
assert(gilValidator.includes(":join-needs-two-parents"), "validates join fan-in");

section("GIL Compiler");
assert(gilCompiler.includes("plan-gil"), "has plan mode (dry-run)");
assert(gilCompiler.includes("apply-gil"), "has apply mode (persist)");
assert(gilCompiler.includes("save-dispatch"), "uses save-dispatch table");
assert(gilCompiler.includes(":gil_ref"), "persists gil_ref on nodes");
```

---

## 11. Implementation Plan

### Phase 1: Foundation (schema + validator + compiler)
1. `src/clj/bitool/gil/schema.clj`
2. `src/clj/bitool/gil/normalize.clj`
3. `src/clj/bitool/gil/validator.clj`
4. `src/clj/bitool/gil/compiler.clj` (plan + apply)
5. `src/clj/bitool/gil/api.clj`
6. Routes in `home.clj`
7. Persist `:gil_ref` on node `:na` map
8. Golden tests (20 cases)

**Verification**: POST hand-written GIL to `/gil/apply`, verify graph in UI.

### Phase 2: NL Translation
1. `src/clj/bitool/gil/nl.clj`
2. Tool schema from registry
3. System prompt with examples
4. `/gil/from-nl` route
5. Frontend AI command bar
6. NL round-trip tests

**Verification**: Type "Build a GET API at /users" → graph appears.

### Phase 3: Patches
1. Patch validation rules
2. Patch compilation with `resolve-node-ref`
3. Context injection for existing graph
4. "Add auth to my API" → works

### Phase 4: Hardening
1. Audit trail (log NL prompt, GIL hash, graph version)
2. DB-backed example management + similarity selection
3. Rate limiting on `/from-nl`
4. GIL version migration support

---

## 12. File Structure

```
src/clj/bitool/gil/
├── schema.clj          ;; Node type registry, edge rules
├── normalize.clj       ;; Raw JSON → canonical EDN
├── validator.clj       ;; Pure validation → {:valid :errors :warnings}
├── compiler.clj        ;; plan-gil (dry-run) + apply-gil (persist)
├── nl.clj              ;; NL → GIL via Anthropic tool_use
└── api.clj             ;; HTTP handlers

test/clj/bitool/
├── gil_golden_test.clj ;; Normalize → validate → compile golden tests
└── gil_nl_test.clj     ;; NL translation tests

resources/public/
└── aiCommandBar.js     ;; Frontend AI command bar
```

---

## 13. Review Findings Addressed

### Round 1

| # | Finding | Resolution |
|---|---------|------------|
| 1 | Dry-run mutates state | Split into `plan-gil` (pure, no DB) and `apply-gil` (persists). `/compile` calls `plan-gil` only. §6.1 |
| 2 | Config shapes don't match graph2 | All configs now show exact `graph2` keys. Save dispatch uses actual function names (`save-rl`, `save-cr`, `save-dx`, etc.). §2.3, §6.2 |
| 3 | Patch identity not durable | `:node-ref` persisted as `:gil_ref` on node `:na` map. `resolve-node-ref` tries gil_ref → name → numeric ID. §2.6, §6.3 |
| 4 | Join port metadata lost | No `:port` metadata. Edge creation order determines left/right (matches graph2 positional model). Validator enforces exactly 2 incoming edges. §2.4 |
| 5 | Tool schema requires nodes+edges for patch | Only `"intent"` is unconditionally required in JSON schema. Intent-specific requirements enforced by validator, not schema. §7.2 |
| 6 | Config save order matters for tcols | Configs saved in topological order (sources first → downstream last). Each save re-reads graph for latest tcols. §6.2 step 4 |
| OQ1 | Terminal node ambiguity | Clarified: exactly one Output node ("O") required. Target ("Tg") is not terminal. §5.1 |
| OQ2 | Function names don't exist | All function references now use actual names from codebase. `save-dispatch` table maps btype → real function. §6.2 |

### Round 2

| # | Finding | Resolution |
|---|---------|------------|
| 1 | Double-persist in apply step 1 | `createGraph` already calls `db/insertGraph`. Removed redundant `db/insertGraph` call. §6.2 step 1 |
| 2 | `:gil_ref` not durable | Split into 2a (create nodes) + 2b (stamp `:gil_ref` on ALL nodes in one `db/insertGraph` pass). Covers nodes with no save function. §6.2 step 2 |
| 3 | Response-builder config shape wrong | `:headers` is now a JSON string (not map). `:template` is vector of `{:output_key :source_column}` maps (not nested map). §2.3 |
| 4 | Endpoint registration wrong payload | Now passes full persisted `:na` map (which includes `:btype`, `:route_path`, `:http_method`) instead of raw GIL config. §6.2 step 5 |
| 5 | Target marked terminal but shouldn't be | Changed `"target"` to `:terminal? false` in schema. Only Output ("O") is terminal. §3 |

### Round 3

| # | Finding | Resolution |
|---|---------|------------|
| 1 | Output ref hardcoded as `"o1"` | Now reads output node's `:node-ref` from the actual GIL spec. §6.2 step 2b |
| 2 | Non-dispatched node configs not persisted | Added explicit `update-node` + `db/insertGraph` for nodes without save functions (P, J, A, S, Mp, Tg, etc.). §6.2 step 4 |
| 3 | Scheduler params use `:name` not `:param_name` | Fixed to `:name` matching `sc-params->tcols` at graph2.clj:1277. §2.3 |
