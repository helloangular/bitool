# Node Tech Design — All Remaining Nodes

This document is the implementation guide for all 14 nodes not yet built. It follows the same format as `Microservices builder.md`. Each node section covers: purpose, data structure, backend changes (`graph2.clj` + `home.clj`), frontend changes, component UI, and verification.

**Source of truth for btype codes:** `src/clj/bitool/graph2.clj` — `btype-codes` map.

---

## Canonical btype-codes (current + planned)

```clojure
;; Existing
"table"           "T"
"join"            "J"
"filter"          "Fi"
"projection"      "P"
"aggregation"     "A"
"sorter"          "S"
"union"           "U"
"function"        "Fu"
"mapping"         "Mp"
"target"          "Tg"
"api-connection"  "Ap"
"graphql-builder" "Gq"
"conditionals"    "C"
"endpoint"        "Ep"
"response-builder" "Rb"
"output"          "O"

;; Phase 1 — Core microservice safety
"validator"       "Vd"
"auth"            "Au"

;; Phase 2 — Middleware / cross-cutting
"db-execute"      "Dx"
"rate-limiter"    "Rl"
"cors"            "Cr"
"logger"          "Lg"

;; Phase 3 — Advanced
"cache"           "Cq"
"event-emitter"   "Ev"
"circuit-breaker" "Ci"

;; Phase 3b — Blocked on edge-metadata schema extension (do NOT add until that lands)
;; "error-handler" "Eh"
;; "parallel"      "Px"

;; Phase 4 — Trigger sources
"scheduler"       "Sc"
"webhook"         "Wh"
```

---

## Rectangles (connection rules)

Add all of the following to the `rectangles` map in `graph2.clj`:

```clojure
;; Pass-through inline nodes — connect to any processing node
"Vd" ["Au" "Dx" "Rl" "Cr" "Lg" "Ci" "Cq" "Ev"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
"Au" ["Vd" "Dx" "Rl" "Cr" "Lg" "Ci" "Cq" "Ev"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
"Rl" ["Vd" "Au" "Dx" "Cr" "Lg" "Ci" "Cq" "Ev"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
"Cr" ["Vd" "Au" "Dx" "Rl" "Lg" "Ci" "Cq" "Ev"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
"Lg" ["Vd" "Au" "Dx" "Rl" "Cr" "Ci" "Cq" "Ev"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
"Ci" ["Vd" "Au" "Dx" "Rl" "Cr" "Lg" "Cq" "Ev"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
"Cq" ["Vd" "Au" "Dx" "Rl" "Cr" "Lg" "Ci" "Ev"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]

;; Data-producing nodes
"Dx" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "Lg" "O"]
"Ev" ["Rb" "O"]           ;; fire-and-forget, then continue to output

;; Blocked on edge-metadata schema extension — do NOT add until that lands:
;; "Eh" ["Rb" "O"]
;; "Px" ["J" "U" "P" "Fi" "Rb" "O"]

;; Source nodes (like Ep)
"Sc" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Dx" "Vd" "Lg" "O"]
"Wh" ["Vd" "Au" "Dx" "Rl" "Lg"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
```

Also update `Ep` rectangles to include new nodes:
```clojure
"Ep" ["Vd" "Au" "Rl" "Cr" "Lg" "Ci" "Cq" "Dx"
      "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
```

---

## `node-keys` additions (`graph2.clj` line 153)

```clojure
"validator"       [:rules]
"auth"            [:auth_type :token_header :claims_to_cols]
"db-execute"      [:connection_id :sql_template :operation :result_mode]
"transformer"     [:transformations]
"rate-limiter"    [:max_requests :window_seconds :key_type :burst]
"cors"            [:allowed_origins :allowed_methods :allowed_headers :allow_credentials :max_age]
"logger"          [:log_level :fields_to_log :destination :format]
"cache"           [:cache_key :ttl_seconds :strategy]
"event-emitter"   [:topic :broker_url :key_template :format]
"circuit-breaker" [:failure_threshold :reset_timeout :fallback_response]
"scheduler"       [:cron_expression :timezone :params]
"webhook"         [:webhook_path :secret_header :secret_value :payload_format]
;; Blocked on edge-metadata schema — do NOT add until that lands:
;; "error-handler" [:error_mappings :fallback_status]
;; "parallel"      [:branches :merge_strategy]
```

---

## `get-btype` set (`src/clj/bitool/routes/home.clj` line 94)

Add all new aliases to the set:
```
"validator" "auth" "db-execute" "transformer"
"rate-limiter" "cors" "logger" "cache"
"event-emitter" "circuit-breaker" "scheduler" "webhook"
```
Note: `"error-handler"` and `"parallel"` are **not** added until the edge-metadata schema extension lands.

---

## Icons (add to `getBtypeIcon` in `resources/public/library/utils.js`)

```javascript
Vd: "&#10003;",   // ✓ checkmark
Au: "&#128274;",  // 🔒 lock
Dx: "&#128190;",  // 💾 database
Eh: "&#9888;",    // ⚠ warning
Tr: "&#8646;",    // ⇶ transform arrows
Rl: "&#9203;",    // ⏳ hourglass
Cr: "&#127760;",  // 🌐 globe
Lg: "&#128220;",  // 📜 scroll
Px: "&#9889;",    // ⚡ parallel
Cq: "&#128268;",  // 🔌 cache
Ev: "&#128233;",  // 📩 event
Ci: "&#9855;",    // ⛟ circuit
Sc: "&#128337;",  // 🕑 clock
Wh: "&#127381;",  // 🔅 webhook
```

---
---

# Phase 1 — Core Safety Nodes

---

## 1. Validator (Vd)

### Purpose
Validates incoming column values against configurable rules before any business logic runs. Returns 422 with structured error details if any rule fails. Transparent to `tcols` on success.

### Data Structure in Graph
```clojure
{:name "validate-user-input"
 :btype "Vd"
 :rules [{:field "email"    :rule "regex"    :value "^[^@]+@[^@]+$" :message "Invalid email"}
         {:field "age"      :rule "min"      :value 18              :message "Must be 18+"}
         {:field "username" :rule "required" :value nil             :message "Username required"}
         {:field "status"   :rule "one-of"   :value ["active" "pending"] :message "Invalid status"}]
 :tcols {<parent-id> [{:column_name "email" :data_type "varchar" :is_nullable "NO"} ...]}
 :x 300 :y 150}
```

**Supported rules:**

| Rule | Config value | Description |
|------|-------------|-------------|
| `required` | nil | Field must be present and non-null |
| `min` | number | Numeric minimum |
| `max` | number | Numeric maximum |
| `min-length` | number | String minimum length |
| `max-length` | number | String maximum length |
| `regex` | pattern string | Must match regex |
| `one-of` | vector of values | Must be in set |
| `type` | "integer"/"varchar"/"boolean" | Type coercion check |

### Backend — `graph2.clj`

#### `btype-codes`
```clojure
"validator" "Vd"
```

#### `item_master` case
```clojure
"Vd" (assoc item "rules" (:rules tmap))
```

#### `save-validator`
```clojure
(defn save-validator [g id params]
  (let [kw-maps      (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        rules        (kw-maps (or (:rules params) []))
        parent-tcols (getTcols g id)
        g            (update-node g id {:rules rules :tcols parent-tcols})
        child-ids    (map second (find-edges g id))]
    (reduce (fn [acc cid]
              (assoc-in acc [:n cid :na :tcols] (getTcols acc cid)))
            g child-ids)))
```

#### `get-validator-item`
```clojure
(defn get-validator-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "rules"   (:rules tmap)
           "items"   (item_columns g (:tcols tmap)))))
```

### Backend — `home.clj`

#### Handler
```clojure
(defn save-validator [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        g       (db/getGraph (:gid (:session request)))
        updated (g2/save-validator g id params)
        cp      (db/insertGraph updated)
        rp      (g2/get-validator-item id cp)]
    (http-response/ok rp)))
```

#### Route
```clojure
["/saveValidator" {:post save-validator}]
```

#### `get-fx-from-btype`
```clojure
"Vd" g2/get-validator-item
```

### Frontend — `validatorComponent.html`

Panel sections:
- **Header**: Save + Close buttons
- **Rules table**: columns — Field, Rule, Value, Error Message, Remove
- **"+ Add Rule" button**
- **Rule type** is a dropdown: required / min / max / min-length / max-length / regex / one-of / type

### Frontend — `validatorComponent.js`

Key behaviour:
- Follows exact `endpointComponent.js` pattern (StateManager + EventHandler)
- State shape: `{ rules: [] }`
- `renderRules(rules)` — builds editable table rows
- `addRule()` — pushes `{field:"", rule:"required", value:"", message:""}` to state
- Save via `POST /saveValidator`

### Verification
1. Add Vd node after Ep, open panel, add a `required` rule on field `email`
2. Save — verify `rules` persists in graph
3. Connect Vd to Fi — verify Ep's columns appear in Fi (Vd is transparent to tcols)

---

## 2. Auth (Au)

### Purpose
Enforces authentication on inbound requests. Supports JWT (Bearer token), API Key (header/query), and Basic Auth. Extracts claims/identity and injects them as columns available to downstream nodes (e.g. `user_id`, `roles`).

### Data Structure in Graph
```clojure
{:name "jwt-auth"
 :btype "Au"
 :auth_type "jwt"               ;; "jwt" | "api-key" | "basic"
 :token_header "Authorization"  ;; Header to inspect
 :secret "-----BEGIN PUBLIC KEY-----..." ;; JWT public key or API key value
 :claims_to_cols [{:claim "sub"   :column "user_id"   :data_type "varchar"}
                  {:claim "roles" :column "user_roles" :data_type "varchar"}]
 :tcols {<parent-id> [{:column_name "user_id"    :data_type "varchar" :is_nullable "NO"}
                      {:column_name "user_roles"  :data_type "varchar" :is_nullable "YES"}]}
 :x 400 :y 150}
```

**Auth types:**

| Type | Token location | Validates |
|------|---------------|-----------|
| `jwt` | Authorization: Bearer `<token>` | Signature, expiry, issuer |
| `api-key` | Configurable header or `?api_key=` | Value equality |
| `basic` | Authorization: Basic `<base64>` | Username:password match |

**tcols behaviour:** Unlike other pass-through nodes, Au **adds** columns (extracted claims) to the `tcols` of its downstream nodes. Existing upstream columns are preserved.

### Backend — `graph2.clj`

#### `btype-codes`
```clojure
"auth" "Au"
```

#### `item_master` case
```clojure
"Au" (merge item (select-keys tmap [:auth_type :token_header :claims_to_cols]))
```

#### `save-auth`
```clojure
(defn save-auth [g id params]
  (let [kw-maps    (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        claims     (kw-maps (or (:claims_to_cols params) []))
        claim-cols (mapv (fn [c] {:column_name (:column c)
                                  :data_type   (or (:data_type c) "varchar")
                                  :is_nullable "YES"}) claims)
        ;; getTcols walks the graph to collect columns from ALL predecessors —
        ;; do NOT read (getData g id)'s own :tcols which may be stale or empty.
        parent-cols (vec (for [[_ v] (getTcols g id), item v] item))
        merged-cols {id (vec (concat parent-cols claim-cols))}
        node-params {:auth_type      (:auth_type params)
                     :token_header   (:token_header params)
                     :secret         (:secret params)
                     :claims_to_cols claims
                     :tcols          merged-cols}
        g           (update-node g id node-params)
        child-ids   (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))
```

#### `get-auth-item`
```clojure
(defn get-auth-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "auth_type"      (:auth_type tmap)
           "token_header"   (:token_header tmap)
           "claims_to_cols" (:claims_to_cols tmap)
           "items"          (item_columns g (:tcols tmap)))))
```

### Backend — `home.clj`

```clojure
(defn save-auth [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        g       (db/getGraph (:gid (:session request)))
        updated (g2/save-auth g id params)
        cp      (db/insertGraph updated)
        rp      (g2/get-auth-item id cp)]
    (http-response/ok rp)))
```

Route: `["/saveAuth" {:post save-auth}]`
`get-fx-from-btype`: `"Au" g2/get-auth-item`

### Frontend — `authComponent.html`

Panel sections:
- **Auth Type** dropdown: JWT / API Key / Basic
- **Token Header** text input (e.g. `Authorization`)
- **Secret / Key** textarea (JWT public key or API key value)
- **Claims to Columns** table: Claim Path | Output Column Name | Data Type | Remove
- **"+ Add Claim" button**
- Auth-type-specific sections shown/hidden via JS (same pattern as Ep body schema toggle)

### Verification
1. Add Au after Ep, configure JWT auth with `sub` → `user_id` claim
2. Save — verify `claims_to_cols` and `tcols` persists
3. Connect Au → Fi — verify Fi column list includes both Ep columns AND `user_id`

---
---

# Phase 2 — Middleware / Cross-Cutting Nodes

---

## 3. DB Execute (Dx)

### Purpose
Executes parameterized SQL against a registered database connection using upstream column values as bind parameters. Results become the new column set flowing downstream.

**Safety**: `{{col}}` placeholder-to-bind-param compilation (`?` substitution) happens at **execution time** in the runtime executor, not at save-time. What this design covers at save-time: (1) operation allowlist — only SELECT/INSERT/UPDATE/DELETE accepted, (2) DDL keyword rejection — regex rejects CREATE/DROP/ALTER/TRUNCATE/GRANT/REVOKE in the template. The executor layer (future implementation) is responsible for JDBC parameterization.

### Data Structure in Graph
```clojure
{:name "get-user-by-id"
 :btype "Dx"
 :connection_id 3                         ;; references db connection registry
 :operation "SELECT"                      ;; SELECT | INSERT | UPDATE | DELETE
 :sql_template "SELECT id, name, email FROM users WHERE id = {{user_id}} AND status = {{status}}"
 :result_mode "single"                    ;; "single" | "multiple" | "count"
 :tcols {<node-id> [{:column_name "id"    :data_type "integer" :is_nullable "NO"}
                    {:column_name "name"  :data_type "varchar" :is_nullable "NO"}
                    {:column_name "email" :data_type "varchar" :is_nullable "YES"}]}
 :x 500 :y 150}
```

**`result_mode`:**
- `single` — returns first row as flat columns
- `multiple` — returns rows; downstream nodes see array column
- `count` — returns `{:column_name "affected_rows" :data_type "integer"}`

**tcols for SELECT:** Derived from the SELECT column list parsed from `sql_template` at save-time. For INSERT/UPDATE/DELETE, tcols = `[{:column_name "affected_rows" :data_type "integer" :is_nullable "NO"}]`.

### Backend — `graph2.clj`

#### `save-dx`
```clojure
(defn parse-dx-tcols
  "Derive tcols from SELECT column list in sql_template.
   For non-SELECT, returns single affected_rows column."
  [id sql-template operation]
  (if (= "SELECT" (string/upper-case (or operation "SELECT")))
    (let [select-body (first (string/split
                               (string/replace sql-template #"(?i)^\s*select\s+" "")
                               #"(?i)\s+from\s+"))
          col-names   (map string/trim (string/split (or select-body "") #","))]
      {id (mapv #(hash-map :column_name % :data_type "varchar" :is_nullable "YES") col-names)})
    {id [{:column_name "affected_rows" :data_type "integer" :is_nullable "NO"}]}))

(defn validate-dx-sql [sql operation]
  (let [allowed #{"SELECT" "INSERT" "UPDATE" "DELETE"}
        op (string/upper-case operation)]
    (when-not (allowed op)
      (throw (ex-info "DB Execute: disallowed SQL operation" {:operation op})))
    (when (re-find #"(?i)(CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)" sql)
      (throw (ex-info "DB Execute: DDL not permitted in sql_template" {})))))

(defn save-dx [g id params]
  (let [sql       (:sql_template params)
        operation (or (:operation params) "SELECT")
        _         (validate-dx-sql sql operation)
        tcols     (parse-dx-tcols id sql operation)
        node-params {:connection_id (:connection_id params)
                     :operation     operation
                     :sql_template  sql
                     :result_mode   (or (:result_mode params) "single")
                     :tcols         tcols}
        g         (update-node g id node-params)
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))
```

#### `get-dx-item`
```clojure
(defn get-dx-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "connection_id" (:connection_id tmap)
           "operation"     (:operation tmap)
           "sql_template"  (:sql_template tmap)
           "result_mode"   (:result_mode tmap)
           "items"         (item_columns g (:tcols tmap)))))
```

### Backend — `home.clj`
Route: `["/saveDx" {:post save-dx}]`
`get-fx-from-btype`: `"Dx" g2/get-dx-item`

### Frontend — `dbExecuteComponent.html`

Panel sections:
- **Connection** dropdown (populated from `/getConn`)
- **Operation** dropdown: SELECT / INSERT / UPDATE / DELETE
- **SQL Template** large textarea — `{{column_name}}` syntax highlighted
- **Result Mode** dropdown (single / multiple / count) — shown only for SELECT
- **Detected Columns** read-only table — auto-populated after SQL parse on blur
- **Validate SQL** button — calls parse client-side and shows column preview

### Frontend — `dbExecuteComponent.js`

Key behaviour:
- On SQL textarea blur: parse SELECT column list and render preview
- On operation change: hide/show result_mode
- Save via `POST /saveDx`

### Verification
1. Add Dx, select connection, enter `SELECT id, name FROM users WHERE id = {{ep_id}}`
2. Save — verify tcols = `[{id, varchar} {name, varchar}]`
3. Connect Dx → Rb — verify Rb sees the two columns
4. Try entering `DROP TABLE users` — verify save is rejected

---

## 4. Error Handler (Eh)

### Purpose
Catches runtime errors from any upstream node and routes them to a structured error response. Has **two outgoing edges**: success (normal data flow) and error (error data flow). On error edge, replaces tcols with error columns.

### Data Structure in Graph
```clojure
{:name "handle-errors"
 :btype "Eh"
 :fallback_status "500"
 :error_mappings [{:error_type "validation-error" :status_code "422" :message_template "Validation failed: {{error_message}}"}
                  {:error_type "not-found"         :status_code "404" :message_template "Resource not found"}
                  {:error_type "auth-error"         :status_code "401" :message_template "Unauthorized"}]
 ;; Two edges stored in graph: [id success-child-id] and [id error-child-id]
 ;; edge metadata distinguishes them: {:edge-type "success"} / {:edge-type "error"}
 :tcols {<parent-id> <upstream-tcols>}  ;; success path: transparent
 ;; error path tcols always:
 ;; [{:column_name "error_type"    :data_type "varchar" :is_nullable "NO"}
 ;;  {:column_name "error_message" :data_type "varchar" :is_nullable "NO"}
 ;;  {:column_name "status_code"   :data_type "varchar" :is_nullable "NO"}]
 :x 600 :y 150}
```

**Graph semantics:**
- Success edge: Eh is transparent; passes upstream tcols unchanged
- Error edge: tcols = `[error_type, error_message, status_code]`

**Required graph schema extension (prerequisite — implement before Eh or Px):**

The current edge structure in `graph2.clj` stores edges as `{target-id {}}`. Eh and Px require per-edge metadata. The edge map must be extended to `{target-id {:edge-type "success"|"error"|"branch" :branch-id "b1"}}`.

Concrete changes needed in `graph2.clj`:

```clojure
;; 1. add-edge — multi-arity to accept optional metadata map (default {})
(defn add-edge
  ([g source target]      (add-edge g source target {}))
  ([g source target meta] (assoc-in g [:n source :e target] meta)))

;; 2. find-edges — return [source target meta] triples
(defn find-edges [g id]
  (for [[k v] (get-in g [:n id :e])]
    [id k v]))   ;; v = {:edge-type "success"} etc — previously always {}

;; 3. addRightClickNode in home.clj — pass {:edge-type "success"} when creating Eh children
;; 4. UI: Eh panel shows "Connect to: [Success Target] [Error Target]" —
;;    two separate connection slots rendered as labelled outgoing ports on the canvas rectangle
```

Until this schema extension is implemented, Eh and Px should **not** be added to `rectangles` or `btype-codes` to avoid broken graph states.

### Backend — `graph2.clj`

```clojure
(def eh-error-cols
  [{:column_name "error_type"    :data_type "varchar" :is_nullable "NO"}
   {:column_name "error_message" :data_type "varchar" :is_nullable "NO"}
   {:column_name "status_code"   :data_type "varchar" :is_nullable "NO"}])

(defn save-eh [g id params]
  (let [kw-maps        (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        error-mappings (kw-maps (or (:error_mappings params) []))
        node-params    {:fallback_status (:fallback_status params)
                        :error_mappings  error-mappings}]
    (update-node g id node-params)))

(defn get-eh-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "fallback_status" (:fallback_status tmap)
           "error_mappings"  (:error_mappings tmap)
           "items"           (item_columns g (:tcols tmap)))))
```

Route: `["/saveEh" {:post save-eh}]`
`get-fx-from-btype`: `"Eh" g2/get-eh-item`

### Frontend — `errorHandlerComponent.html`

Panel sections:
- **Fallback Status Code** input (default 500)
- **Error Mappings** table: Error Type | Status Code | Message Template | Remove
  - Message template supports `{{error_message}}` placeholder
- **"+ Add Mapping" button**
- **Info box**: explains dual-edge behaviour — success path is transparent, error path produces error columns

---

## 5. Rate Limiter (Rl)

### Purpose
Throttles inbound requests. Inline pass-through — does not modify tcols. Returns 429 on limit exceeded, short-circuiting to Output.

### Data Structure in Graph
```clojure
{:name "rate-limit"
 :btype "Rl"
 :max_requests 100
 :window_seconds 60
 :key_type "ip"          ;; "ip" | "api-key" | "user-id" | "header:<name>"
 :burst 20               ;; allowed burst above max_requests
 :tcols {<parent-id> <upstream-tcols>}  ;; transparent
 :x 350 :y 150}
```

### Backend — `graph2.clj`

```clojure
(defn save-rl [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:max_requests   (Integer. (or (:max_requests params) 100))
                             :window_seconds (Integer. (or (:window_seconds params) 60))
                             :key_type       (or (:key_type params) "ip")
                             :burst          (Integer. (or (:burst params) 0))
                             :tcols          parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-rl-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "max_requests"   (:max_requests tmap)
           "window_seconds" (:window_seconds tmap)
           "key_type"       (:key_type tmap)
           "burst"          (:burst tmap)
           "items"          (item_columns g (:tcols tmap)))))
```

Route: `["/saveRl" {:post save-rl}]`
`get-fx-from-btype`: `"Rl" g2/get-rl-item`

### Frontend — `rateLimiterComponent.html`

Panel sections:
- **Max Requests** number input
- **Window** number input + unit label (seconds)
- **Burst** number input
- **Key Type** dropdown: IP / API Key / User ID / Custom Header
- **Custom Header Name** text input (shown only when key_type = `header:<name>`)
- Info: "This node is pass-through — tcols are unchanged"

---

## 7. CORS (Cr)

### Purpose
Adds CORS response headers and handles preflight OPTIONS requests. Inline pass-through — tcols unchanged.

### Data Structure in Graph
```clojure
{:name "cors-policy"
 :btype "Cr"
 :allowed_origins ["https://app.example.com" "*"]
 :allowed_methods ["GET" "POST" "PUT" "DELETE" "OPTIONS"]
 :allowed_headers ["Content-Type" "Authorization" "X-Request-Id"]
 :allow_credentials false
 :max_age 86400
 :tcols {<parent-id> <upstream-tcols>}
 :x 300 :y 150}
```

### Backend — `graph2.clj`

```clojure
(defn save-cr [g id params]
  (let [str->vec     (fn [v] (if (string? v) (clojure.string/split v #",\s*") (or v [])))
        parent-tcols (getTcols g id)
        g (update-node g id {:allowed_origins   (str->vec (:allowed_origins params))
                             :allowed_methods   (str->vec (:allowed_methods params))
                             :allowed_headers   (str->vec (:allowed_headers params))
                             :allow_credentials (= "true" (str (:allow_credentials params)))
                             :max_age           (Integer. (or (:max_age params) 86400))
                             :tcols             parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-cr-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "allowed_origins"   (:allowed_origins tmap)
           "allowed_methods"   (:allowed_methods tmap)
           "allowed_headers"   (:allowed_headers tmap)
           "allow_credentials" (:allow_credentials tmap)
           "max_age"           (:max_age tmap))))
```

Route: `["/saveCr" {:post save-cr}]`
`get-fx-from-btype`: `"Cr" g2/get-cr-item`

### Frontend — `corsComponent.html`

Panel sections:
- **Allowed Origins** — editable tag list (one per line textarea)
- **Allowed Methods** — checkbox group: GET / POST / PUT / DELETE / PATCH / OPTIONS
- **Allowed Headers** — editable tag list
- **Allow Credentials** — toggle
- **Max Age** — number input (seconds)
- Info: "Pass-through — does not modify columns"

---

## 8. Logger (Lg)

### Purpose
Logs request data, column values, or response data at configurable points in the pipeline. Side-effect only — tcols are fully transparent.

### Data Structure in Graph
```clojure
{:name "audit-logger"
 :btype "Lg"
 :log_level "INFO"                ;; DEBUG | INFO | WARN | ERROR
 :fields_to_log ["user_id" "action" "amount"]  ;; empty = log all columns
 :destination "console"          ;; "console" | "file" | "external"
 :format "json"                   ;; "json" | "text"
 :external_url ""                 ;; used when destination = "external"
 :tcols {<parent-id> <upstream-tcols>}
 :x 400 :y 200}
```

### Backend — `graph2.clj`

```clojure
(defn save-lg [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:log_level     (or (:log_level params) "INFO")
                             :fields_to_log (or (:fields_to_log params) [])
                             :destination   (or (:destination params) "console")
                             :format        (or (:format params) "json")
                             :external_url  (:external_url params)
                             :tcols         parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-lg-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "log_level"     (:log_level tmap)
           "fields_to_log" (:fields_to_log tmap)
           "destination"   (:destination tmap)
           "format"        (:format tmap)
           "external_url"  (:external_url tmap)
           "items"         (item_columns g (:tcols tmap)))))
```

Route: `["/saveLg" {:post save-lg}]`
`get-fx-from-btype`: `"Lg" g2/get-lg-item`

### Frontend — `loggerComponent.html`

Panel sections:
- **Log Level** dropdown: DEBUG / INFO / WARN / ERROR
- **Fields to Log** — multi-select from upstream columns (empty = all)
- **Destination** dropdown: Console / File / External URL
- **External URL** text input (shown when destination = external)
- **Format** dropdown: JSON / Text
- Info: "Pass-through — does not modify columns"

---
---

# Phase 3 — Advanced Nodes

---

## 9. Parallel Executor (Px)

### Purpose
Fan-out: splits execution into N parallel branches, each running independently. Fan-in: merges results from all branches into a combined column set. Useful for concurrent DB lookups, parallel API calls, etc.

### Data Structure in Graph
```clojure
{:name "parallel-lookup"
 :btype "Px"
 :merge_strategy "merge"          ;; "merge" | "first" | "array"
 :branches [{:branch_id "b1" :label "User Lookup"}
            {:branch_id "b2" :label "Account Balance"}]
 ;; Each branch is a subgraph connected via Px's outgoing branch edges
 ;; Branch edges use metadata: {:edge-type "branch" :branch-id "b1"}
 :tcols {<node-id> [...merged columns from all branches...]}
 :x 600 :y 200}
```

**Merge strategies:**
- `merge` — all branch output columns combined (name collision = last-wins)
- `first` — only first completed branch's columns used
- `array` — each branch result becomes a nested array column

**Graph implementation note:** Px depends on the same edge-metadata schema extension described in the Eh section above (`{target-id {:edge-type "branch" :branch-id "b1"}}`). Each branch is a sequence of nodes that terminates at a virtual fan-in node (a new `Pj` — Parallel Join — internal btype, not user-facing). Px is the most complex node architecturally. Do not implement until the edge-metadata extension is merged and tested with Eh first.

### Backend — `graph2.clj`

```clojure
(defn save-px [g id params]
  (let [kw-maps (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        branches (kw-maps (or (:branches params) []))]
    (update-node g id {:merge_strategy (or (:merge_strategy params) "merge")
                       :branches       branches})))

(defn get-px-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "merge_strategy" (:merge_strategy tmap)
           "branches"       (:branches tmap)
           "items"          (item_columns g (:tcols tmap)))))
```

Route: `["/savePx" {:post save-px}]`
`get-fx-from-btype`: `"Px" g2/get-px-item`

### Frontend — `parallelComponent.html`

Panel sections:
- **Merge Strategy** dropdown: Merge / First / Array
- **Branches** list — each branch shows label, branch_id
- **"+ Add Branch" button**
- Info box: "Connect each branch output to the next node. All branches run concurrently."

---

## 10. Cache (Cq)

### Purpose
Implements read-through / write-through / cache-aside caching. Transparent to tcols. On cache hit, skips downstream nodes and returns cached result directly. On cache miss, executes pipeline and stores result.

### Data Structure in Graph
```clojure
{:name "user-cache"
 :btype "Cq"
 :cache_key "user:{{user_id}}"   ;; template using upstream columns
 :ttl_seconds 300
 :strategy "read-through"         ;; "read-through" | "write-through" | "aside"
 :tcols {<parent-id> <upstream-tcols>}
 :x 450 :y 200}
```

### Backend — `graph2.clj`

```clojure
(defn save-cq [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:cache_key   (or (:cache_key params) "")
                             :ttl_seconds (Integer. (or (:ttl_seconds params) 300))
                             :strategy    (or (:strategy params) "read-through")
                             :tcols       parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-cq-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "cache_key"   (:cache_key tmap)
           "ttl_seconds" (:ttl_seconds tmap)
           "strategy"    (:strategy tmap)
           "items"       (item_columns g (:tcols tmap)))))
```

Route: `["/saveCq" {:post save-cq}]`
`get-fx-from-btype`: `"Cq" g2/get-cq-item`

### Frontend — `cacheComponent.html`

Panel sections:
- **Cache Key Template** text input — `{{column_name}}` syntax
- **TTL** number input (seconds)
- **Strategy** dropdown: Read-Through / Write-Through / Cache-Aside
- **Available columns** helper list (click to insert `{{col}}` into key)
- Info: "Pass-through — does not modify columns"

---

## 11. Event Emitter (Ev)

### Purpose
Publishes a message to a Kafka topic or message queue using current column values to build the key and payload. Fire-and-forget — does not block the pipeline. tcols are transparent.

### Data Structure in Graph
```clojure
{:name "publish-order-event"
 :btype "Ev"
 :topic "orders.created"
 :broker_url "kafka://broker:9092"
 :key_template "{{order_id}}"
 :format "json"                   ;; "json" | "avro" | "edn"
 :tcols {<parent-id> <upstream-tcols>}
 :x 500 :y 250}
```

### Backend — `graph2.clj`

```clojure
(defn save-ev [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:topic        (or (:topic params) "")
                             :broker_url   (or (:broker_url params) "")
                             :key_template (or (:key_template params) "")
                             :format       (or (:format params) "json")
                             :tcols        parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-ev-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "topic"        (:topic tmap)
           "broker_url"   (:broker_url tmap)
           "key_template" (:key_template tmap)
           "format"       (:format tmap)
           "items"        (item_columns g (:tcols tmap)))))
```

Route: `["/saveEv" {:post save-ev}]`
`get-fx-from-btype`: `"Ev" g2/get-ev-item`

### Frontend — `eventEmitterComponent.html`

Panel sections:
- **Broker URL** text input
- **Topic** text input
- **Message Key Template** text input — `{{column}}` syntax
- **Format** dropdown: JSON / Avro / EDN
- **Columns available** helper list
- Info: "Fire-and-forget — pipeline continues after publish regardless of broker response"

---

## 12. Circuit Breaker (Ci)

### Purpose
Wraps downstream calls (typically Dx or Ap) to prevent cascading failures. Has three states: Closed (normal), Open (failing fast with fallback), Half-Open (testing recovery). Inline pass-through when Closed.

### Data Structure in Graph
```clojure
{:name "db-circuit-breaker"
 :btype "Ci"
 :failure_threshold 5             ;; failures before opening
 :reset_timeout 30                ;; seconds before half-open
 :fallback_response "{\"error\": \"service unavailable\"}"
 :tcols {<parent-id> <upstream-tcols>}
 :x 480 :y 150}
```

### Backend — `graph2.clj`

```clojure
(defn save-ci [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:failure_threshold (Integer. (or (:failure_threshold params) 5))
                             :reset_timeout     (Integer. (or (:reset_timeout params) 30))
                             :fallback_response (or (:fallback_response params) "{}")
                             :tcols             parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-ci-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "failure_threshold" (:failure_threshold tmap)
           "reset_timeout"     (:reset_timeout tmap)
           "fallback_response" (:fallback_response tmap)
           "items"             (item_columns g (:tcols tmap)))))
```

Route: `["/saveCi" {:post save-ci}]`
`get-fx-from-btype`: `"Ci" g2/get-ci-item`

### Frontend — `circuitBreakerComponent.html`

Panel sections:
- **Failure Threshold** number input
- **Reset Timeout** number input (seconds)
- **Fallback Response** JSON textarea
- **State indicator** (design-time shows "CLOSED" always; runtime shows actual state)
- Info: "Place before Dx or Ap nodes to protect against downstream failures"

---
---

# Phase 4 — Trigger Source Nodes

> **Runtime implementation status — Known Gap:** Sc and Wh config save/load/tcols propagation is fully implemented. Runtime execution is **not yet implemented** and requires the following additions:
>
> **Scheduler (Sc) — prerequisites:**
> - Add `clojurewerkz/quartzite` (or equivalent cron library) as a project dependency
> - New `scheduler.clj`: `schedule-graph!` / `unschedule-graph!` that register/deregister a Quartz job per Sc node
> - Job body: construct synthetic row `{:triggered_at <now> :job_id <uuid> :run_number <n>}`, merge extra params, feed into `execute-pipeline` (a future function that traverses the connected graph from the Sc node)
> - `save-sc` handler in `home.clj` must call `schedule-graph!` after `db/insertGraph`
> - On app startup, re-register all Sc nodes from all persisted graphs
>
> **Webhook (Wh) — prerequisites:**
> - `save-wh` handler must call `endpoint/register-endpoint!` with `method :post` and `(:webhook_path params)` after saving, mirroring the Ep pattern
> - `build-handler` in `endpoint.clj` needs a webhook variant that: (1) verifies HMAC signature before executing (see Wh HMAC section below), (2) extracts `event_type` from event-type headers (e.g. `X-GitHub-Event`), (3) constructs `{:event_type "..." :payload "<raw-body>" :received_at "<iso>"}` flat params and calls `execute-graph`
> - `dynamic-endpoint-handler` already dispatches POST requests; Wh nodes need only be registered into `endpoint-registry` to be active

---

## 13. Scheduler (Sc)

### Purpose
Source node (like Ep) that triggers graph execution on a cron schedule instead of HTTP. Injects schedule context columns (`triggered_at`, `job_id`, `run_number`) as the input column set.

### Data Structure in Graph
```clojure
{:name "daily-report-trigger"
 :btype "Sc"
 :cron_expression "0 8 * * MON-FRI"   ;; standard cron syntax
 :timezone "Europe/Istanbul"
 :params [{:name "report_date" :value "{{triggered_at|date}}" :data_type "varchar"}]
 :tcols {<node-id> [{:column_name "triggered_at" :data_type "varchar"   :is_nullable "NO"}
                    {:column_name "job_id"        :data_type "varchar"   :is_nullable "NO"}
                    {:column_name "run_number"    :data_type "integer"   :is_nullable "NO"}
                    {:column_name "report_date"   :data_type "varchar"   :is_nullable "YES"}]}
 :x 100 :y 200}
```

**tcols:** Always includes the three system columns (`triggered_at`, `job_id`, `run_number`) plus any user-defined params.

### Backend — `graph2.clj`

```clojure
(defn sc-params->tcols [id params]
  (let [system-cols [{:column_name "triggered_at" :data_type "varchar" :is_nullable "NO"}
                     {:column_name "job_id"        :data_type "varchar" :is_nullable "NO"}
                     {:column_name "run_number"    :data_type "integer" :is_nullable "NO"}]
        param-cols  (mapv #(hash-map :column_name (:name %)
                                     :data_type   (or (:data_type %) "varchar")
                                     :is_nullable "YES") params)]
    {id (vec (concat system-cols param-cols))}))

(defn save-sc [g id params]
  (let [kw-maps (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        sc-params (kw-maps (or (:params params) []))
        tcols     (sc-params->tcols id sc-params)
        node-params {:cron_expression (:cron_expression params)
                     :timezone        (or (:timezone params) "UTC")
                     :params          sc-params
                     :tcols           tcols}
        g         (update-node g id node-params)
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-sc-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "cron_expression" (:cron_expression tmap)
           "timezone"        (:timezone tmap)
           "params"          (:params tmap)
           "items"           (item_columns g (:tcols tmap)))))
```

Route: `["/saveSc" {:post save-sc}]`
`get-fx-from-btype`: `"Sc" g2/get-sc-item`

### Frontend — `schedulerComponent.html`

Panel sections:
- **Cron Expression** text input with human-readable preview (e.g. "Every weekday at 08:00")
- **Timezone** dropdown (common timezones)
- **Extra Params** table: Name | Value Template | Data Type | Remove
- **"+ Add Param" button**
- **System Columns** info box: `triggered_at`, `job_id`, `run_number` always injected
- **Next 5 runs** preview (computed client-side from cron expression)

---

## 14. Webhook (Wh)

### Purpose
Source node (like Ep) that registers an inbound webhook URL. When a POST arrives at that URL, its payload becomes the column set flowing through the graph. Supports HMAC signature verification.

### Data Structure in Graph
```clojure
{:name "github-webhook"
 :btype "Wh"
 :webhook_path "/webhooks/github"
 :secret_header "X-Hub-Signature-256"    ;; header to verify
 :secret_value "my-webhook-secret"       ;; HMAC secret
 :payload_format "json"                  ;; "json" | "form" | "xml"
 :tcols {<node-id> [{:column_name "event_type" :data_type "varchar" :is_nullable "YES"}
                    {:column_name "payload"     :data_type "json"    :is_nullable "NO"}
                    {:column_name "received_at" :data_type "varchar" :is_nullable "NO"}]}
 :x 100 :y 300}
```

**tcols:** Three system columns always injected (`event_type`, `payload`, `received_at`). Payload can be further decomposed by a downstream Tr node.

**Dynamic route registration:** Like Ep, Wh registers its path in `endpoint.clj`'s registry at graph activation time. Uses the same `dynamic-endpoint-handler` mechanism, restricted to POST only.

### Backend — `graph2.clj`

```clojure
(defn wh-tcols [id]
  {id [{:column_name "event_type" :data_type "varchar" :is_nullable "YES"}
       {:column_name "payload"    :data_type "json"    :is_nullable "NO"}
       {:column_name "received_at" :data_type "varchar" :is_nullable "NO"}]})

(defn save-wh [g id params]
  (let [tcols       (wh-tcols id)
        node-params {:webhook_path  (:webhook_path params)
                     :secret_header (:secret_header params)
                     :secret_value  (:secret_value params)
                     :payload_format (or (:payload_format params) "json")
                     :tcols         tcols}
        g           (update-node g id node-params)
        child-ids   (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-wh-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "webhook_path"   (:webhook_path tmap)
           "secret_header"  (:secret_header tmap)
           "payload_format" (:payload_format tmap)
           "items"          (item_columns g (:tcols tmap)))))
```

Route: `["/saveWh" {:post save-wh}]`
`get-fx-from-btype`: `"Wh" g2/get-wh-item`

### Frontend — `webhookComponent.html`

Panel sections:
- **Webhook Path** text input (e.g. `/webhooks/github`)
- **Signature Header** text input (e.g. `X-Hub-Signature-256`)
- **Secret** password input (stored as-is in graph atom — **TODO**: encrypt at rest before production use)
- **Payload Format** dropdown: JSON / Form / XML
- **Registered URL** read-only display: `https://your-host/api/v1/ep/<graph-id><webhook_path>`
- **System Columns** info box: `event_type`, `payload`, `received_at` always injected

---
---

# Implementation Checklist (per node)

> **Exception — Eh (Error Handler) and Px (Parallel Executor):** Steps 1–3 (btype-codes, rectangles, node-keys) and step 8 (get-btype set) **must be skipped** until the edge-metadata schema extension (`add-edge` multi-arity + `find-edges` triples) is implemented. All other steps can be done in advance.

For each node, the following locations must be updated:

| # | File (full path) | Change |
|---|-----------------|--------|
| 1 | `src/clj/bitool/graph2.clj` | Add to `btype-codes` |
| 2 | `src/clj/bitool/graph2.clj` | Add to `rectangles` |
| 3 | `src/clj/bitool/graph2.clj` | Add to `node-keys` |
| 4 | `src/clj/bitool/graph2.clj` | Add case to `item_master` |
| 5 | `src/clj/bitool/graph2.clj` | Add `save-<node>` function |
| 6 | `src/clj/bitool/graph2.clj` | Add `get-<node>-item` function |
| 7 | `src/clj/bitool/routes/home.clj` | Add alias to `get-btype` set |
| 8 | `src/clj/bitool/routes/home.clj` | Add case to `get-fx-from-btype` |
| 9 | `src/clj/bitool/routes/home.clj` | Add `save-<node>` handler |
| 10 | `src/clj/bitool/routes/home.clj` | Add route to `home-routes` |
| 11 | `resources/public/library/utils.js` | Add to `closeOpenedPanel` panels array |
| 12 | `resources/public/library/utils.js` | Add to `BTYPES` in `getShortBtype` |
| 13 | `resources/public/library/utils.js` | Add to `ICON` in `getBtypeIcon` |
| 14 | `resources/public/rectangleComponent.js` | Add case to `setComponentVisibility` |
| 15 | `resources/public/index.html` | Add toolbar button |
| 16 | `resources/public/index.html` | Add `<x-component>` element |
| 17 | `resources/public/index.html` | Add `<script>` import |
| 18 | `resources/public/<node>Component.js` | **New file** — Web Component |
| 19 | `resources/public/<node>Component.html` | **New file** — Panel template |

---

# Known Gotchas (Applies to All Nodes)

1. **tcols must be maps** — `{:column_name ... :data_type ... :is_nullable ...}`. Never vectors.
2. **Nested JSON arrays need `kw-maps`** — `wrap-keyword-params` only keywordizes top-level. Any nested array (rules, transformations, branches) needs explicit keywordization.
3. **Propagate tcols to children** — After `update-node`, always reduce over `(find-edges g id)` and `assoc-in` new tcols to each child. Otherwise downstream nodes don't see column changes.
4. **Orphan nodes need Output wiring** — If source node has no outgoing edge, `find-edge` returns nil. Wire `[parent → child]` and `[child → Output]` explicitly in `addSpecialNode`.
5. **Remove stale UI components** — Before appending expression-components or table rows in `open()`, clear existing ones first.
6. **StateManager `current` not `currentState`** — Access via `.current` getter.
7. **`getShortBtype` and `btype-codes` must stay in sync** — Any btype added to `graph2.clj` must also be added to `BTYPES` in `utils.js`.
8. **Pass-through nodes still need `tcols` stored** — Even if Rl/Cr/Lg/Ci don't modify columns, `save-*` must copy parent tcols into the node so downstream `getTcols` works correctly.
9. **Dual-edge nodes (Eh, Px) need graph schema extension** — The current `{target-id {}}` edge structure must carry `{:edge-type "success"/"error"/"branch"}` metadata. Plan this before implementing Eh or Px.
10. **Route param syntax is `{param}` internally** — Converts to `:param` for Compojure/Ring at runtime registration in `endpoint.clj`.
