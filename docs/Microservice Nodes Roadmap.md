# Microservice Nodes Roadmap

## Existing Nodes (Reusable for Microservices)

| Node | Btype | Microservice Role |
|------|-------|-------------------|
| Endpoint (Ep) | Ep | HTTP request entry point (source node) |
| Filter | Fi | Validate/filter request data |
| Projection | P | Select/rename response fields |
| Join | J | Enrich with related data |
| Union | U | Combine multiple data sources |
| Aggregation | A | Summarize data (COUNT, SUM, etc.) |
| Sort | S | Order response data |
| Function | Fu | Transform individual columns |
| Output | O | Terminal node (becomes HTTP response) |

---

## Phase 1: Core Request/Response (Priority: Immediate)

### 1. Response Builder (Rb)
- **Purpose**: Shape the HTTP response — set status code, headers, content type
- **Config**: Status code (200/201/400/404/500), custom headers, response format (JSON/XML/CSV)
- **Connects**: Any node → Rb → Output
- **Why first**: Without this, all responses are raw 200 OK with default JSON

### 2. Validator (Vd)
- **Purpose**: Validate incoming request data against rules
- **Config**: Per-column rules: required, type, min/max, regex, enum, custom expressions
- **Connects**: Ep → Vd → downstream
- **Behavior**: Returns 400 + validation errors if any rule fails

### 3. DB Execute (Dx)
- **Purpose**: Execute parameterized SQL against a connected database
- **Config**: SQL template with `{{column}}` placeholders, connection ref, operation (SELECT/INSERT/UPDATE/DELETE)
- **Connects**: Any node → Dx → downstream (results become columns)
- **Why needed**: Current Table node reads entire tables; Dx runs targeted queries with request params
- **Safety**: All `{{column}}` placeholders MUST compile to JDBC bind parameters (`?`), never raw string concatenation. Operations restricted to allow-listed SQL verbs (SELECT/INSERT/UPDATE/DELETE). No DDL (CREATE/DROP/ALTER). Template compilation validates at save-time that no raw interpolation is possible.

### 4. Error Handler (Eh)
- **Purpose**: Catch errors from upstream nodes and return structured error responses
- **Config**: Map error types to status codes, custom error templates, fallback behavior
- **Connects**: Wraps any node chain; intercepts errors before they reach Output
- **Graph semantics**: Eh has two outgoing edges — **success** (normal flow) and **error** (error flow). When any predecessor throws, execution jumps to the error edge. Eh does NOT modify tcols on the success path; on the error path it produces `{:column_name "error_type" ...}`, `{:column_name "error_message" ...}`, `{:column_name "status_code" ...}`

---

## Phase 2: Security & Middleware (Priority: High)

### 5. Auth (Au)
- **Purpose**: Authenticate requests
- **Config**: Auth type (JWT/API Key/Basic/OAuth2), token location (header/query/cookie), secret/key reference, claims extraction
- **Connects**: Ep → Au → downstream
- **Behavior**: Returns 401 if auth fails; extracts user info as columns (user_id, roles, etc.)

### 6. Rate Limiter (Rl)
- **Purpose**: Throttle requests per client/endpoint
- **Config**: Max requests, time window, key (IP/API key/user), burst allowance
- **Behavior**: Returns 429 Too Many Requests when exceeded
- **Graph semantics**: Inline pass-through node. Transparent to tcols — input tcols = output tcols. On limit exceeded, short-circuits to Output with 429 status. Does not branch; always single outgoing edge.

### 7. CORS (Cr)
- **Purpose**: Configure Cross-Origin Resource Sharing
- **Config**: Allowed origins, methods, headers, credentials, max age
- **Behavior**: Adds CORS headers, handles preflight OPTIONS requests
- **Graph semantics**: Inline pass-through node. Transparent to tcols. Adds response headers only — does not modify data flow. For preflight OPTIONS, short-circuits directly to Output with 204.

### 8. Logger (Lg)
- **Purpose**: Log request/response data for debugging and auditing
- **Config**: Log level, fields to log, destination (console/file/external), format
- **Connects**: Insertable anywhere in the chain
- **Graph semantics**: Inline pass-through node. Transparent to tcols — reads columns for logging but does not modify them. Side-effect only (writes to log sink). Single outgoing edge, never blocks or branches.

---

## Phase 3: Data Integration (Priority: Medium)

### 9. HTTP Call (Hc)
- **Purpose**: Make outbound HTTP requests to external APIs
- **Config**: URL template, method, headers, body mapping, response extraction, timeout, retry
- **Connects**: Any node → Hc → downstream (response fields become columns)
- **Difference from API node**: API is a source node; Hc is a mid-chain call that uses upstream columns as params

### 10. Cache (Ca)
- **Purpose**: Cache responses to reduce load
- **Config**: Cache key expression, TTL, invalidation rules, storage (in-memory/Redis)
- **Behavior**: Returns cached response if available; otherwise passes through and caches result

### 11. Transformer (Tr)
- **Purpose**: Complex data transformations beyond simple column functions
- **Config**: JSONPath/JMESPath expressions, map/reduce operations, data reshaping rules
- **Connects**: Any node → Tr → downstream

### 12. Conditional Branch (C) — Existing node, enhanced
- **Purpose**: Route data flow based on conditions (if/else for models)
- **Config**: Condition expression, true-path target, false-path target
- **Connects**: One input → two possible downstream paths
- **Why needed**: Different logic for admin vs. regular user, different DB queries based on params
- **Note**: Extends the existing `conditionals → "C"` btype already in the codebase

### 13. Loop/Iterator (Li)
- **Purpose**: Process collections — iterate over array items
- **Config**: Source array column, body subgraph reference, aggregation mode (collect/reduce)
- **Use case**: Batch operations, processing array request bodies

---

## Phase 4: Operations & Advanced (Priority: Future)

### 14. Queue Publisher (Qp)
- **Purpose**: Publish messages to message queues (Kafka, RabbitMQ, SQS)
- **Config**: Queue/topic, serialization format, partition key, async/sync

### 15. Queue Consumer (Qc)
- **Purpose**: Source node that receives messages from queues (event-driven microservices)
- **Config**: Queue/topic, consumer group, offset strategy, deserialization

### 16. WebSocket (Ws)
- **Purpose**: Bidirectional real-time communication endpoint
- **Config**: Events to handle, message format, connection lifecycle hooks

### 17. Circuit Breaker (Ci)
- **Purpose**: Protect downstream services from cascading failures
- **Config**: Failure threshold, reset timeout, fallback response, half-open behavior

### 18. Retry (Rt)
- **Purpose**: Retry failed operations with configurable strategy
- **Config**: Max retries, backoff (fixed/exponential), retryable errors, delay

### 19. Parallel (Pl)
- **Purpose**: Execute multiple downstream paths in parallel, merge results
- **Config**: Parallel branches, merge strategy (combine/race/all), timeout

### 20. Scheduler/Cron (Sc)
- **Purpose**: Source node that triggers on schedule (cron jobs, periodic tasks)
- **Config**: Cron expression, timezone, overlap policy

---

## Example Microservice Graphs

### REST API - User CRUD
```
GET /users/{id}
  Ep → Auth(JWT) → Validator → DB Execute(SELECT) → Projection → Response Builder(200) → Output

POST /users
  Ep → Auth(JWT) → Validator → DB Execute(INSERT) → Response Builder(201) → Output

PUT /users/{id}
  Ep → Auth(JWT) → Validator → DB Execute(UPDATE) → Response Builder(200) → Output

DELETE /users/{id}
  Ep → Auth(JWT) → DB Execute(DELETE) → Response Builder(204) → Output
```

### API Gateway Pattern
```
GET /api/orders/{id}
  Ep → Auth(JWT) → Rate Limiter → Cache →
    Parallel[
      HTTP Call(user-service) → Projection(user fields),
      HTTP Call(order-service) → Projection(order fields),
      HTTP Call(payment-service) → Projection(payment fields)
    ] → Transformer(merge) → Response Builder(200) → Output
```

### Event-Driven
```
Queue Consumer(order-events) → Conditional Branch(event_type) →
  [created]: DB Execute(INSERT audit) → HTTP Call(notification-service) → Logger → Output
  [cancelled]: DB Execute(UPDATE status) → Queue Publisher(refund-queue) → Logger → Output
```

---

## OpenAPI Spec → Auto-Graph Generation

Once nodes are built, an OpenAPI parser can auto-create complete graphs:

1. **Parse OpenAPI spec** → extract paths, methods, parameters, schemas, security
2. **For each operation**: create node chain via the platform HTTP API:
   - `POST /addSingle` with `alias=endpoint`, `btype=Ep` → creates an Ep node
   - `POST /connectSingle` with `source=<id>&target=<id>` → connects nodes
   - `POST /saveEndpoint` with method, path, params → configures the Ep node
3. **Map OpenAPI concepts to nodes**:
   - `paths["/users/{id}"].get` → Ep node (method=GET, path=/users/{id})
   - `securitySchemes.bearerAuth` → Auth node (type=JWT)
   - `parameters` → Validator node (rules from schema constraints)
   - `requestBody.schema` → Validator + Ep body_schema
   - `responses.200.schema` → Projection + Response Builder
   - `responses.4xx/5xx` → Error Handler

This becomes **IGL (Intermediate Graph Language)** — a declarative representation of microservice logic as a directed graph, analogous to ISL/IRL in compiler design.

**Route parameter syntax convention**: OpenAPI uses `{param}` (e.g. `/users/{id}`). Internally, BiTool stores routes using the same `{param}` format. The Ep component parses `{param}` segments to auto-populate path_params. When registering Ring routes at runtime, `{param}` is converted to Compojure `:param` syntax.

---

## IGL — Intermediate Graph Language

BiTool graphs function as an **Intermediate Graph Language (IGL)**, analogous to how compilers use intermediate representations:

### Compiler Analogy

| Compiler Layer | Compiler | BiTool |
|----------------|----------|--------|
| **Source** | High-level language (Java, C) | OpenAPI spec / UI canvas / DSL |
| **IR** | SSA / Three-address code | **IGL** — the graph `{:n {id {:na {:btype :params :tcols} :e {target {}}}}}` |
| **Target** | Machine code / bytecode | Ring handler chain (live HTTP microservice) |

### IGL Properties

1. **Serializable** — Graphs save/load from DB (EDN atom serialization)
2. **Composable** — Nodes connect with typed edges; columns (tcols) flow as typed data
3. **Optimizable** — Reorder independent nodes, cache intermediates, parallelize branches
4. **Importable** — OpenAPI spec → IGL (auto-generate graphs from API definitions)
5. **Exportable** — IGL → OpenAPI (generate API documentation from graphs)
6. **Versionable** — Graph snapshots = deployment versions; diff two versions to see changes
7. **Executable** — Graph traversal from source (Ep) to sink (Output) produces runtime behavior

### IGL Node Instruction Format

Each node in the graph is an "instruction" with:
- **Opcode**: btype (Ep, Fi, Vd, Au, Dx, P, Rb, O, ...)
- **Operands**: node-specific params (route_path, sql_template, auth_type, ...)
- **Input type**: tcols inherited from predecessors (column names + types)
- **Output type**: tcols passed to successors (potentially transformed)
- **Edges**: directed connections defining execution order

### Future: IGL Textual Notation

```
graph "user-api" {
  ep1 = Ep(GET, "/users/{id}", params=[id:integer])
  au1 = Auth(JWT, header="Authorization")
  vd1 = Validator(id: {required, min:1})
  dx1 = DbExecute(SELECT * FROM users WHERE id = {{id}})
  p1  = Projection(id, name, email)
  rb1 = ResponseBuilder(200, json)

  ep1 -> au1 -> vd1 -> dx1 -> p1 -> rb1 -> Output
}
```

This textual form could serve as an alternative to the visual canvas — both compile to the same IGL graph structure.
