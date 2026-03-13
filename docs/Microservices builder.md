# Plan: Add HTTP Endpoint (Ep) Node

## Context

BiTool currently builds data pipelines that **pull** data from sources (tables, APIs) through transformations to an Output node. To support microservice creation, we need an **Endpoint (Ep)** node that does the opposite: it **receives** HTTP requests and feeds request data into the graph as columns. This is the foundational node that turns a BiTool graph into an HTTP microservice.

The Ep node acts as a **source node** (like Table), where path params, query params, and request body fields become columns that flow downstream through filters, joins, projections, etc. The Output node becomes the HTTP response.

---

## Files to Modify

| # | File | Change |
|---|------|--------|
| 1 | `src/clj/bitool/graph2.clj` | Add btype, rectangles, node-keys, save/get functions |
| 2 | `src/clj/bitool/routes/home.clj` | Add route handler, update dispatchers |
| 3 | `resources/public/endpointComponent.js` | **New** - Web Component for config panel |
| 4 | `resources/public/endpointComponent.html` | **New** - HTML template for config panel |
| 5 | `resources/public/rectangleComponent.js` | Add Ep to `setComponentVisibility` switch |
| 6 | `resources/public/library/utils.js` | Add Ep to BTYPES, ICON, closeOpenedPanel |
| 7 | `resources/public/index.html` | Add toolbar button, component element, script tag |
| 8 | `src/clj/bitool/endpoint.clj` | **New** - Dynamic route registry for testing |
| 9 | `src/clj/bitool/handler.clj` | Wire dynamic endpoint handler |

---

## Step 1: Backend - Register Ep btype (`graph2.clj`)

### 1a. Add to `btype-codes` (line 31)
```clojure
"endpoint" "Ep"
```

### 1b. Add to `rectangles` (line 20)
Ep is a source node, can connect to same targets as Table:
```clojure
"Ep" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "O"]
```

### 1c. Add to `node-keys` (line 151)
```clojure
"endpoint" [:http_method :route_path :path_params :query_params :body_schema :response_format :description]
```

---

## Step 2: Backend - Ep node functions (`graph2.clj`)

### 2a. Column synthesis: `ep-params->tcols`
Converts endpoint parameters into standard column format (maps with `:column_name`, `:data_type`, `:is_nullable` — matching what `column_row_details` expects):
```clojure
(defn ep-params->tcols [node-id path-params query-params body-schema]
  (let [make-col (fn [name dtype nullable]
                   {:column_name name :data_type dtype :is_nullable (if nullable "YES" "NO")})
        path-cols  (mapv #(make-col (:param_name %) (or (:data_type %) "varchar") false) path-params)
        query-cols (mapv #(make-col (:param_name %) (or (:data_type %) "varchar") (not (:required %))) query-params)
        body-cols  (mapv #(make-col (:field_name %) (or (:data_type %) "varchar") (not (:required %))) body-schema)]
    {node-id (vec (concat path-cols query-cols body-cols))}))
```

### 2b. Save function: `save-endpoint`
Follows pattern of `save-api` (line 633) and `save-filter` (line 576):
```clojure
(defn save-endpoint [g id params]
  (let [path-params  (or (:path_params params) [])
        query-params (or (:query_params params) [])
        body-schema  (or (:body_schema params) [])
        tcols        (ep-params->tcols id path-params query-params body-schema)
        node-params  (select-keys params [:http_method :route_path :path_params
                                          :query_params :body_schema :response_format :description])]
    (-> g
        (update-node id (assoc node-params :tcols tcols)))))
```

### 2c. Get function: `get-endpoint-item`
Follows pattern of `get-item` (line 966):
```clojure
(defn get-endpoint-item [id g]
  (let [tmap (getData g id)
        name (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "items" (item_columns g (:tcols tmap))
           "http_method" (:http_method tmap)
           "route_path" (:route_path tmap)
           "path_params" (:path_params tmap)
           "query_params" (:query_params tmap)
           "body_schema" (:body_schema tmap)
           "response_format" (:response_format tmap)
           "description" (:description tmap))))
```

### 2d. Update `item_master` (line 889)
Add Ep case to the `case btype` block:
```clojure
"Ep" (merge item (select-keys tmap [:http_method :route_path :path_params
                                     :query_params :body_schema :response_format :description]))
```

---

## Step 3: Backend - Route handlers (`home.clj`)

### 3a. Update `get-btype` (line 92)
Add `"endpoint"` to the known operations set.

### 3b. Update `get-fx-from-btype` (line 272)
Add:
```clojure
"Ep" g2/get-endpoint-item
```

### 3c. Add save handler
```clojure
(defn save-endpoint [request]
  (let [params (:params request)
        id     (Integer. (:id params))
        g      (db/getGraph (:gid (:session request)))
        cp     (db/insertGraph (g2/save-endpoint g id params))
        rp     (g2/get-endpoint-item id cp)]
    (http-response/ok rp)))
```

### 3d. Register route in `home-routes` (line 543)
```clojure
["/saveEndpoint" {:post save-endpoint}]
```

---

## Step 4: Frontend - Utils (`library/utils.js`)

### 4a. Add to `closeOpenedPanel` panels array (line 41)
```javascript
"endpoint-component"
```

### 4b. Add to `BTYPES` in `getShortBtype` (line 75)
```javascript
"endpoint": "Ep"
```

### 4c. Add to `ICON` in `getBtypeIcon` (line 101)
```javascript
Ep: "&#9881;"   // gear icon
```

---

## Step 5: Frontend - Rectangle dispatch (`rectangleComponent.js`)

### 5a. Add case to `setComponentVisibility` (line 146)
After the `'api-connection'` case (line 173):
```javascript
case getShortBtype('endpoint'):
  document.querySelector("endpoint-component")?.setAttribute("visibility", "open");
  break;
```

---

## Step 6: Frontend - Index page (`index.html`)

### 6a. Add toolbar button (after line 80, before the scheduler button)
```html
<div class="tooltip">
  <button data-label="endpoint" data-conn_id="123" data-schema="schema"
    onclick="addRectangle('endpoint')">&#9881;</button>
  <span class="tooltiptext">Endpoint</span>
</div>
```

### 6b. Add component element (after line 182, after api-connection-component)
```html
<endpoint-component style="height: 100vh; width: 70%; position: fixed; top: 0;right: 0;background-color: white;">
</endpoint-component>
```

### 6c. Add script tag (after line 252, after apiConnectionComponent.js)
```html
<script type="module" src="endpointComponent.js"></script>
```

---

## Step 7: Frontend - Config panel (`endpointComponent.js` + `endpointComponent.html`) - NEW FILES

### 7a. `endpointComponent.html`
Panel layout with:
- **Header**: Save + Close buttons
- **HTTP Method**: Dropdown (GET/POST/PUT/DELETE/PATCH)
- **Route Path**: Text input with `{param}` syntax hint
- **Description**: Text input
- **Response Format**: Dropdown (JSON/CSV/EDN)
- **Path Parameters**: Table auto-parsed from route, columns: Name, Type, Description
- **Query Parameters**: Editable table, columns: Name, Type, Required, Default
- **Body Schema**: Editable table (visible only for POST/PUT/PATCH), columns: Field, Type, Required, Description
- **Test Section**: Send test request and view response

### 7b. `endpointComponent.js`
Web Component following `filterComponent.js` pattern:
- `class EndpointComponent extends HTMLElement`
- Shadow DOM with external template loading
- `observedAttributes: ["visibility"]`
- `StateManager` for dirty tracking
- Auto-parse `{param}` segments from route path into path_params table
- Toggle body schema visibility on method change
- Save via `request("/saveEndpoint", { method: "POST", body: ... })`

---

## Step 8: Backend - Dynamic route registry (`endpoint.clj`) - NEW FILE

Minimal registry for design-time testing:
- `endpoint-registry` atom: `{graph-id {node-id {:method :path :config}}}`
- `register-endpoint!` / `unregister-endpoint!` functions
- `execute-graph` stub: initially echoes received params (full graph traversal is a future enhancement)
- `build-handler`: creates Ring handler from registry entry
- `dynamic-endpoint-handler`: catch-all that checks registry for matching routes

Route prefix: `/api/v1/ep/<graph-id>/` to avoid conflicts with BiTool's own routes.

---

## Step 9: Backend - Wire dynamic handler (`handler.clj`)

Add the dynamic endpoint handler to the Ring routes chain (line 26) as a fallback before the default 404 handler:
```clojure
(fn [request]
  (when-let [resp (endpoint/dynamic-endpoint-handler request)]
    resp))
```

---

## Ep Node Data Structure

When stored in the graph:
```clojure
{:name "endpoint-users"
 :btype "Ep"
 :http_method "GET"
 :route_path "/users/{id}"
 :path_params [{:param_name "id" :data_type "integer" :description "User ID"}]
 :query_params [{:param_name "limit" :data_type "integer" :required false :default_value "10"}]
 :body_schema []   ;; populated for POST/PUT/PATCH
 :response_format "json"
 :description "Fetch user by ID"
 :tcols {<node-id> [{:column_name "id" :data_type "integer" :is_nullable "NO"}
                     {:column_name "limit" :data_type "integer" :is_nullable "YES"}]}
 :x 150 :y 150}
```

Parameters become columns via `ep-params->tcols`. Downstream nodes see these as regular table columns.

---

## Verification

1. **Backend**: Start app with `lein run`, create a new graph, call `POST /addSingle` with `alias=endpoint` to add an Ep node. Verify it appears in the graph.
2. **Get Item**: Call `GET /getItem?id=<ep-id>` and verify Ep-specific fields are returned.
3. **Save**: Call `POST /saveEndpoint` with method/path/params, verify graph persists and columns propagate.
4. **Frontend**: Open app in browser, click the Endpoint toolbar button, verify node appears on canvas. Click it and verify the config panel opens.
5. **Panel**: Fill in route path, verify path params are auto-parsed. Save and verify persistence.
6. **Downstream**: Connect Ep to a Filter node, verify Ep's columns appear in the Filter's column list.

---

---

## Known Gotchas (Reference)

These are hard-won lessons from the Ep implementation. Relevant to anyone building new nodes:

1. **tcols must be maps, not vectors**: `column_row_details` expects `{:column_name ... :data_type ... :is_nullable ...}`. Do not use 11-element vectors.

2. **Nested JSON needs manual keywordization**: `wrap-keyword-params` only keywordizes top-level request params. For nested arrays (e.g. `path_params`, `body_schema`), use a `kw-maps` helper to recursively keywordize.

3. **Orphan source nodes need explicit Output wiring**: When adding a child to a source node that has no outgoing edge, `find-edge` returns nil. Must explicitly wire `[parent → child]` and `[child → Output]`.

4. **Remove stale components before appending**: UI components like `expression-component` must be removed before re-appending, or old instances with stale column data persist in the DOM.

5. **Don't forget `update-table-cols`**: When connecting nodes programmatically, column propagation only happens if `update-table-cols` is called. Check that it's not commented out.
