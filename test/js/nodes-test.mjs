/**
 * BiTool Node UI Tests — Node.js version
 *
 * Runs without a browser. Reads source files from disk and verifies:
 * - All btype codes, icons, and aliases are consistent
 * - All component JS files follow correct patterns (no this.nodeName, proper lifecycle)
 * - All component HTML templates exist with required elements
 * - Toolbar buttons, script tags, and component elements present in index.html
 * - Backend save routes, item_master cases, node-keys, rectangles all complete
 *
 * Run: node test/js/nodes-test.mjs
 */

import { readFileSync, existsSync } from "fs";
import { join } from "path";

const ROOT = join(new URL(import.meta.url).pathname, "..", "..", "..");

let passed = 0;
let failed = 0;

function read(relPath) {
  return readFileSync(join(ROOT, relPath), "utf-8");
}

function section(name) {
  console.log(`\n--- ${name} ---`);
}

function assert(cond, msg) {
  if (cond) {
    passed++;
    console.log(`  \x1b[32mPASS\x1b[0m: ${msg}`);
  } else {
    failed++;
    console.log(`  \x1b[31mFAIL\x1b[0m: ${msg}`);
  }
}

function assertEqual(a, b, msg) {
  assert(a === b, `${msg} — expected "${b}", got "${a}"`);
}

// =========================================================================
// Constants
// =========================================================================
const expectedBtypes = {
  "function": "Fu", "aggregation": "A", "sorter": "S", "union": "U",
  "mapping": "Mp", "filter": "Fi", "table": "T", "join": "J",
  "projection": "P", "target": "Tg", "api-connection": "Ap",
  "conditionals": "C", "endpoint": "Ep", "response-builder": "Rb",
  "validator": "Vd", "auth": "Au", "db-execute": "Dx",
  "rate-limiter": "Rl", "cors": "Cr", "logger": "Lg",
  "cache": "Cq", "event-emitter": "Ev", "circuit-breaker": "Ci",
  "scheduler": "Sc", "webhook": "Wh", "output": "O"
};

const newNodeAliases = [
  "endpoint", "response-builder", "validator", "auth", "db-execute",
  "rate-limiter", "cors", "logger", "cache", "event-emitter",
  "circuit-breaker", "scheduler", "webhook"
];

const componentMap = {
  "endpoint":         "endpoint-component",
  "response-builder": "response-builder-component",
  "validator":        "validator-component",
  "auth":             "auth-component",
  "db-execute":       "db-execute-component",
  "rate-limiter":     "rate-limiter-component",
  "cors":             "cors-component",
  "logger":           "logger-component",
  "cache":            "cache-component",
  "event-emitter":    "event-emitter-component",
  "circuit-breaker":  "circuit-breaker-component",
  "scheduler":        "scheduler-component",
  "webhook":          "webhook-component"
};

const componentFiles = [
  "authComponent.js", "dbExecuteComponent.js", "rateLimiterComponent.js",
  "corsComponent.js", "loggerComponent.js", "cacheComponent.js",
  "eventEmitterComponent.js", "circuitBreakerComponent.js",
  "endpointComponent.js", "responseBuilderComponent.js",
  "validatorComponent.js", "schedulerComponent.js", "webhookComponent.js"
];

// =========================================================================
// 1. BTYPE CODES in utils.js
// =========================================================================
section("getShortBtype — btype map in utils.js");

const utilsSrc = read("resources/public/library/utils.js");

for (const [alias, code] of Object.entries(expectedBtypes)) {
  assert(utilsSrc.includes(`"${alias}": "${code}"`),
    `utils.js BTYPES: "${alias}" -> "${code}"`);
}

// =========================================================================
// 2. BTYPE ICONS in utils.js
// =========================================================================
section("getBtypeIcon — icons in utils.js");

const iconBtypes = [
  "Fi", "Fu", "J", "A", "U", "S", "Mp", "Ap", "C", "P",
  "Ep", "Rb", "Vd", "Au", "Dx", "Rl", "Cr", "Lg", "Cq", "Ev", "Ci", "Sc", "Wh"
];
for (const bt of iconBtypes) {
  assert(utilsSrc.includes(`${bt}:`), `utils.js ICON: has "${bt}" entry`);
}

// =========================================================================
// 3. closeOpenedPanel — has all component tags and is null-safe
// =========================================================================
section("closeOpenedPanel — completeness and null safety");

for (const tag of Object.values(componentMap)) {
  assert(utilsSrc.includes(`"${tag}"`), `closeOpenedPanel panels includes "${tag}"`);
}
// Check null-safe pattern
assert(utilsSrc.includes("const el = document.querySelector(componentName)"),
  "closeOpenedPanel uses null-safe querySelector");
assert(utilsSrc.includes("if (el &&"),
  "closeOpenedPanel checks el before getAttribute");

// =========================================================================
// 4. Component JS — no this.nodeName conflict
// =========================================================================
section("Component JS — no this.nodeName (HTMLElement conflict)");

for (const file of componentFiles) {
  const src = read(`resources/public/${file}`);
  assert(!/this\.nodeName\s*=/.test(src),
    `${file}: no "this.nodeName =" assignment`);
}

// =========================================================================
// 5. Component JS — structural correctness
// =========================================================================
section("Component JS — structural checks");

for (const file of componentFiles) {
  const src = read(`resources/public/${file}`);

  assert(src.includes('observedAttributes'), `${file}: has observedAttributes`);
  assert(src.includes('"visibility"'), `${file}: observes "visibility"`);
  assert(/close\(\)\s*\{[^}]*display\s*=\s*"none"/.test(src),
    `${file}: close() sets display=none`);
  assert(src.includes('loadTemplate'), `${file}: has loadTemplate()`);
  assert(src.includes('customElements.define'), `${file}: registers custom element`);

  // The 8 newer components (auth through circuit-breaker) should not have these issues.
  // Older components (endpoint, response-builder, validator, scheduler, webhook) may differ.
  const newerComponents = [
    "authComponent.js", "dbExecuteComponent.js", "rateLimiterComponent.js",
    "corsComponent.js", "loggerComponent.js", "cacheComponent.js",
    "eventEmitterComponent.js", "circuitBreakerComponent.js"
  ];
  if (newerComponents.includes(file)) {
    // loadTemplate must NOT call this.close()
    const ltMatch = src.match(/async\s+loadTemplate\(\)\s*\{([\s\S]*?)\n\s{2}\}/);
    if (ltMatch) {
      assert(!ltMatch[1].includes('this.close()'),
        `${file}: loadTemplate() does not call this.close()`);
    }
    // close() must NOT call removeGroup
    const closeMatch = src.match(/close\(\)\s*\{([\s\S]*?)\n\s{2}\}/);
    if (closeMatch) {
      assert(!closeMatch[1].includes('removeGroup'),
        `${file}: close() does not call removeGroup`);
    }
  }
}

// =========================================================================
// 6. Component HTML templates exist with required elements
// =========================================================================
section("Component HTML — existence and structure");

for (const file of componentFiles) {
  const htmlFile = file.replace(".js", ".html");
  const htmlPath = join(ROOT, "resources/public", htmlFile);
  assert(existsSync(htmlPath), `${htmlFile} exists`);
  if (existsSync(htmlPath)) {
    const html = readFileSync(htmlPath, "utf-8");
    const hasSave = html.includes('id="saveBtn"') || html.includes('id="saveButton"');
    const hasClose = html.includes('id="closeBtn"') || html.includes('id="closeButton"');
    assert(hasSave, `${htmlFile}: has save button`);
    assert(hasClose, `${htmlFile}: has close button`);
  }
}

// =========================================================================
// 7. Save routes — each component POSTs to the correct endpoint
// =========================================================================
section("Component JS — save route correctness");

const saveRoutes = {
  "authComponent.js":           "/saveAuth",
  "dbExecuteComponent.js":      "/saveDx",
  "rateLimiterComponent.js":    "/saveRl",
  "corsComponent.js":           "/saveCr",
  "loggerComponent.js":         "/saveLg",
  "cacheComponent.js":          "/saveCq",
  "eventEmitterComponent.js":   "/saveEv",
  "circuitBreakerComponent.js": "/saveCi",
  "endpointComponent.js":       "/saveEndpoint",
  "responseBuilderComponent.js":"/saveResponseBuilder",
  "validatorComponent.js":      "/saveValidator",
  "schedulerComponent.js":      "/saveSc",
  "webhookComponent.js":        "/saveWh",
};

for (const [file, route] of Object.entries(saveRoutes)) {
  const src = read(`resources/public/${file}`);
  assert(src.includes(route), `${file}: POSTs to ${route}`);
}

// =========================================================================
// 8. rectangleComponent.js — setComponentVisibility dispatch
// =========================================================================
section("rectangleComponent.js — setComponentVisibility dispatch");

const rectSrc = read("resources/public/rectangleComponent.js");

for (const [alias, tag] of Object.entries(componentMap)) {
  assert(rectSrc.includes(`getShortBtype('${alias}')`),
    `setComponentVisibility: case for "${alias}"`);
  // querySelector uses single or double quotes around the tag name
  assert(rectSrc.includes(tag),
    `setComponentVisibility: querySelector for "${tag}"`);
}

// =========================================================================
// 9. Selmer template (index.html) — toolbar, components, scripts
// =========================================================================
section("Selmer template (resources/html/index.html)");

const indexSrc = read("resources/html/index.html");

for (const alias of newNodeAliases) {
  assert(indexSrc.includes(`addRectangle('${alias}')`),
    `index.html: toolbar button for "${alias}"`);
}

for (const tag of Object.values(componentMap)) {
  assert(indexSrc.includes(`<${tag}`),
    `index.html: has <${tag}> element`);
}

for (const file of componentFiles) {
  assert(indexSrc.includes(`src="${file}"`),
    `index.html: has <script> for ${file}`);
}

// =========================================================================
// 10. Backend: btype-codes in graph2.clj
// =========================================================================
section("Backend graph2.clj — btype-codes");

const g2Src = read("src/clj/bitool/graph2.clj");

for (const [alias, code] of Object.entries(expectedBtypes)) {
  if (["table", "grid"].includes(alias)) continue;
  assert(g2Src.includes(`"${alias}" "${code}"`),
    `btype-codes: "${alias}" -> "${code}"`);
}

// =========================================================================
// 11. Backend: node-keys for new btypes
// =========================================================================
section("Backend graph2.clj — node-keys");

const nodeKeysAliases = [
  "filter", "calculated", "aggregation", "endpoint", "response-builder",
  "validator", "auth", "db-execute", "rate-limiter", "cors", "logger",
  "cache", "event-emitter", "circuit-breaker", "scheduler", "webhook"
];

for (const alias of nodeKeysAliases) {
  assert(g2Src.includes(`"${alias}"`), `node-keys: has "${alias}"`);
}

// =========================================================================
// 12. Backend: item_master cases
// =========================================================================
section("Backend graph2.clj — item_master cases");

const itemMasterStart = g2Src.indexOf("defn item_master");
const itemMasterEnd = g2Src.indexOf("defn item_columns");
const itemMasterBody = g2Src.substring(itemMasterStart, itemMasterEnd);

const caseCodes = ["Ep", "Rb", "Vd", "Au", "Dx", "Rl", "Cr", "Lg", "Cq", "Ev", "Ci", "Sc", "Wh"];
for (const code of caseCodes) {
  assert(itemMasterBody.includes(`"${code}"`),
    `item_master: has case for "${code}"`);
}

// =========================================================================
// 13. Backend: rectangles connection rules
// =========================================================================
section("Backend graph2.clj — rectangles connection rules");

const rectStart = g2Src.indexOf("def rectangles");
const rectEnd = g2Src.indexOf("def btype-codes");
const rectBody = g2Src.substring(rectStart, rectEnd);

for (const code of caseCodes) {
  assert(rectBody.includes(`"${code}"`),
    `rectangles: has entry for/references "${code}"`);
}

// =========================================================================
// 14. Backend: get-fx-from-btype in home.clj
// =========================================================================
section("Backend home.clj — get-fx-from-btype dispatch");

const homeSrc = read("src/clj/bitool/routes/home.clj");

for (const code of caseCodes) {
  assert(homeSrc.includes(`"${code}"`),
    `get-fx-from-btype: handles "${code}"`);
}

// =========================================================================
// 15. Backend: save routes registered
// =========================================================================
section("Backend home.clj — save routes registered");

const expectedBackendRoutes = [
  "/saveAuth", "/saveDx", "/saveRl", "/saveCr", "/saveLg",
  "/saveCq", "/saveEv", "/saveCi", "/saveEndpoint",
  "/saveResponseBuilder", "/saveValidator", "/saveSc", "/saveWh"
];
for (const route of expectedBackendRoutes) {
  assert(homeSrc.includes(`"${route}"`),
    `home.clj: route "${route}" registered`);
}

// =========================================================================
// 16. Backend: get-btype allowlist
// =========================================================================
section("Backend home.clj — get-btype allowlist");

for (const alias of newNodeAliases) {
  assert(homeSrc.includes(`"${alias}"`),
    `get-btype allowlist: includes "${alias}"`);
}

// =========================================================================
// 17. main.js — addRectangle function
// =========================================================================
section("main.js — addRectangle");

const mainSrc = read("resources/public/main.js");
assert(mainSrc.includes("window.addRectangle"), "main.js: defines addRectangle");
assert(mainSrc.includes("alias: name"), "main.js: addRectangle sets alias");
assert(mainSrc.includes("sendRectangleData"), "main.js: addRectangle calls sendRectangleData");

// =========================================================================
// 18. Runtime safety — guards against null/missing params
// =========================================================================
section("Runtime safety — null/missing param guards");

// dbExecuteComponent must NOT call loadConnections() in bindElements()
const dxSrc = read("resources/public/dbExecuteComponent.js");
const dxBindMatch = dxSrc.match(/bindElements\(\)\s*\{([\s\S]*?)\n\s{2}\}/);
if (dxBindMatch) {
  assert(!dxBindMatch[1].includes("loadConnections"),
    "dbExecuteComponent.js: bindElements() does NOT call loadConnections()");
} else {
  assert(false, "dbExecuteComponent.js: could not find bindElements()");
}
// loadConnections should be called in open() instead
const dxOpenMatch = dxSrc.match(/open\(\)\s*\{([\s\S]*?)\n\s{2}\}/);
if (dxOpenMatch) {
  assert(dxOpenMatch[1].includes("loadConnections"),
    "dbExecuteComponent.js: open() calls loadConnections()");
} else {
  assert(false, "dbExecuteComponent.js: could not find open()");
}

// rectangleComponent must guard against null id before calling getItem
assert(rectSrc.includes('"null"') || rectSrc.includes("'null'"),
  "rectangleComponent.js: guards against null id string");

// treeComponent sendRectangleData should handle errors in catch block
const treeSrc = read("resources/public/treeComponent.js");
assert(treeSrc.includes("sendRectangleData") && treeSrc.includes(".catch"),
  "treeComponent.js: sendRectangleData has error handling");
assert(treeSrc.includes('setAttribute("endpoint-label"'),
  "treeComponent.js: passes endpoint-label to rectangle-component");

assert(rectSrc.includes('"endpoint-label"'),
  "rectangleComponent.js: observes endpoint-label attribute");
assert(rectSrc.includes("endpoint-path"),
  "rectangleComponent.js: renders endpoint path label for Ep/Wh nodes");

assert(g2Src.includes(":endpoint_label"),
  "graph2.clj: coordinate payload includes endpoint_label for node rendering");

// Backend: getFunctionTypes route exists
assert(homeSrc.includes("getFunctionTypes"),
  "home.clj: /getFunctionTypes route registered");

// utils.js request() extracts error field from response
const utilsSrc2 = utilsSrc;
assert(utilsSrc2.includes("data?.error"),
  "utils.js: request() extracts error field from non-OK response");

// Backend: get-conn handles missing conn-id
const homeSrc2 = homeSrc;
assert(homeSrc2.includes("nil? conn-id") || homeSrc2.includes("nil? conn-id-str"),
  "home.clj: get-conn handles nil conn-id");

// Backend: add-single checks for missing gid
assert(homeSrc2.includes("No graph selected"),
  "home.clj: add-single returns error when no graph selected");

// Backend: get-item validates id param
assert(homeSrc2.includes("Invalid item id") || homeSrc2.includes("null\" id-str"),
  "home.clj: get-item validates id parameter");

// Backend: moveSingle route exists
assert(homeSrc2.includes("moveSingle"),
  "home.clj: /moveSingle route registered");

// Component numeric values must be String()-wrapped for smart elements
const numericValueFiles = [
  "circuitBreakerComponent.js", "cacheComponent.js",
  "rateLimiterComponent.js", "corsComponent.js"
];
for (const file of numericValueFiles) {
  const src = read(`resources/public/${file}`);
  assert(!/\.value\s*=\s*(?:rect\.\w+\s*\?\?\s*\d|(?!String\()(?!rect\.)(?!this\.)\d+\s*;)/.test(src),
    `${file}: no raw numeric assignment to .value`);
}

// =========================================================================
// 19. OpenAPI Import — frontend UI
// =========================================================================
section("OpenAPI Import — frontend UI");

const oaiSrc = read("resources/public/openApiImport.js");

// Modal has URL input field
assert(oaiSrc.includes('id="oai-url"'), "openApiImport.js: has URL input (#oai-url)");

// Modal has Fetch button
assert(oaiSrc.includes('id="oai-fetch"'), "openApiImport.js: has Fetch button (#oai-fetch)");

// Fetch button has click listener
assert(oaiSrc.includes("oai-fetch") && oaiSrc.includes("doFetchUrl"),
  "openApiImport.js: Fetch button wired to doFetchUrl");

// doFetchUrl function exists and fetches from URL
assert(oaiSrc.includes("async function doFetchUrl"),
  "openApiImport.js: doFetchUrl function defined");
assert(oaiSrc.includes("await fetch(url)"),
  "openApiImport.js: doFetchUrl calls fetch(url)");

// doFetchUrl populates spec textarea
assert(oaiSrc.includes("specArea.value = text"),
  "openApiImport.js: doFetchUrl populates textarea with fetched text");

// doFetchUrl auto-fills graph name from spec title
assert(oaiSrc.includes("info?.title"),
  "openApiImport.js: doFetchUrl auto-fills graph name from spec title");

// doFetchUrl handles errors
assert(oaiSrc.includes("Failed to fetch"),
  "openApiImport.js: doFetchUrl shows error on failure");

// openModal clears URL field
assert(oaiSrc.includes('"#oai-url"') && oaiSrc.includes('value = ""'),
  "openApiImport.js: openModal clears URL input");

// Modal has paste textarea
assert(oaiSrc.includes('id="oai-spec"'), "openApiImport.js: has paste textarea (#oai-spec)");

// Modal has import button
assert(oaiSrc.includes('id="oai-import"'), "openApiImport.js: has Import button (#oai-import)");

// Import posts to /importOpenApi
assert(oaiSrc.includes("/importOpenApi"),
  "openApiImport.js: doImport posts to /importOpenApi");

// Modal has graph name input
assert(oaiSrc.includes('id="oai-name"'), "openApiImport.js: has Graph Name input (#oai-name)");

// toggleOpenApiImport exposed globally
assert(oaiSrc.includes("window.toggleOpenApiImport"),
  "openApiImport.js: toggleOpenApiImport exposed on window");

// =========================================================================
// 20. OpenAPI Import — Selmer template integration
// =========================================================================
section("OpenAPI Import — Selmer template integration");

// index.html has the OpenAPI button
assert(indexSrc.includes("toggleOpenApiImport"),
  "index.html: has OpenAPI button (toggleOpenApiImport)");

// index.html has Deploy API button
assert(indexSrc.includes("deployEndpoints()"),
  "index.html: has Deploy API button (deployEndpoints)");

// index.html loads openApiImport.js script
assert(indexSrc.includes('src="openApiImport.js"'),
  "index.html: has <script> for openApiImport.js");

assert(mainSrc.includes("window.deployEndpoints"),
  "main.js: deployEndpoints function exposed globally");
assert(mainSrc.includes("/deployEndpoints"),
  "main.js: deployEndpoints calls /deployEndpoints");

// =========================================================================
// 21. OpenAPI Import — backend route and openapi.clj graph generation
// =========================================================================
section("OpenAPI Import — backend");

// Route registered
assert(homeSrc.includes('"/importOpenApi"'),
  "home.clj: /importOpenApi route registered");
assert(homeSrc.includes('"/deployEndpoints"'),
  "home.clj: /deployEndpoints route registered");
assert(homeSrc.includes("deploy-graph-endpoints!"),
  "home.clj: deploy-endpoints calls endpoint/deploy-graph-endpoints!");

// import-open-api handler calls spec->graph
assert(homeSrc.includes("spec->graph"),
  "home.clj: import-open-api calls openapi/spec->graph");

// openapi.clj exists and generates correct node types
const oapiSrc = read("src/clj/bitool/api/openapi.clj");

assert(oapiSrc.includes("spec->graph"),
  "openapi.clj: spec->graph function defined");

// Generates Ep nodes
assert(oapiSrc.includes('"Ep"'),
  'openapi.clj: generates Ep (Endpoint) nodes');

// Generates Au nodes for security
assert(oapiSrc.includes('"Au"'),
  'openapi.clj: generates Au (Auth) nodes');

// Generates Vd nodes for validation
assert(oapiSrc.includes('"Vd"'),
  'openapi.clj: generates Vd (Validator) nodes');

// Generates Rb nodes for responses
assert(oapiSrc.includes('"Rb"'),
  'openapi.clj: generates Rb (Response Builder) nodes');

// Output node (id=1) is always created
assert(oapiSrc.includes(':btype "O"'),
  'openapi.clj: creates Output node (btype O, id 1)');

// Nodes are wired with edges
assert(oapiSrc.includes("wire"),
  "openapi.clj: wires nodes together with edges");

// Ep node gets route_path, http_method, params
assert(oapiSrc.includes(":route_path") && oapiSrc.includes(":http_method"),
  "openapi.clj: Ep node has route_path and http_method");
assert(oapiSrc.includes(":path_params") && oapiSrc.includes(":query_params"),
  "openapi.clj: Ep node has path_params and query_params");
assert(oapiSrc.includes(":body_schema"),
  "openapi.clj: Ep node has body_schema");

// Au node gets auth_type and token_header
assert(oapiSrc.includes(":auth_type") && oapiSrc.includes(":token_header"),
  "openapi.clj: Au node has auth_type and token_header");

// Vd node gets rules
assert(oapiSrc.includes(":rules"),
  "openapi.clj: Vd node has validation rules");

// Rb node gets status_code and template
assert(oapiSrc.includes(":status_code") && oapiSrc.includes(":template"),
  "openapi.clj: Rb node has status_code and template");

// Rb wires to Output (node 1)
assert(oapiSrc.includes("wire rb-id 1"),
  "openapi.clj: Rb node wires to Output (node 1)");

// Chain is Ep → [Au] → [Vd] → Rb → O
assert(oapiSrc.includes("wire ep-id au-id") || oapiSrc.includes("wire g ep-id"),
  "openapi.clj: Ep wires to next node in chain");
assert(oapiSrc.includes("wire prev-id rb-id"),
  "openapi.clj: chain connects to Rb");

// Handles path-level params merged with operation params
assert(oapiSrc.includes("path-param-idx") && oapiSrc.includes("merge"),
  "openapi.clj: merges path-level and operation-level parameters");

// import-open-api registers Ep/Wh endpoints
assert(homeSrc.includes("register-endpoint!"),
  "home.clj: import registers Ep/Wh endpoints for dynamic routing");

// import-open-api sets session :gid after import so components work
assert(homeSrc.includes('assoc :session') && homeSrc.includes('new-gid'),
  "home.clj: import-open-api sets session :gid after graph insert");
assert(homeSrc.includes('new-ver'),
  "home.clj: import-open-api sets session :ver after graph insert");
// Verify gid is extracted from inserted graph, not from the (possibly nil) request session
assert(homeSrc.includes('(get-in cp [:a :id])'),
  "home.clj: import-open-api extracts gid from inserted graph (cp), not request session");
// Verify the response wraps http-response/ok with session assoc
assert(homeSrc.includes('(assoc :session (assoc session :gid new-gid :ver new-ver))'),
  "home.clj: import-open-api response includes session with gid and ver");

// OpenAPI importer uses correct field keys for UI compatibility
assert(oapiSrc.includes(":param_name"),
  "openapi.clj: params use :param_name (not :name) for UI compatibility");
assert(oapiSrc.includes(":field_name"),
  "openapi.clj: body_schema uses :field_name (not :name) for UI compatibility");

// OpenAPI importer generates tcols for Ep nodes
assert(oapiSrc.includes("ep-params->tcols"),
  "openapi.clj: build-chain generates tcols for Ep nodes during import");
assert(oapiSrc.includes("g2/ep-params->tcols"),
  "openapi.clj: uses graph2/ep-params->tcols to generate Ep columns");

// ep-params->tcols accepts :name as fallback key (for flexibility)
assert(g2Src.includes(":param_name :name") || g2Src.includes(':param_name :name "param_name"'),
  "graph2.clj: ep-params->tcols accepts :name as fallback key for param_name");

// =========================================================================
// 22. Conditional (C) node — backend + frontend integration
// =========================================================================

// Backend: btype-codes has "conditionals" -> "C"
assert(g2Src.includes('"conditionals" "C"'),
  "graph2.clj: btype-codes maps conditionals to C");

// Backend: rectangles config includes "C"
assert(g2Src.includes('"C"  ["J"'),
  "graph2.clj: rectangles has C node with valid children");

// Backend: other nodes can connect to C
assert(g2Src.includes('"Tg" "C" "O"') || g2Src.includes('"C" "O"'),
  "graph2.clj: source nodes (T, P, etc.) list C as valid child");

// Backend: node-keys has conditionals
assert(g2Src.includes('"conditionals" [:cond_type :branches :default_branch]'),
  "graph2.clj: node-keys includes conditionals with correct keys");

// Backend: save-conditional function exists
assert(g2Src.includes("defn save-conditional"),
  "graph2.clj: save-conditional function defined");

// Backend: get-conditional-item function exists
assert(g2Src.includes("defn get-conditional-item"),
  "graph2.clj: get-conditional-item function defined");

// Backend: item_master handles C btype
assert(g2Src.includes('"C"  (merge item (select-keys tmap [:cond_type :branches :default_branch]))'),
  "graph2.clj: item_master has C case with correct keys");

// Backend: get-fx-from-btype dispatches C
assert(homeSrc.includes('"C"  g2/get-conditional-item'),
  "home.clj: get-fx-from-btype dispatches C to get-conditional-item");

// Backend: save route exists
assert(homeSrc.includes('"/saveConditional"'),
  "home.clj: /saveConditional route registered");

// Backend: save handler exists
assert(homeSrc.includes("defn save-conditional"),
  "home.clj: save-conditional handler defined");
assert(homeSrc.includes("g2/save-conditional"),
  "home.clj: save-conditional calls g2/save-conditional");

// Frontend: mainComponent.js posts to /saveConditional
const condMainSrc = read("resources/public/conditional forms/mainComponent.js");
assert(condMainSrc.includes("/saveConditional"),
  "mainComponent.js: save posts to /saveConditional");
assert(condMainSrc.includes("loadFromRect"),
  "mainComponent.js: loads saved data from rect on open");
assert(condMainSrc.includes("collectData"),
  "mainComponent.js: uses collectData to get sub-component state");
assert(condMainSrc.includes("cond_type"),
  "mainComponent.js: sends cond_type in save payload");

// Frontend: utils.js posts to /saveConditional (not /addtable)
const condUtilsSrc = read("resources/public/conditional forms/Library/utils.js");
assert(condUtilsSrc.includes("/saveConditional"),
  "conditional utils.js: storeData posts to /saveConditional");
assert(!condUtilsSrc.includes("/addtable"),
  "conditional utils.js: no longer posts to /addtable");

// Frontend: sub-components have collectData and loadData
const ifElseSrc = read("resources/public/conditional forms/Library/ifElse.js");
assert(ifElseSrc.includes("collectData"), "ifElse.js: has collectData method");
assert(ifElseSrc.includes("loadData"), "ifElse.js: has loadData method");

const multiIfSrc = read("resources/public/conditional forms/Library/multiIf.js");
assert(multiIfSrc.includes("collectData"), "multiIf.js: has collectData method");
assert(multiIfSrc.includes("loadData"), "multiIf.js: has loadData method");

const caseSrc = read("resources/public/conditional forms/Library/case.js");
assert(caseSrc.includes("collectData"), "case.js: has collectData method");
assert(caseSrc.includes("loadData"), "case.js: has loadData method");

const condSrc = read("resources/public/conditional forms/Library/condition.js");
assert(condSrc.includes("collectData"), "condition.js: has collectData method");
assert(condSrc.includes("loadData"), "condition.js: has loadData method");

const patternSrc = read("resources/public/conditional forms/Library/patternMatch.js");
assert(patternSrc.includes("collectData"), "patternMatch.js: has collectData method");
assert(patternSrc.includes("loadData"), "patternMatch.js: has loadData method");

// Frontend: rectangleComponent opens control-flow-component for conditionals
assert(rectSrc.includes("control-flow-component"),
  "rectangleComponent.js: opens control-flow-component for conditionals");

// Frontend: utils.js has C in BTYPES and ICON
assert(utilsSrc.includes('"conditionals"') && utilsSrc.includes('"C"'),
  "utils.js: BTYPES has conditionals -> C");

// =========================================================================
// 25. Logic / Function (Fu) Node — Backend
// =========================================================================
section("Logic / Function (Fu) Node — Backend");

// graph2.clj: rectangles has Fu entry
assert(g2Src.includes('"Fu" [') || g2Src.match(/"Fu"\s*\[/),
  "graph2.clj: rectangles has Fu entry defining valid children");

// graph2.clj: node-keys has function with correct keys
assert(g2Src.includes('"function"') && g2Src.includes(":fn_name") && g2Src.includes(":fn_params")
  && g2Src.includes(":fn_lets") && g2Src.includes(":fn_return") && g2Src.includes(":fn_outputs"),
  "graph2.clj: node-keys includes function with fn_name/fn_params/fn_lets/fn_return/fn_outputs");

// graph2.clj: logic-outputs->tcols defined
assert(g2Src.includes("logic-outputs->tcols"),
  "graph2.clj: logic-outputs->tcols function defined");

// graph2.clj: save-logic defined
assert(g2Src.includes("defn save-logic"),
  "graph2.clj: save-logic function defined");

// graph2.clj: save-logic generates tcols from fn_outputs
assert(g2Src.includes("logic-outputs->tcols") && g2Src.includes(":fn_outputs"),
  "graph2.clj: save-logic generates tcols from fn_outputs");

// graph2.clj: save-logic propagates to children
assert(g2Src.includes("find-edges") && g2Src.includes("child-ids"),
  "graph2.clj: save-logic propagates tcols to children");

// graph2.clj: get-logic-item defined
assert(g2Src.includes("defn get-logic-item"),
  "graph2.clj: get-logic-item function defined");

// graph2.clj: get-logic-item returns parent columns as items
assert(g2Src.includes("get-logic-item") && g2Src.includes('"items"'),
  "graph2.clj: get-logic-item returns parent columns as items");

// graph2.clj: item_master has Fu case
assert(g2Src.includes('"Fu"') && g2Src.includes(":fn_name"),
  "graph2.clj: item_master has Fu case with fn_ keys");

// graph2.clj: update-table-cols propagates through Fu
assert(/find-nodes-btype\s+g\s+\[.*"Fu"/.test(g2Src),
  "graph2.clj: update-table-cols includes Fu in propagation list");

// home.clj: get-fx-from-btype dispatches Fu to get-logic-item
assert(homeSrc.includes('"Fu"') && homeSrc.includes("get-logic-item"),
  "home.clj: get-fx-from-btype dispatches Fu to get-logic-item");

// home.clj: /saveLogic route registered
assert(homeSrc.includes("/saveLogic"),
  "home.clj: /saveLogic route registered");

// home.clj: save-logic handler defined
assert(homeSrc.includes("defn save-logic"),
  "home.clj: save-logic handler defined");

// home.clj: save-logic calls g2/save-logic
assert(homeSrc.includes("g2/save-logic"),
  "home.clj: save-logic calls g2/save-logic");

// =========================================================================
// 26. Logic / Function (Fu) Node — Frontend
// =========================================================================
section("Logic / Function (Fu) Node — Frontend");

const fbSrc = read("resources/public/functionBuilder.js");

// functionBuilder.js: defines lambda-function-builder custom element
assert(fbSrc.includes("lambda-function-builder"),
  "functionBuilder.js: defines lambda-function-builder custom element");

// functionBuilder.js: has collectData method
assert(fbSrc.includes("collectData"),
  "functionBuilder.js: has collectData method");

// functionBuilder.js: collectData returns fn_params
assert(fbSrc.includes("fn_params"),
  "functionBuilder.js: collectData returns fn_params");

// functionBuilder.js: collectData returns fn_lets
assert(fbSrc.includes("fn_lets"),
  "functionBuilder.js: collectData returns fn_lets");

// functionBuilder.js: collectData returns fn_return
assert(fbSrc.includes("fn_return"),
  "functionBuilder.js: collectData returns fn_return");

// functionBuilder.js: collectData returns fn_outputs
assert(fbSrc.includes("fn_outputs"),
  "functionBuilder.js: collectData returns fn_outputs");

// functionBuilder.js: save posts to /saveLogic
assert(fbSrc.includes("/saveLogic"),
  "functionBuilder.js: save posts to /saveLogic");

// functionBuilder.js: imports request from utils
assert(fbSrc.includes("import") && fbSrc.includes("request"),
  "functionBuilder.js: imports request utility");

// rectangleComponent.js: opens lambda-function-builder for Fu
assert(rectSrc.includes("lambda-function-builder"),
  "rectangleComponent.js: opens lambda-function-builder for Fu btype");

// utils.js: closeOpenedPanel includes lambda-function-builder
assert(utilsSrc.includes("lambda-function-builder"),
  "utils.js: closeOpenedPanel includes lambda-function-builder");

// utils.js: BTYPES has function -> Fu
assert(utilsSrc.includes('"function"') && utilsSrc.includes('"Fu"'),
  "utils.js: BTYPES has function -> Fu");

// utils.js: ICON has Fu entry
assert(utilsSrc.includes("Fu:"),
  "utils.js: ICON has Fu entry");

// =========================================================================
// Summary
// =========================================================================
console.log(`\n${"=".repeat(50)}`);
if (failed === 0) {
  console.log(`\x1b[32m${passed} passed, 0 failed\x1b[0m`);
} else {
  console.log(`\x1b[31m${passed} passed, ${failed} failed\x1b[0m`);
}
console.log(`${"=".repeat(50)}`);

process.exit(failed > 0 ? 1 : 0);
