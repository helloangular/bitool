import EventHandler from "./library/eventHandler.js";
import { customConfirm, getPanelItems, request, setPanelItems } from "./library/utils.js";
import { aiAssistCSS, renderAiTrigger, renderAiLoading, renderAiCard, bindAiTriggers, callAiEndpoint } from "./aiAssistCard.js";

const template = document.createElement("template");
template.innerHTML = `
<style>
  :host {
    display: none;
    background: #f6f2e8;
    color: #1e2a24;
    overflow: auto;
    font-family: Georgia, "Times New Roman", serif;
  }
  .shell {
    min-height: 100vh;
    padding: 24px;
    background:
      radial-gradient(circle at top left, rgba(183, 137, 78, 0.20), transparent 32%),
      linear-gradient(180deg, #fbf8f1 0%, #f4eee2 100%);
  }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    margin-bottom: 20px;
  }
  .title h2 { margin: 0; font-size: 28px; font-weight: 600; }
  .title p { margin: 4px 0 0 0; color: #5d665f; font-size: 14px; }
  .mode-badge {
    display: inline-block;
    padding: 3px 10px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.5px;
    border-radius: 3px;
    margin-left: 10px;
  }
  .mode-etl { background: #dbeafe; color: #1e40af; }
  .mode-service { background: #dcfce7; color: #166534; }
  .actions { display: flex; gap: 10px; flex-wrap: wrap; }
  button {
    border: 1px solid #233126;
    background: #233126;
    color: #fffaf0;
    padding: 10px 14px;
    cursor: pointer;
  }
  button.secondary {
    background: transparent;
    color: #233126;
  }
  .card {
    background: rgba(255, 252, 246, 0.9);
    border: 1px solid rgba(35, 49, 38, 0.15);
    box-shadow: 0 20px 40px rgba(35, 49, 38, 0.08);
    padding: 18px;
    margin-bottom: 16px;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 12px;
  }
  label {
    display: block;
    font-size: 13px;
    color: #49564d;
    margin-bottom: 4px;
  }
  input, select, textarea {
    width: 100%;
    box-sizing: border-box;
    border: 1px solid rgba(35, 49, 38, 0.18);
    background: #fffdf7;
    color: #1e2a24;
    padding: 8px 10px;
    font-size: 14px;
  }
  textarea {
    min-height: 92px;
    resize: vertical;
    font-family: "SFMono-Regular", Consolas, monospace;
  }
  .endpoint-list { display: grid; gap: 14px; }
  .endpoint-card {
    border: 1px solid rgba(35, 49, 38, 0.15);
    background: rgba(255, 255, 255, 0.85);
    padding: 14px;
  }
  .endpoint-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    margin-bottom: 10px;
  }
  .endpoint-head strong { font-size: 16px; }
  .hint { color: #66736c; font-size: 12px; }
  .row { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .row > * { flex: 1; }
  .inline { display: flex; align-items: center; gap: 8px; }
  .inline input[type="checkbox"] { width: auto; }
  .spec-tools { display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }
  .spec-tools .grow { flex: 1; min-width: 220px; }
  .status { min-height: 20px; color: #49564d; font-size: 13px; margin-top: 10px; }
  .mono { font-family: "SFMono-Regular", Consolas, monospace; font-size: 12px; }

  /* Tabs */
  .tabs {
    display: flex;
    gap: 0;
    border-bottom: 2px solid rgba(35, 49, 38, 0.15);
    margin-bottom: 12px;
  }
  .tab {
    padding: 8px 16px;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    border: none;
    background: transparent;
    color: #49564d;
    border-bottom: 2px solid transparent;
    margin-bottom: -2px;
  }
  .tab.active {
    color: #233126;
    border-bottom-color: #233126;
  }
  .tab:hover:not(.active) { color: #1e2a24; }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* Collapsible auth */
  .collapse-toggle {
    cursor: pointer;
    font-size: 14px;
    font-weight: 600;
    color: #233126;
    user-select: none;
    margin-bottom: 8px;
  }
  .collapse-toggle::before { content: "\\25B6 "; font-size: 10px; }
  .collapse-toggle.open::before { content: "\\25BC "; }
  .collapse-body { display: none; }
  .collapse-body.open { display: block; }

  /* Endpoint selection table */
  .ep-table-wrap { margin-top: 12px; }
  .ep-table-wrap table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .ep-table-wrap th, .ep-table-wrap td { padding: 6px 10px; text-align: left; border-bottom: 1px solid rgba(35,49,38,0.12); }
  .ep-table-wrap thead { position: sticky; top: 0; background: #fffdf7; z-index: 1; }
  .ep-table-wrap tr.ep-row:hover { background: rgba(35,49,38,0.04); cursor: pointer; }
  .ep-table-wrap tr.ep-row.checked { background: rgba(35,49,38,0.07); }
  .ep-table-wrap .method-badge { font-size: 11px; font-weight: 600; font-family: monospace; padding: 2px 6px; border-radius: 2px; }
  .ep-table-wrap .method-badge.get { background: #dbeafe; color: #1e40af; }
  .ep-table-wrap .method-badge.post { background: #fef3c7; color: #92400e; }
  .ep-table-wrap .method-badge.put { background: #e0e7ff; color: #3730a3; }
  .ep-table-wrap .method-badge.delete { background: #fee2e2; color: #991b1b; }
  .ep-table-wrap .method-badge.patch { background: #ede9fe; color: #5b21b6; }
  .ep-filter-row { display: flex; gap: 10px; align-items: center; margin-bottom: 8px; }
  .ep-filter-row input[type="text"] { flex: 1; }
  .ep-filter-row select { width: 110px; }

  /* AI Assist (shared) */
  ${aiAssistCSS}
</style>
<div class="shell">
  <div class="header">
    <div class="title">
      <h2>API Configuration <span id="modeBadge" class="mode-badge"></span></h2>
      <p id="modeSubtitle"></p>
    </div>
    <div class="actions">
      <select id="runEndpointName"><option value="">Run all</option></select>
      <button id="runButton" class="secondary" type="button">Run</button>
      <button id="saveButton" type="button">Save</button>
      <button id="closeButton" class="secondary" type="button">Close</button>
    </div>
  </div>

  <!-- Top-level tabs -->
  <div class="tabs" id="mainTabs">
    <div class="tab active" data-tab="connection">Connection</div>
    <div class="tab" data-tab="endpoints">Endpoints</div>
    <div class="tab" data-tab="advanced" id="advancedTab">Advanced</div>
  </div>

  <!-- CONNECTION TAB -->
  <div class="tab-panel active" id="panel-connection">
    <div class="card">
      <div class="grid">
        <div>
          <label for="apiName">API Name</label>
          <input id="apiName" name="api_name" data-field="api_name" type="text" />
        </div>
        <div>
          <label for="sourceSystem">Source System</label>
          <input id="sourceSystem" name="source_system" data-field="source_system" type="text" placeholder="samsara" />
        </div>
        <div>
          <label for="baseUrl">Base URL</label>
          <input id="baseUrl" name="base_url" data-field="base_url" type="text" placeholder="http://localhost:3001" />
        </div>
        <div>
          <label for="specificationUrl">Specification URL</label>
          <input id="specificationUrl" name="specification_url" data-field="specification_url" type="text" />
        </div>
      </div>
    </div>

    <div class="card">
      <div class="collapse-toggle" id="authToggle">Authentication</div>
      <div class="collapse-body" id="authBody">
        <div class="grid">
          <div>
            <label for="authType">Auth Type</label>
            <select id="authType" data-field="auth_ref.type">
              <option value="">None</option>
              <option value="bearer">Bearer</option>
              <option value="api-key">API Key</option>
            </select>
          </div>
          <div>
            <label for="authSecretRef">Secret Ref</label>
            <input id="authSecretRef" data-field="auth_ref.secret_ref" type="text" placeholder="SAMSARA_TOKEN" />
          </div>
          <div>
            <label for="authToken">Inline Token / Key</label>
            <input id="authToken" data-field="auth_ref.token" type="text" />
          </div>
          <div>
            <label for="authLocation">API Key Location</label>
            <select id="authLocation" data-field="auth_ref.location">
              <option value="header">Header</option>
              <option value="query">Query</option>
            </select>
          </div>
          <div>
            <label for="authParamName">API Key Param Name</label>
            <input id="authParamName" data-field="auth_ref.param_name" type="text" placeholder="api_key" />
          </div>
          <div>
            <label for="authHeaderName">API Key Header Name</label>
            <input id="authHeaderName" data-field="auth_ref.header_name" type="text" placeholder="X-API-Key" />
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- ENDPOINTS TAB -->
  <div class="tab-panel" id="panel-endpoints">
    <div class="card">
      <div class="spec-tools">
        <div class="grow">
          <label for="discoverSpecUrl">Specification URL</label>
          <input id="discoverSpecUrl" type="text" placeholder="Paste OpenAPI spec URL" />
        </div>
        <button id="discoverButton" class="secondary" type="button">Get Endpoints</button>
      </div>
      <div class="status" id="statusText"></div>

      <!-- Endpoint selection table -->
      <div id="endpointTableWrap" class="ep-table-wrap" style="display:none;">
        <div class="ep-filter-row">
          <input id="epSearch" type="text" placeholder="Search endpoints..." />
          <select id="epMethodFilter">
            <option value="">All Methods</option>
            <option value="GET">GET</option>
            <option value="POST">POST</option>
            <option value="PUT">PUT</option>
            <option value="DELETE">DELETE</option>
            <option value="PATCH">PATCH</option>
          </select>
          <label class="inline" style="font-size:12px; white-space:nowrap;"><input type="checkbox" id="epSelectAll" /> All</label>
        </div>
        <div style="max-height:300px; overflow-y:auto; border:1px solid rgba(35,49,38,0.15);">
          <table>
            <thead>
              <tr>
                <th style="width:40px; text-align:center;"></th>
                <th>Endpoint</th>
                <th style="width:70px;">Method</th>
              </tr>
            </thead>
            <tbody id="epTableBody"></tbody>
          </table>
        </div>
        <div style="margin-top:6px; font-size:12px; color:#49564d;" id="epSelectionCount"></div>
      </div>
    </div>
    <div style="margin:10px 0;">
      <button id="addEndpointButton" type="button">+ Add Endpoint</button>
    </div>
    <div class="endpoint-list" id="endpointList"></div>
  </div>

  <!-- ADVANCED TAB (ETL-only fields) -->
  <div class="tab-panel" id="panel-advanced">
    <div class="card">
      <p class="hint">These settings apply per endpoint. Select an endpoint to configure schema inference and ETL mapping.</p>
      <div id="advancedEndpointList"></div>
    </div>
  </div>
</div>
`;

const DEFAULT_ENDPOINT = () => ({
  endpoint_name: "",
  endpoint_url: "",
  http_method: "GET",
  load_type: "incremental",
  pagination_strategy: "cursor",
  pagination_location: "query",
  cursor_field: "pagination.endCursor",
  cursor_param: "after",
  schema_mode: "infer",
  sample_records: 100,
  max_inferred_columns: 100,
  type_inference_enabled: true,
  schema_evolution_mode: "advisory",
  primary_key_fields: ["id"],
  selected_nodes: [],
  inferred_fields: [],
  query_params: {},
  request_headers: {},
  body_params: {},
  json_explode_rules: [],
  retry_policy: { max_retries: 3, base_backoff_ms: 1000 },
  page_size: 10,
  time_window_minutes: 60,
  enabled: true,
});

const PRETTY_JSON = (value, fallback) => {
  const out = value ?? fallback;
  if (out === undefined || out === null) return "";
  return JSON.stringify(out, null, 2);
};

const escapeHtml = (value) => String(value ?? "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll("\"", "&quot;")
  .replaceAll("'", "&#39;");

const autoEndpointBronzeTableName = (cfg) => {
  const seed = String(cfg?.endpoint_name || cfg?.endpoint_url || "bronze_auto").trim().toLowerCase();
  const sanitized = seed
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!sanitized) return "bronze_auto";
  return /^[a-z_]/.test(sanitized) ? sanitized : `t_${sanitized}`;
};

const bronzeTableLabel = (cfg) =>
  cfg.bronze_table_name
  || cfg.table_name
  || bronzeTableFromRunResult(cfg?.latest_run_result)
  || autoEndpointBronzeTableName(cfg);

const inferredColumnsLabel = (cfg) => {
  const count = (cfg.inferred_fields || []).length;
  return `${count} inferred column${count === 1 ? "" : "s"}`;
};

const LOCAL_MOCK_ENDPOINTS = Object.freeze([
  // Vehicles
  "/fleet/vehicles",
  "/fleet/vehicles/stats",
  "/fleet/vehicles/locations",
  "/fleet/vehicles/fuel-energy",
  // Drivers
  "/fleet/drivers",
  // Safety
  "/fleet/safety/events",
  "/fleet/safety/scores/vehicles",
  "/fleet/safety/scores/drivers",
  // HOS / Compliance
  "/fleet/hos/logs",
  "/fleet/hos/violations",
  "/fleet/hos/clocks",
  "/fleet/hos/daily-logs",
  // Trailers
  "/fleet/trailers",
  "/fleet/trailers/locations",
  "/fleet/trailers/stats",
  // Assets
  "/fleet/assets",
  "/fleet/assets/locations",
  "/fleet/assets/reefers/stats",
  // Maintenance / DVIR
  "/fleet/maintenance/dvirs",
  "/fleet/defects",
  // Routing / Dispatch
  "/fleet/routes",
  "/fleet/dispatch/routes",
  "/fleet/dispatch/jobs",
  // Fuel / Efficiency
  "/fleet/fuel-energy/vehicle-report",
  "/fleet/ifta/summary",
  // Documents
  "/fleet/document-types",
  "/fleet/documents",
  // Addresses / Geofences
  "/addresses",
  "/fleet/geofences",
  // Tags
  "/tags",
  // Users / Org
  "/fleet/users",
  "/me",
  // Contacts
  "/contacts",
  // Webhooks
  "/webhooks",
  // Alerts
  "/alerts",
  "/alerts/configurations",
  // Industrial / Sensors
  "/industrial/data",
  "/sensors/list",
  "/sensors/temperature",
  "/sensors/humidity",
  "/sensors/door",
]);

const normalizeEndpointPath = (value) => {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return "";
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
};

const isLocalMockBaseUrl = (baseUrl) => {
  try {
    const url = new URL(String(baseUrl ?? "").trim());
    const port = url.port || (url.protocol === "https:" ? "443" : "80");
    return (url.hostname === "localhost" || url.hostname === "127.0.0.1") && port === "3001";
  } catch {
    return false;
  }
};

const isLocalMockEndpoint = (endpointUrl) => LOCAL_MOCK_ENDPOINTS.includes(normalizeEndpointPath(endpointUrl));

const localMockPreviewMessage = (endpointUrl) =>
  `Local mock server on http://localhost:3001 does not implement ${normalizeEndpointPath(endpointUrl) || "that endpoint"}. Supported preview endpoints: ${LOCAL_MOCK_ENDPOINTS.join(", ")}.`;

/** Detect timestamp fields suitable as watermarks. Returns array of field objects. */
const detectTimestampFields = (fields) => {
  if (!Array.isArray(fields) || !fields.length) return [];
  return fields.filter(f =>
    f.enabled !== false &&
    (/TIMESTAMP|DATE/i.test(f.data_type || f.type || "")
     || /(?:^|_)(date|time|timestamp|created_at|updated_at|event_at|event_time)$/i.test(f.column_name || ""))
  );
};

/** Auto-detect best watermark column name from inferred fields. */
const detectWatermarkColumn = (fields) => {
  const timestamps = detectTimestampFields(fields);
  if (!timestamps.length) return null;
  const updated = timestamps.find(f => /updated/i.test(f.column_name));
  if (updated) return updated.column_name;
  const created = timestamps.find(f => /created/i.test(f.column_name));
  if (created) return created.column_name;
  const timed = timestamps.find(f => /time|date/i.test(f.column_name));
  if (timed) return timed.column_name;
  return timestamps[0].column_name;
};

/** Auto-mark is_watermark on the best timestamp field if no fields are already marked.
    Prefers updatedAt variants. Only marks one field — user can add more manually. */
const autoMarkWatermarkFields = (fields) => {
  if (!Array.isArray(fields) || !fields.length) return false;
  if (fields.some(f => f.is_watermark)) return false;
  const timestamps = detectTimestampFields(fields);
  if (!timestamps.length) return false;
  // Pick the single best candidate
  const best = timestamps.find(f => /updated/i.test(f.column_name))
            || timestamps.find(f => /modified/i.test(f.column_name))
            || timestamps.find(f => /event.*time/i.test(f.column_name))
            || timestamps[0];
  best.is_watermark = true;
  return true;
};

const stripCommonFieldPrefixes = (value) => {
  const text = String(value || "").trim();
  return text.replace(/^(data_items_|data_item_|data_|items_|item_)/i, "");
};

const inferredFieldCanonicalName = (field) => {
  const path = String(field?.path || "").trim();
  if (path) {
    const cleaned = path
      .replace(/^\$\./, "")
      .replace(/\[\]/g, "")
      .split(".")
      .filter(Boolean)
      .pop();
    if (cleaned) return cleaned;
  }
  return stripCommonFieldPrefixes(field?.column_name || "");
};

const normalizeKeyName = (value) => stripCommonFieldPrefixes(value).replace(/^:+/, "").trim().toUpperCase();

const endpointEntityTokens = (endpoint) => {
  const raw = `${endpoint?.endpoint_name || ""} ${endpoint?.endpoint_url || ""}`;
  const baseTokens = raw
    .split(/[^A-Za-z0-9]+/)
    .map((token) => token.trim().toLowerCase())
    .filter((token) => token.length >= 2);
  const expanded = new Set();
  baseTokens.forEach((token) => {
    expanded.add(token);
    if (token.endsWith("ies") && token.length > 3) {
      expanded.add(`${token.slice(0, -3)}y`);
    } else if (token.endsWith("s") && token.length > 3) {
      expanded.add(token.slice(0, -1));
    }
  });
  return [...expanded];
};

const existingPrimaryKeysMatchInferredFields = (endpoint) => {
  const configured = Array.isArray(endpoint?.primary_key_fields)
    ? endpoint.primary_key_fields.map(normalizeKeyName).filter(Boolean)
    : [];
  if (!configured.length) return false;
  const inferred = Array.isArray(endpoint?.inferred_fields) ? endpoint.inferred_fields : [];
  const available = new Set(inferred.flatMap((field) => {
    const canonical = normalizeKeyName(inferredFieldCanonicalName(field));
    const column = normalizeKeyName(field?.column_name);
    return [canonical, column].filter(Boolean);
  }));
  return configured.every((key) => available.has(key));
};

const detectPrimaryKeyFields = (endpoint) => {
  const fields = Array.isArray(endpoint?.inferred_fields) ? endpoint.inferred_fields.filter((field) => field?.enabled !== false) : [];
  if (!fields.length) return [];
  const entityTokens = endpointEntityTokens(endpoint);
  const ranked = fields
    .map((field, index) => {
      const canonical = normalizeKeyName(inferredFieldCanonicalName(field));
      const column = normalizeKeyName(field?.column_name);
      if (!canonical && !column) return null;
      const target = canonical || column;
      let score = 0;
      if (target === "ID") score += 1000;
      if (target.endsWith("_ID")) score += 700;
      if (target.endsWith("_KEY")) score += 500;
      if (target.endsWith("_NUM")) score += 250;
      if (target.endsWith("_NUMBER")) score += 250;
      if (field?.nullable === false) score += 80;
      if ((field?.sample_coverage || 0) >= 0.99) score += 40;
      entityTokens.forEach((token) => {
        const normalizedToken = normalizeKeyName(token);
        if (!normalizedToken) return;
        if (target === `${normalizedToken}_ID`) score += 1200;
        else if (target.includes(`${normalizedToken}_`)) score += 400;
        else if (target.includes(normalizedToken)) score += 150;
      });
      return { field, index, score, target };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score || a.index - b.index);
  const best = ranked[0];
  if (!best || best.score < 250) return [];
  return [inferredFieldCanonicalName(best.field)];
};

const autoApplyPrimaryKeyFields = (endpoint) => {
  if (!endpoint || !Array.isArray(endpoint.inferred_fields) || !endpoint.inferred_fields.length) return false;
  if (existingPrimaryKeysMatchInferredFields(endpoint)) return false;
  const detected = detectPrimaryKeyFields(endpoint);
  if (!detected.length) return false;
  const current = Array.isArray(endpoint.primary_key_fields) ? endpoint.primary_key_fields : [];
  const same = current.length === detected.length && current.every((value, index) => normalizeKeyName(value) === normalizeKeyName(detected[index]));
  if (same) return false;
  endpoint.primary_key_fields = detected;
  return true;
};

const findInferredFieldForRecommendation = (endpoint, fieldName) => {
  const target = normalizeKeyName(fieldName);
  if (!target || !Array.isArray(endpoint?.inferred_fields)) return null;
  return endpoint.inferred_fields.find((field) => {
    const candidates = [
      normalizeKeyName(field?.column_name),
      normalizeKeyName(inferredFieldCanonicalName(field)),
    ].filter(Boolean);
    return candidates.includes(target);
  }) || null;
};

const applyServerRecommendations = (endpoint) => {
  const rec = endpoint?.schema_recommendations;
  if (!endpoint || !rec) return false;
  let changed = false;

  if ((!Array.isArray(endpoint.json_explode_rules) || !endpoint.json_explode_rules.length)
      && Array.isArray(rec.json_explode_rules)
      && rec.json_explode_rules.length
      && Number(rec?.grain?.confidence || 0) >= 80) {
    endpoint.json_explode_rules = rec.json_explode_rules.map((rule) => ({ ...rule }));
    changed = true;
  }

  if (Array.isArray(rec?.pk?.fields)
      && rec.pk.fields.length
      && Number(rec.pk.confidence || 0) >= 80
      && !existingPrimaryKeysMatchInferredFields(endpoint)) {
    endpoint.primary_key_fields = [...rec.pk.fields];
    changed = true;
  }

  if (rec?.watermark?.field
      && Number(rec.watermark.confidence || 0) >= 80
      && !endpoint.watermark_column) {
    endpoint.watermark_column = rec.watermark.field;
    changed = true;
  }

  if (rec?.watermark?.field && Array.isArray(endpoint.inferred_fields) && !endpoint.inferred_fields.some((field) => field.is_watermark)) {
    const inferredField = findInferredFieldForRecommendation(endpoint, rec.watermark.field);
    if (inferredField) {
      inferredField.is_watermark = true;
      changed = true;
    }
  }

  return changed;
};

const transientEndpointKeys = new Set(["latest_run_result", "schema_recommendations"]);

const persistedEndpointConfig = (cfg) =>
  Object.fromEntries(Object.entries(cfg || {}).filter(([key]) => !transientEndpointKeys.has(key)));

const stableStateSnapshot = (state) => JSON.stringify({
  ...(state || {}),
  endpoint_configs: Array.isArray(state?.endpoint_configs)
    ? state.endpoint_configs.map((cfg) => persistedEndpointConfig(cfg))
    : [],
});

const latestCommittedManifest = (result) => {
  const manifests = Array.isArray(result?.manifests) ? result.manifests : [];
  const committed = manifests.filter((manifest) => manifest?.status === "committed");
  return committed.at(-1) || manifests.at(-1) || null;
};

const bronzeTableFromRunResult = (result) =>
  latestCommittedManifest(result)?.table_name || result?.target_table || "";

const endpointRunSummary = (cfg) => {
  const result = cfg?.latest_run_result;
  if (!result) return "";
  const bronzeTable = bronzeTableFromRunResult(result);
  const rowsWritten = Number(result.rows_written || 0);
  if (result.status === "succeeded") {
    return bronzeTable
      ? `Last run loaded ${rowsWritten} row${rowsWritten === 1 ? "" : "s"} into ${bronzeTable}.`
      : `Last run loaded ${rowsWritten} row${rowsWritten === 1 ? "" : "s"}.`;
  }
  return result.error_message || result.error_summary || `Last run ${result.status || "failed"}.`;
};

const sleep = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));

/** Extract auth and pagination hints from an OpenAPI spec object */
function inferFromSpec(spec) {
  const result = { auth: null, pagination: null, baseUrl: "" };
  if (!spec) return result;

  // Base URL
  if (spec.servers && spec.servers[0]?.url) {
    result.baseUrl = spec.servers[0].url;
  } else if (spec.host) {
    const scheme = (spec.schemes && spec.schemes[0]) || "https";
    result.baseUrl = `${scheme}://${spec.host}${spec.basePath || ""}`;
  }

  // Auth from securityDefinitions (Swagger 2) or components.securitySchemes (OpenAPI 3)
  const schemes = spec.securityDefinitions || spec.components?.securitySchemes || {};
  for (const [name, scheme] of Object.entries(schemes)) {
    if (scheme.type === "apiKey") {
      result.auth = {
        type: "api-key",
        location: scheme.in === "query" ? "query" : "header",
        param_name: scheme.name || name,
        header_name: scheme.name || name,
      };
      break;
    }
    if (scheme.type === "http" && scheme.scheme === "bearer") {
      result.auth = { type: "bearer" };
      break;
    }
    if (scheme.type === "oauth2") {
      result.auth = { type: "bearer" };
      break;
    }
  }

  // Pagination — look for common pagination params in paths
  const paths = spec.paths || {};
  for (const [, methods] of Object.entries(paths)) {
    const get = methods.get || methods.GET;
    if (!get?.parameters) continue;
    const paramNames = get.parameters.map(p => p.name?.toLowerCase());
    if (paramNames.includes("cursor") || paramNames.includes("after")) {
      result.pagination = { strategy: "cursor", cursor_param: paramNames.includes("cursor") ? "cursor" : "after" };
      break;
    }
    if (paramNames.includes("page_token") || paramNames.includes("pagetoken") || paramNames.includes("next_page_token")) {
      result.pagination = { strategy: "token", token_param: paramNames.find(p => p.includes("token")) };
      break;
    }
    if (paramNames.includes("offset") || paramNames.includes("skip")) {
      result.pagination = { strategy: "offset" };
      break;
    }
    if (paramNames.includes("page") || paramNames.includes("page_number")) {
      result.pagination = { strategy: "page" };
      break;
    }
  }

  return result;
}

function parseJsonField(value, fallback = {}) {
  const text = String(value || "").trim();
  if (!text) return fallback;
  try {
    return JSON.parse(text);
  } catch (_) {
    throw new Error("Invalid JSON field");
  }
}

function serializeEndpointConfig(cfg) {
  return {
    ...persistedEndpointConfig(cfg),
    query_params: typeof cfg.query_params === "string" ? parseJsonField(cfg.query_params, {}) : (cfg.query_params || {}),
    request_headers: typeof cfg.request_headers === "string" ? parseJsonField(cfg.request_headers, {}) : (cfg.request_headers || {}),
    body_params: typeof cfg.body_params === "string" ? parseJsonField(cfg.body_params, {}) : (cfg.body_params || {}),
    json_explode_rules: typeof cfg.json_explode_rules === "string" ? parseJsonField(cfg.json_explode_rules, []) : (cfg.json_explode_rules || []),
    inferred_fields: Array.isArray(cfg.inferred_fields) ? cfg.inferred_fields.map((field) => ({
      ...field,
      enabled: field.enabled !== false,
      nullable: field.nullable !== false,
    })) : [],
    sample_records: Number(cfg.sample_records ?? 100),
    max_inferred_columns: Number(cfg.max_inferred_columns ?? 100),
    type_inference_enabled: cfg.type_inference_enabled !== false,
    retry_policy: {
      max_retries: Number(cfg.retry_policy?.max_retries ?? 0),
      base_backoff_ms: Number(cfg.retry_policy?.base_backoff_ms ?? 1000),
    },
  };
}

function selectedRectangle() {
  const selectedId = window.data?.selectedRectangle;
  return getPanelItems().find((item) => String(item.id) === String(selectedId)) || null;
}

/** Detect if this API node is in ETL mode (connected to a warehouse Target) */
function detectMode() {
  const rect = selectedRectangle();
  if (!rect) return "service";
  const items = getPanelItems();
  // Check if any downstream node is a Target (Tg)
  const children = items.filter(i => String(i.parent) === String(rect.id));
  for (const child of children) {
    if (child.btype === "Tg" || child.btype === "O") {
      // Check if there's a target connected to output
      const grandchildren = items.filter(i => String(i.parent) === String(child.id));
      if (child.btype === "Tg") return "etl";
      if (grandchildren.some(gc => gc.btype === "Tg")) return "etl";
    }
  }
  // Also check parent chain — API → Output → Target
  const output = items.find(i => i.btype === "O");
  if (output) {
    const targets = items.filter(i => String(i.parent) === String(output.id) && i.btype === "Tg");
    if (targets.length > 0) return "etl";
  }
  return "service";
}

class ApiComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.savedState = null;
    this.discovered = [];
    this.mode = "service";
    this.endpointTabState = [];
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this.bindElements();
    this.closeImmediately();
  }

  disconnectedCallback() { EventHandler.removeGroup("API"); }

  attributeChangedCallback(name, _oldVal, newVal) {
    if (name !== "visibility") return;
    newVal === "open" ? this.open() : this.close();
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.saveButton = sr.querySelector("#saveButton");
    this.runButton = sr.querySelector("#runButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.endpointList = sr.querySelector("#endpointList");
    this.statusText = sr.querySelector("#statusText");
    this.discoverSpecUrl = sr.querySelector("#discoverSpecUrl");
    this.modeBadge = sr.querySelector("#modeBadge");
    this.modeSubtitle = sr.querySelector("#modeSubtitle");
    this.advancedTab = sr.querySelector("#advancedTab");
    this.epTableWrap = sr.querySelector("#endpointTableWrap");
    this.epTableBody = sr.querySelector("#epTableBody");
    this.epSearch = sr.querySelector("#epSearch");
    this.epMethodFilter = sr.querySelector("#epMethodFilter");
    this.epSelectAll = sr.querySelector("#epSelectAll");
    this.epSelectionCount = sr.querySelector("#epSelectionCount");
  }

  async open() {
    this.style.display = "block";
    this.mode = detectMode();
    this.discovered = [];
    this.connSelectedEndpoints = []; // endpoints pre-selected during left-tree API setup

    // Fetch rich node data from server (panelItems only has basic fields)
    const rect = selectedRectangle() || {};
    let nodeData = {};
    if (rect.id) {
      try {
        nodeData = await request(`/getItem?id=${rect.id}`);
      } catch (e) {
        console.warn("Could not load node data:", e);
      }
    }

    // Build initial state from rich node data
    this.state = {
      id: rect.id || null,
      api_name: nodeData.api_name || "",
      source_system: nodeData.source_system || "",
      base_url: nodeData.base_url || "http://localhost:3001",
      specification_url: nodeData.specification_url || "",
      auth_ref: {
        type: nodeData.auth_ref?.type || "bearer",
        secret_ref: nodeData.auth_ref?.secret_ref || "",
        token: nodeData.auth_ref?.token || nodeData.auth_ref?.key || "mock-samsara-token",
        location: nodeData.auth_ref?.location || "header",
        param_name: nodeData.auth_ref?.param_name || "api_key",
        header_name: nodeData.auth_ref?.header_name || "X-API-Key",
      },
      endpoint_configs: (nodeData.endpoint_configs || []).map((cfg) => {
        const merged = {
          ...DEFAULT_ENDPOINT(),
          ...cfg,
          primary_key_fields: Array.isArray(cfg.primary_key_fields) ? cfg.primary_key_fields : ["id"],
          selected_nodes: Array.isArray(cfg.selected_nodes) ? cfg.selected_nodes : [],
          inferred_fields: Array.isArray(cfg.inferred_fields) ? cfg.inferred_fields : [],
          query_params: cfg.query_params || {},
          request_headers: cfg.request_headers || {},
          body_params: cfg.body_params || {},
          json_explode_rules: cfg.json_explode_rules || [],
          retry_policy: cfg.retry_policy || { max_retries: 3, base_backoff_ms: 1000 },
        };
        // Auto-mark watermark checkboxes on timestamp fields and derive watermark_column
        if (merged.inferred_fields.length) {
          autoMarkWatermarkFields(merged.inferred_fields);
          autoApplyPrimaryKeyFields(merged);
          if (!merged.watermark_column) {
            const wmFields = merged.inferred_fields.filter(f => f.is_watermark && f.enabled !== false).map(f => f.column_name);
            if (wmFields.length) merged.watermark_column = wmFields.join(",");
          }
        }
        return merged;
      }),
    };

    // Load connection data for spec URL, auth, and pre-selected endpoints
    const connId = rect.conn_id || rect.c;
    if (connId) {
      try {
        const conn = await request(`/getConnectionDetail?conn_id=${connId}`);
        if (conn.host && !this.state.specification_url) this.state.specification_url = conn.host;
        if (conn.connection_name && !this.state.api_name) this.state.api_name = conn.connection_name;
        if (conn.username && !this.state.auth_ref.secret_ref) this.state.auth_ref.secret_ref = conn.username;
        if (conn.password && !this.state.auth_ref.token) this.state.auth_ref.token = conn.password;
        if (conn.schema && !this.state.auth_ref.type) {
          // Normalize auth type from left-tree format (apikey → api-key)
          const authMap = { apikey: "api-key", bearer: "bearer", basic: "bearer", noauth: "", oauth2: "bearer" };
          this.state.auth_ref.type = authMap[conn.schema] ?? conn.schema;
        }
        if (Array.isArray(conn.selected_endpoints)) {
          this.connSelectedEndpoints = conn.selected_endpoints;
        }
      } catch (e) {
        console.warn("Could not load connection data:", e);
      }
    }

    this.savedState = stableStateSnapshot(this.state);
    this.updateModeUI();
    this.render();
    this.setEvents();

    // Auto-discover from spec if URL is set
    if (this.state.specification_url) {
      this.discoverSpecUrl.value = this.state.specification_url;
      await this.discoverAndInfer();
    }
  }

  updateModeUI() {
    if (this.mode === "etl") {
      this.modeBadge.textContent = "ETL";
      this.modeBadge.className = "mode-badge mode-etl";
      this.modeSubtitle.textContent = "Connected to a warehouse target. Showing ETL configuration.";
      this.advancedTab.style.display = "";
      this.advancedTab.textContent = "ETL / Schema";
    } else {
      this.modeBadge.textContent = "SERVICE";
      this.modeBadge.className = "mode-badge mode-service";
      this.modeSubtitle.textContent = "Microservice orchestration mode. Configure endpoints and request/response.";
      this.advancedTab.style.display = "none";
    }
  }

  closeImmediately() { this.style.display = "none"; }

  async close() {
    if (this.style.display === "none") return;
    if (stableStateSnapshot(this.state) !== this.savedState) {
      const discard = await customConfirm("Discard unsaved API changes?");
      if (!discard) return;
    }
    this.closeImmediately();
  }

  setEvents() {
    EventHandler.removeGroup("API");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "API");
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "API");
    EventHandler.on(this.runButton, "click", () => this.run(), false, "API");
    EventHandler.on(this.shadowRoot.querySelector("#addEndpointButton"), "click", () => {
      this.state.endpoint_configs.push(DEFAULT_ENDPOINT());
      this.renderEndpoints();
      this.markDirty();
    }, false, "API");
    EventHandler.on(this.shadowRoot.querySelector("#discoverButton"), "click", () => this.discoverAndInfer(), false, "API");

    // Endpoint table: search, filter, select-all
    EventHandler.on(this.epSearch, "input", () => this.filterEndpointTable(), false, "API");
    EventHandler.on(this.epMethodFilter, "change", () => this.filterEndpointTable(), false, "API");
    EventHandler.on(this.epSelectAll, "change", () => {
      const checked = this.epSelectAll.checked;
      // Only toggle visible rows
      this.epTableBody.querySelectorAll("tr.ep-row").forEach(tr => {
        if (tr.style.display === "none") return;
        const cb = tr.querySelector(".ep-check");
        if (cb && cb.checked !== checked) {
          cb.checked = checked;
          this.handleEndpointToggle(tr.dataset.endpoint, tr.dataset.method, checked);
        }
      });
      this.updateSelectionCount();
      this.renderEndpoints();
      this.markDirty();
    }, false, "API");

    // Auth toggle
    const authToggle = this.shadowRoot.querySelector("#authToggle");
    const authBody = this.shadowRoot.querySelector("#authBody");
    EventHandler.on(authToggle, "click", () => {
      authToggle.classList.toggle("open");
      authBody.classList.toggle("open");
    }, false, "API");

    // Main tabs
    this.shadowRoot.querySelectorAll("#mainTabs .tab").forEach(tab => {
      EventHandler.on(tab, "click", () => this.switchTab(tab.dataset.tab), false, "API");
    });

    // Root fields
    this.shadowRoot.querySelectorAll("[data-field]").forEach((el) => {
      EventHandler.on(el, el.tagName === "TEXTAREA" || el.tagName === "INPUT" ? "input" : "change", (event) => {
        this.updateRootField(event.target.dataset.field, event.target.type === "checkbox" ? event.target.checked : event.target.value);
      }, false, "API");
    });
  }

  switchTab(tabName) {
    this.shadowRoot.querySelectorAll("#mainTabs .tab").forEach(t => t.classList.toggle("active", t.dataset.tab === tabName));
    this.shadowRoot.querySelectorAll(".tab-panel").forEach(p => p.classList.toggle("active", p.id === `panel-${tabName}`));
  }

  getEndpointTab(index) {
    return this.endpointTabState[index] || "basic";
  }

  setEndpointTab(index, tabName) {
    this.endpointTabState[index] = tabName;
  }

  markDirty() {
    this.saveButton.disabled = stableStateSnapshot(this.state) === this.savedState;
  }

  updateRootField(field, value) {
    if (field.startsWith("auth_ref.")) {
      this.state.auth_ref[field.replace("auth_ref.", "")] = value;
    } else {
      this.state[field] = value;
      if (field === "specification_url" && !this.discoverSpecUrl.value) {
        this.discoverSpecUrl.value = value;
      }
    }
    this.markDirty();
  }

  render() {
    const sr = this.shadowRoot;
    sr.querySelector("#apiName").value = this.state.api_name;
    sr.querySelector("#sourceSystem").value = this.state.source_system;
    sr.querySelector("#baseUrl").value = this.state.base_url;
    sr.querySelector("#specificationUrl").value = this.state.specification_url;
    sr.querySelector("#discoverSpecUrl").value = this.state.specification_url;
    sr.querySelector("#authType").value = this.state.auth_ref.type || "";
    sr.querySelector("#authSecretRef").value = this.state.auth_ref.secret_ref || "";
    sr.querySelector("#authToken").value = this.state.auth_ref.token || "";
    sr.querySelector("#authLocation").value = this.state.auth_ref.location || "header";
    sr.querySelector("#authParamName").value = this.state.auth_ref.param_name || "api_key";
    sr.querySelector("#authHeaderName").value = this.state.auth_ref.header_name || "X-API-Key";
    this.renderEndpoints();
    this.renderRunOptions();
    if (this.mode === "etl") this.renderAdvanced();
    this.markDirty();
  }

  endpointHtml(cfg, index) {
    const name = cfg.endpoint_name || `Endpoint ${index + 1}`;
    const activeTab = this.getEndpointTab(index);
    return `
      <div class="endpoint-card" data-index="${index}">
        <div class="endpoint-head">
          <div>
            <strong>${escapeHtml(name)}</strong>
            <div class="hint" style="margin-top:4px;">${escapeHtml(inferredColumnsLabel(cfg))} map to one Bronze table: <span class="mono">${escapeHtml(bronzeTableLabel(cfg))}</span></div>
            ${cfg.latest_run_result ? `<div class="hint" style="margin-top:4px;">${escapeHtml(endpointRunSummary(cfg))}</div>` : ""}
          </div>
          <div class="row">
            <button class="secondary preview-schema" type="button">Preview Schema</button>
            <button class="secondary remove-endpoint" type="button">Remove</button>
          </div>
        </div>

        <!-- Endpoint sub-tabs -->
        <div class="tabs ep-tabs" data-ep-index="${index}">
          <div class="tab ${activeTab === "basic" ? "active" : ""}" data-ep-tab="basic">Basic</div>
          <div class="tab ${activeTab === "pagination" ? "active" : ""}" data-ep-tab="pagination">Pagination</div>
          <div class="tab ${activeTab === "request" ? "active" : ""}" data-ep-tab="request">Request</div>
          <div class="tab ${activeTab === "schema" ? "active" : ""}" data-ep-tab="schema">Schema</div>
        </div>

        <!-- Basic -->
        <div class="tab-panel ep-panel ${activeTab === "basic" ? "active" : ""}" data-ep-index="${index}" data-ep-panel="basic">
          <div class="grid">
            <div>
              <label>Endpoint Name</label>
              <input data-endpoint="${index}" data-key="endpoint_name" type="text" value="${escapeHtml(cfg.endpoint_name || "")}" />
            </div>
            <div>
              <label>Endpoint URL</label>
              <input data-endpoint="${index}" data-key="endpoint_url" type="text" value="${escapeHtml(cfg.endpoint_url || "")}" />
            </div>
            <div>
              <label>HTTP Method</label>
              <select data-endpoint="${index}" data-key="http_method">
                ${["GET", "POST", "PUT", "PATCH", "DELETE"].map(m => `<option value="${m}" ${m === (cfg.http_method || "GET") ? "selected" : ""}>${m}</option>`).join("")}
              </select>
            </div>
            <div>
              <label>Load Type</label>
              <select data-endpoint="${index}" data-key="load_type">
                ${["full", "incremental", "snapshot"].map(v => `<option value="${v}" ${v === (cfg.load_type || "full") ? "selected" : ""}>${v}</option>`).join("")}
              </select>
            </div>
            <div>
              <label>Primary Keys (CSV)</label>
              <input data-endpoint="${index}" data-key="primary_key_fields" type="text" value="${(cfg.primary_key_fields || []).join(", ")}" />
            </div>
            <div class="inline">
              <label>Enabled</label>
              <input data-endpoint="${index}" data-key="enabled" type="checkbox" ${cfg.enabled !== false ? "checked" : ""} />
            </div>
          </div>
        </div>

        <!-- Pagination -->
        <div class="tab-panel ep-panel ${activeTab === "pagination" ? "active" : ""}" data-ep-index="${index}" data-ep-panel="pagination">
          <div class="grid">
            <div>
              <label>Strategy</label>
              <select data-endpoint="${index}" data-key="pagination_strategy">
                ${["none", "offset", "page", "cursor", "token", "time", "link-header"].map(v => `<option value="${v}" ${v === (cfg.pagination_strategy || "none") ? "selected" : ""}>${v}</option>`).join("")}
              </select>
            </div>
            <div>
              <label>Location</label>
              <select data-endpoint="${index}" data-key="pagination_location">
                ${["query", "body"].map(v => `<option value="${v}" ${v === (cfg.pagination_location || "query") ? "selected" : ""}>${v}</option>`).join("")}
              </select>
            </div>
            <div>
              <label>Page Size</label>
              <input data-endpoint="${index}" data-key="page_size" type="number" value="${cfg.page_size ?? 100}" />
            </div>
            <div>
              <label>Cursor Field</label>
              <input data-endpoint="${index}" data-key="cursor_field" type="text" value="${escapeHtml(cfg.cursor_field || "")}" />
            </div>
            <div>
              <label>Cursor Param</label>
              <input data-endpoint="${index}" data-key="cursor_param" type="text" value="${escapeHtml(cfg.cursor_param || "cursor")}" />
            </div>
            <div>
              <label>Token Field</label>
              <input data-endpoint="${index}" data-key="token_field" type="text" value="${escapeHtml(cfg.token_field || "")}" />
            </div>
            <div>
              <label>Token Param</label>
              <input data-endpoint="${index}" data-key="token_param" type="text" value="${escapeHtml(cfg.token_param || "page_token")}" />
            </div>
            <div>
              <label>Time Field</label>
              <input data-endpoint="${index}" data-key="time_field" type="text" value="${escapeHtml(cfg.time_field || "")}" />
            </div>
            <div>
              <label>Time Param</label>
              <input data-endpoint="${index}" data-key="time_param" type="text" value="${escapeHtml(cfg.time_param || "updated_since")}" />
            </div>
            <div>
              <label>Time Window Minutes</label>
              <input data-endpoint="${index}" data-key="time_window_minutes" type="number" value="${cfg.time_window_minutes ?? 60}" />
            </div>
          </div>
        </div>

        <!-- Request -->
        <div class="tab-panel ep-panel ${activeTab === "request" ? "active" : ""}" data-ep-index="${index}" data-ep-panel="request">
          <div class="grid">
            <div>
              <label>Query Params JSON</label>
              <textarea data-endpoint="${index}" data-key="query_params">${PRETTY_JSON(cfg.query_params, {})}</textarea>
            </div>
            <div>
              <label>Request Headers JSON</label>
              <textarea data-endpoint="${index}" data-key="request_headers">${PRETTY_JSON(cfg.request_headers, {})}</textarea>
            </div>
            <div>
              <label>Body Params JSON</label>
              <textarea data-endpoint="${index}" data-key="body_params">${PRETTY_JSON(cfg.body_params, {})}</textarea>
            </div>
            <div>
              <label>Selected Nodes (CSV)</label>
              <input data-endpoint="${index}" data-key="selected_nodes" type="text" value="${(cfg.selected_nodes || []).join(", ")}" />
            </div>
            <div>
              <label>Max Retries</label>
              <input data-endpoint="${index}" data-key="retry_policy.max_retries" type="number" value="${cfg.retry_policy?.max_retries ?? 3}" />
            </div>
            <div>
              <label>Base Backoff Ms</label>
              <input data-endpoint="${index}" data-key="retry_policy.base_backoff_ms" type="number" value="${cfg.retry_policy?.base_backoff_ms ?? 1000}" />
            </div>
          </div>
        </div>

        <!-- Schema -->
        <div class="tab-panel ep-panel ${activeTab === "schema" ? "active" : ""}" data-ep-index="${index}" data-ep-panel="schema">
          ${cfg.schema_preview_status ? `<div style="margin-bottom:8px; font-size:12px; color:#5c6070;">${escapeHtml(cfg.schema_preview_status)}</div>` : ""}
          ${this._renderRecommendationCard(cfg, index)}
          <div class="hint" style="margin-bottom:8px;">Preview rows below become columns in one Bronze table for this endpoint.</div>
          <div class="grid">
            <div>
              <label>Schema Mode</label>
              <select data-endpoint="${index}" data-key="schema_mode">
                ${["manual", "infer", "hybrid"].map(v => `<option value="${v}" ${v === (cfg.schema_mode || "manual") ? "selected" : ""}>${v}</option>`).join("")}
              </select>
            </div>
            <div>
              <label>Schema Evolution</label>
              <select data-endpoint="${index}" data-key="schema_evolution_mode">
                ${["none", "advisory", "additive"].map(v => `<option value="${v}" ${v === (cfg.schema_evolution_mode || "advisory") ? "selected" : ""}>${v}</option>`).join("")}
              </select>
            </div>
            <div>
              <label>Sample Records</label>
              <input data-endpoint="${index}" data-key="sample_records" type="number" value="${cfg.sample_records ?? 100}" />
            </div>
            <div>
              <label>Max Inferred Columns</label>
              <input data-endpoint="${index}" data-key="max_inferred_columns" type="number" value="${cfg.max_inferred_columns ?? 100}" />
            </div>
            <div class="inline">
              <label>Type Inference</label>
              <input data-endpoint="${index}" data-key="type_inference_enabled" type="checkbox" ${cfg.type_inference_enabled !== false ? "checked" : ""} />
            </div>
            <div>
              <label>Bronze Table</label>
              <input data-endpoint="${index}" data-key="bronze_table_name" type="text" value="${escapeHtml(cfg.bronze_table_name || bronzeTableFromRunResult(cfg.latest_run_result) || autoEndpointBronzeTableName(cfg))}" />
              <div class="hint" style="margin-top:4px;">Leave blank to use the resolved Bronze table shown here.</div>
            </div>
            <div>
              <label>Silver Table</label>
              <input data-endpoint="${index}" data-key="silver_table_name" type="text" value="${escapeHtml(cfg.silver_table_name || "")}" />
            </div>
            <div>
              <label>Watermark Columns</label>
              <input data-endpoint="${index}" data-key="watermark_column" type="text" value="${escapeHtml(cfg.watermark_column || "")}"
                placeholder="Check WM boxes in schema below" style="background:#f5f5f0;" />
              <div class="hint" style="margin-top:4px;">Derived from WM checkboxes in the schema table. Populates <code>event_time_utc</code> in Bronze.</div>
              ${(cfg.load_type || "incremental") !== "full" && !cfg.watermark_column
                ? `<div class="hint" style="margin-top:4px; color:#b8860b; font-weight:600;">Warning: ${cfg.load_type || "incremental"} load without a watermark will re-fetch all data. Check WM boxes below.</div>`
                : ""}
            </div>
            <div>
              <label>Watermark API Param</label>
              <input data-endpoint="${index}" data-key="watermark_param" type="text" value="${escapeHtml(cfg.watermark_param || "")}"
                placeholder="e.g. updatedAfter" />
              <div class="hint" style="margin-top:4px;">API query parameter sent on incremental requests. If blank, uses watermark column name.</div>
            </div>
            <div>
              <label>Overlap Minutes</label>
              <input data-endpoint="${index}" data-key="watermark_overlap_minutes" type="number" value="${cfg.watermark_overlap_minutes ?? 0}" />
            </div>
            <div>
              <label>Explode Rules JSON</label>
              <textarea data-endpoint="${index}" data-key="json_explode_rules">${PRETTY_JSON(cfg.json_explode_rules, [])}</textarea>
            </div>
          </div>
          <div style="margin-top:10px; overflow:auto;">
            <table style="width:100%; border-collapse:collapse; font-size:12px;">
              <thead>
                <tr>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">On</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Path</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Column</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Type</th>
                  <th style="text-align:center; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;" title="Watermark — used for incremental load">WM</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Override</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Coverage</th>
                </tr>
              </thead>
              <tbody>
                ${(cfg.inferred_fields || []).map((field, fi) => `
                  <tr>
                    <td><input data-inferred-endpoint="${index}" data-inferred-index="${fi}" data-inferred-key="enabled" type="checkbox" ${field.enabled !== false ? "checked" : ""} /></td>
                    <td class="mono">${escapeHtml(field.path || "")}</td>
                    <td><input data-inferred-endpoint="${index}" data-inferred-index="${fi}" data-inferred-key="column_name" type="text" value="${escapeHtml(field.column_name || "")}" /></td>
                    <td class="mono">${escapeHtml(field.type || "STRING")}</td>
                    <td style="text-align:center;"><input data-inferred-endpoint="${index}" data-inferred-index="${fi}" data-inferred-key="is_watermark" type="checkbox" ${field.is_watermark ? "checked" : ""} /></td>
                    <td><input data-inferred-endpoint="${index}" data-inferred-index="${fi}" data-inferred-key="override_type" type="text" value="${escapeHtml(field.override_type || "")}" placeholder="optional" /></td>
                    <td>${Number(field.sample_coverage || 0).toFixed(2)}</td>
                  </tr>
                `).join("") || '<tr><td colspan="7" class="hint" style="padding:8px;">No inferred fields. Use Preview Schema.</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    `;
  }

  /** Compute grain/PK/watermark recommendations from inferred fields (client-side heuristics).
   *  In Phase 2, this will come from the backend grain_planner.clj. */
  _computeRecommendations(cfg) {
    if (cfg?.schema_recommendations) return cfg.schema_recommendations;
    const fields = cfg.inferred_fields || [];
    if (!fields.length) return null;

    // Already have explode rules, PK, and watermark — score existing config
    const hasExplode = cfg.json_explode_rules?.length > 0;
    const hasPK = cfg.primary_key_fields?.length > 0 && cfg.primary_key_fields[0] !== "";
    const hasWM = !!cfg.watermark_column;

    // Detect record grain from field paths
    const arrayPaths = new Map();
    for (const f of fields) {
      const match = (f.path || "").match(/^([^[]+)\[\]/);
      if (match) {
        const p = match[1];
        if (!arrayPaths.has(p)) arrayPaths.set(p, { path: p, fields: [], idCandidates: [], tsCandidates: [] });
        const entry = arrayPaths.get(p);
        entry.fields.push(f);
        if (f.type === "STRING" && /(?:^id$|_id$|_key$)/i.test(f.column_name)) entry.idCandidates.push(f.column_name);
        if (/TIMESTAMP|DATE/i.test(f.type || "")) entry.tsCandidates.push(f.column_name);
        if (/updated|modified/i.test(f.column_name)) entry.tsCandidates.push(f.column_name);
      }
    }

    if (arrayPaths.size === 0) return null;

    // Score candidates
    const candidates = [];
    for (const [path, info] of arrayPaths) {
      let score = 50; // base: it's an array of objects
      if (info.idCandidates.length > 0) score += 30;
      if (info.tsCandidates.length > 0) score += 20;
      if (info.fields.length > 5) score += 10;
      // Penalty for very nested paths
      const depth = (path.match(/\./g) || []).length;
      if (depth > 1) score -= 15;
      candidates.push({ ...info, score });
    }

    candidates.sort((a, b) => b.score - a.score);
    const best = candidates[0];
    if (!best) return null;

    // Detect child entities (nested arrays within the best candidate)
    const childCandidates = [];
    for (const [path, info] of arrayPaths) {
      if (path !== best.path && path.startsWith(best.path + ".")) {
        childCandidates.push({
          path,
          entityName: path.split(".").pop().toLowerCase(),
          idCandidates: info.idCandidates,
          parentKeys: best.idCandidates,
          confidence: Math.min(info.score, 85),
        });
      }
    }

    // Build recommendations
    const rec = {
      grain: { path: best.path, confidence: best.score },
      pk: best.idCandidates.length > 0
        ? { fields: [best.idCandidates[0]], confidence: best.score >= 80 ? 90 : 65 }
        : null,
      watermark: null,
      children: childCandidates,
      reasons: [],
    };

    // Watermark: prefer "updated" variants
    const wmCandidates = [...new Set(best.tsCandidates)];
    const updatedWM = wmCandidates.find(c => /updated/i.test(c));
    if (updatedWM) {
      rec.watermark = { field: updatedWM, confidence: 85 };
    } else if (wmCandidates.length > 0) {
      rec.watermark = { field: wmCandidates[0], confidence: 60 };
    }

    // Reasons
    rec.reasons.push(`${best.path}[] is an array of ${best.fields.length}-field objects`);
    if (rec.pk) rec.reasons.push(`${rec.pk.fields[0]} is a stable ID candidate`);
    if (rec.watermark) rec.reasons.push(`${rec.watermark.field} is a timestamp with high coverage`);
    if (childCandidates.length) rec.reasons.push(`${childCandidates.length} nested child array(s) detected`);

    return rec;
  }

  _renderRecommendationCard(cfg, index) {
    const rec = this._computeRecommendations(cfg);
    if (!rec) return "";

    const badgeHtml = (confidence) => {
      if (confidence >= 80) return `<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;font-family:'JetBrains Mono',monospace;background:rgba(15,169,104,0.1);color:#0fa968;">Auto-detected</span>`;
      if (confidence >= 50) return `<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;font-family:'JetBrains Mono',monospace;background:rgba(217,119,6,0.1);color:#d97706;">Suggested</span>`;
      return `<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;font-family:'JetBrains Mono',monospace;background:rgba(139,145,163,0.1);color:#8b91a3;">Manual</span>`;
    };

    const grainLine = `<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;">
      <span><strong>Record Grain:</strong> <code style="font-family:'JetBrains Mono',monospace;font-size:11px;background:#f5f6f8;padding:2px 6px;border-radius:4px;">${escapeHtml(rec.grain.path)}[]</code></span>
      ${badgeHtml(rec.grain.confidence)}
    </div>`;

    const pkLine = rec.pk
      ? `<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;">
          <span><strong>Primary Key:</strong> <code style="font-family:'JetBrains Mono',monospace;font-size:11px;background:#f5f6f8;padding:2px 6px;border-radius:4px;">${escapeHtml(rec.pk.fields.join(", "))}</code></span>
          ${badgeHtml(rec.pk.confidence)}
        </div>`
      : `<div style="padding:4px 0;color:#8b91a3;"><strong>Primary Key:</strong> <em>No stable key detected — manual selection required</em></div>`;

    const wmLine = rec.watermark
      ? `<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;">
          <span><strong>Watermark:</strong> <code style="font-family:'JetBrains Mono',monospace;font-size:11px;background:#f5f6f8;padding:2px 6px;border-radius:4px;">${escapeHtml(rec.watermark.field)}</code></span>
          ${badgeHtml(rec.watermark.confidence)}
        </div>`
      : `<div style="padding:4px 0;color:#8b91a3;"><strong>Watermark:</strong> <em>No timestamp detected — consider full load</em></div>`;

    const reasonsHtml = rec.reasons.length
      ? `<details style="margin-top:8px;font-size:11px;color:#5c6070;">
          <summary style="cursor:pointer;font-weight:600;color:#3b7ddd;">Show reasoning</summary>
          <ul style="margin:6px 0 0 16px;padding:0;list-style:none;">
            ${rec.reasons.map(r => `<li style="padding:2px 0;display:flex;gap:6px;"><span style="color:#0fa968;">&#10003;</span> ${escapeHtml(r)}</li>`).join("")}
          </ul>
        </details>`
      : "";

    const childrenHtml = rec.children.length
      ? `<details style="margin-top:10px;border-top:1px solid #eceef2;padding-top:10px;">
          <summary style="cursor:pointer;font-size:12px;font-weight:600;color:#7c5cfc;">
            Possible child entities (${rec.children.length})
          </summary>
          ${rec.children.map(child => `
            <div style="margin-top:8px;padding:10px 12px;background:#f9fafb;border:1px solid #eceef2;border-radius:8px;">
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <strong style="font-size:12px;">${escapeHtml(child.entityName)}</strong>
                ${badgeHtml(child.confidence)}
              </div>
              <div style="font-size:11px;color:#5c6070;margin-top:4px;">
                Path: <code style="font-family:'JetBrains Mono',monospace;font-size:10px;background:#f0f1f4;padding:1px 4px;border-radius:3px;">${escapeHtml(child.path)}[]</code>
              </div>
              ${child.parentKeys.length ? `<div style="font-size:11px;color:#5c6070;">Keys: ${escapeHtml([...child.parentKeys, ...child.idCandidates].join(" + "))}</div>` : ""}
              <button class="create-child-endpoint" data-parent-index="${index}" data-child-path="${escapeHtml(child.path)}" data-child-name="${escapeHtml(child.entityName)}" data-child-keys="${escapeHtml(JSON.stringify([...child.parentKeys, ...child.idCandidates]))}"
                style="margin-top:6px;padding:4px 12px;border:1px solid #e2e4ea;border-radius:6px;background:#fff;color:#7c5cfc;font-size:11px;font-weight:600;cursor:pointer;font-family:'DM Sans',sans-serif;">
                Create as separate endpoint
              </button>
            </div>
          `).join("")}
        </details>`
      : "";

    // AI assist buttons (P1-A: Explain, P1-B: Refine, P3-A: Business Shape)
    const aiTriggers = `
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;border-top:1px solid #eceef2;padding-top:10px;">
        ${renderAiTrigger("Explain with AI", "explain_preview", index)}
        ${renderAiTrigger("Refine recommendation", "suggest_keys", index)}
        ${renderAiTrigger("What does this endpoint represent?", "business_shape", index)}
      </div>`;

    // AI result placeholder (filled dynamically)
    const aiResultId = `ai-result-${index}`;
    const aiResultSlot = `<div id="${aiResultId}"></div>`;

    return `
      <div style="margin-bottom:12px;padding:14px 16px;background:#fff;border:1px solid #e2e4ea;border-radius:10px;border-left:3px solid #0fa968;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
        <div style="font-size:12px;font-weight:600;color:#1a1d26;margin-bottom:8px;">Grain Recommendation</div>
        ${grainLine}
        ${pkLine}
        ${wmLine}
        ${reasonsHtml}
        ${childrenHtml}
        ${aiTriggers}
        ${aiResultSlot}
      </div>
    `;
  }

  renderEndpoints() {
    this.endpointTabState = this.state.endpoint_configs.map((_, index) => this.getEndpointTab(index));
    this.endpointList.innerHTML = this.state.endpoint_configs.map((cfg, i) => this.endpointHtml(cfg, i)).join("");

    // Endpoint field events
    this.endpointList.querySelectorAll("[data-endpoint]").forEach((el) => {
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (event) => this.updateEndpointField(event), false, "API");
      if (el.type === "checkbox") {
        EventHandler.on(el, "change", (event) => this.updateEndpointField(event), false, "API");
      }
    });

    // Inferred field events
    this.endpointList.querySelectorAll("[data-inferred-endpoint]").forEach((el) => {
      EventHandler.on(el, el.type === "checkbox" ? "change" : "input", (event) => this.updateInferredField(event), false, "API");
    });

    // Remove buttons
    this.endpointList.querySelectorAll(".remove-endpoint").forEach((btn) => {
      EventHandler.on(btn, "click", (event) => {
        const idx = Number(event.target.closest(".endpoint-card").dataset.index);
        this.state.endpoint_configs.splice(idx, 1);
        this.endpointTabState.splice(idx, 1);
        if (!this.state.endpoint_configs.length) this.state.endpoint_configs.push(DEFAULT_ENDPOINT());
        this.renderEndpoints();
        this.markDirty();
      }, false, "API");
    });

    // Create child endpoint buttons
    this.endpointList.querySelectorAll(".create-child-endpoint").forEach((btn) => {
      EventHandler.on(btn, "click", (event) => {
        const el = event.target;
        const parentIdx = Number(el.dataset.parentIndex);
        const childPath = el.dataset.childPath;
        const childName = el.dataset.childName;
        let childKeys = [];
        try { childKeys = JSON.parse(el.dataset.childKeys); } catch (_) {}

        const parent = this.state.endpoint_configs[parentIdx];
        if (!parent) return;

        const child = {
          ...DEFAULT_ENDPOINT(),
          endpoint_name: `${parent.endpoint_name}.${childName}`,
          endpoint_url: parent.endpoint_url,
          http_method: parent.http_method,
          load_type: parent.load_type,
          pagination_strategy: parent.pagination_strategy,
          cursor_field: parent.cursor_field,
          cursor_param: parent.cursor_param,
          json_explode_rules: [{ path: childPath }],
          primary_key_fields: childKeys,
          schema_mode: "infer",
        };

        this.state.endpoint_configs.push(child);
        this.renderEndpoints();
        this.markDirty();
        this.statusText.textContent = `Created child endpoint "${child.endpoint_name}" from parent recommendation.`;
        this.statusText.style.color = "#7c5cfc";
      }, false, "API");
    });

    // Preview schema buttons
    this.endpointList.querySelectorAll(".preview-schema").forEach((btn) => {
      EventHandler.on(btn, "click", (event) => {
        const idx = Number(event.target.closest(".endpoint-card").dataset.index);
        this.previewSchema(idx);
      }, false, "API");
    });

    // AI assist triggers (P1-A / P1-B)
    bindAiTriggers(this.endpointList, async (action, index, btn) => {
      const cfg = this.state.endpoint_configs[index];
      if (!cfg?.inferred_fields?.length) return;
      const resultEl = this.endpointList.querySelector(`#ai-result-${index}`);
      if (!resultEl) return;
      const loadingMsg = { explain_preview: "Explaining schema...", suggest_keys: "Refining recommendations...", business_shape: "Analyzing business entity..." }[action] || "Thinking...";
      resultEl.innerHTML = renderAiLoading(loadingMsg);
      try {
        const endpointMap = { explain_preview: "/aiExplainPreviewSchema", suggest_keys: "/aiSuggestBronzeKeys", business_shape: "/aiExplainEndpointBusinessShape" };
        const result = await callAiEndpoint(endpointMap[action], {
          endpoint_config: cfg,
          inferred_fields: cfg.inferred_fields,
        });
        const titleMap = { explain_preview: "Schema Explanation", suggest_keys: "Key Recommendations", business_shape: "Business Entity Analysis" };
        resultEl.innerHTML = renderAiCard(result, { title: titleMap[action] });
      } catch (err) {
        resultEl.innerHTML = `<div class="ai-warnings">&#9888; ${err.message || "AI request failed"}</div>`;
      }
    });

    // Endpoint sub-tabs
    this.endpointList.querySelectorAll(".ep-tabs .tab").forEach(tab => {
      EventHandler.on(tab, "click", (e) => {
        const epIndex = Number(e.target.closest(".ep-tabs").dataset.epIndex);
        const tabName = e.target.dataset.epTab;
        this.setEndpointTab(epIndex, tabName);
        const card = e.target.closest(".endpoint-card");
        card.querySelectorAll(".ep-tabs .tab").forEach(t => t.classList.toggle("active", t.dataset.epTab === tabName));
        card.querySelectorAll(".ep-panel").forEach(p => p.classList.toggle("active", p.dataset.epPanel === tabName));
      }, false, "API");
    });
  }

  renderAdvanced() {
    // Advanced tab shows summary of ETL config per endpoint
    const list = this.shadowRoot.querySelector("#advancedEndpointList");
    if (!list) return;
    list.innerHTML = this.state.endpoint_configs.map((cfg, i) => `
      <div class="card" style="margin-bottom:8px;">
        <strong>${escapeHtml(cfg.endpoint_name || `Endpoint ${i + 1}`)}</strong>
        <div class="grid" style="margin-top:8px;">
          <div><label>Bronze</label><span class="mono">${escapeHtml(bronzeTableLabel(cfg))}</span></div>
          <div><label>Silver</label><span class="mono">${escapeHtml(cfg.silver_table_name || "(auto)")}</span></div>
          <div><label>Schema</label><span class="mono">${cfg.schema_mode || "manual"}</span></div>
          <div><label>Columns</label><span class="mono">${escapeHtml(inferredColumnsLabel(cfg))}</span></div>
        </div>
      </div>
    `).join("");
  }

  renderRunOptions() {
    const existing = this.shadowRoot.querySelector("#runEndpointName");
    if (!existing) return;
    existing.innerHTML = `<option value="">Run all enabled</option>${this.state.endpoint_configs.map(cfg =>
      `<option value="${escapeHtml(cfg.endpoint_name || "")}">${escapeHtml(cfg.endpoint_name || cfg.endpoint_url || "unnamed")}</option>`
    ).join("")}`;
  }

  updateEndpointField(event) {
    const idx = Number(event.target.dataset.endpoint);
    const key = event.target.dataset.key;
    const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
    const endpoint = this.state.endpoint_configs[idx];
    if (!endpoint) return;

    if (key === "primary_key_fields" || key === "selected_nodes") {
      endpoint[key] = value.split(",").map(s => s.trim()).filter(Boolean);
    } else if (key.startsWith("retry_policy.")) {
      endpoint.retry_policy = endpoint.retry_policy || {};
      endpoint.retry_policy[key.replace("retry_policy.", "")] = Number(value || 0);
    } else if (["query_params", "request_headers", "body_params", "json_explode_rules"].includes(key)) {
      endpoint[key] = value;
    } else if (["page_size", "time_window_minutes", "watermark_overlap_minutes", "sample_records", "max_inferred_columns"].includes(key)) {
      endpoint[key] = Number(value || 0);
    } else {
      endpoint[key] = value;
    }

    if (key === "endpoint_name") this.renderEndpoints();
    this.markDirty();
  }

  updateInferredField(event) {
    const endpointIndex = Number(event.target.dataset.inferredEndpoint);
    const fieldIndex = Number(event.target.dataset.inferredIndex);
    const key = event.target.dataset.inferredKey;
    const endpoint = this.state.endpoint_configs[endpointIndex];
    const field = endpoint?.inferred_fields?.[fieldIndex];
    if (!field) return;
    field[key] = event.target.type === "checkbox" ? event.target.checked : event.target.value;

    // Sync watermark_column from checked WM fields
    if (key === "is_watermark" && endpoint) {
      const wmFields = (endpoint.inferred_fields || [])
        .filter(f => f.is_watermark && f.enabled !== false)
        .map(f => f.column_name);
      endpoint.watermark_column = wmFields.join(",") || "";
    }
    this.markDirty();
  }

  async discoverEndpoints() {
    const spec = (this.discoverSpecUrl.value || this.state.specification_url || "").trim();
    if (!spec) {
      this.statusText.textContent = "Enter a specification URL first.";
      return;
    }
    try {
      this.statusText.textContent = "Loading endpoints from spec...";
      const endpoints = await request(`/getEndpoints?url=${encodeURIComponent(spec)}`);
      const discovered = Array.isArray(endpoints) ? endpoints : [];
      if (isLocalMockBaseUrl(this.state.base_url)) {
        // Start with spec endpoints that match mock, then add any mock-only endpoints
        const specMatched = discovered.filter((ep) => isLocalMockEndpoint(ep?.endpoint));
        const specPaths = new Set(specMatched.map((ep) => normalizeEndpointPath(ep?.endpoint)));
        const mockOnly = LOCAL_MOCK_ENDPOINTS
          .filter((p) => !specPaths.has(p))
          .map((p) => ({ endpoint: p.replace(/^\//, ""), method: "GET" }));
        this.discovered = [...specMatched, ...mockOnly];
        this.renderEndpointTable();
        this.statusText.textContent = `Loaded ${this.discovered.length} mock server endpoints (${discovered.length} in spec).`;
        return;
      }
      this.discovered = discovered;
      this.renderEndpointTable();
      this.statusText.textContent = `Loaded ${this.discovered.length} endpoints from spec.`;
    } catch (error) {
      this.statusText.textContent = error.message || "Failed to load spec.";
    }
  }

  /** Render the discovered endpoints as a selectable table */
  renderEndpointTable() {
    if (!this.discovered.length) {
      this.epTableWrap.style.display = "none";
      return;
    }
    this.epTableWrap.style.display = "";
    this.epTableBody.innerHTML = "";

    // Build set of already-selected endpoint URLs (from endpoint_configs + left-tree setup)
    const selectedUrls = new Set();
    for (const cfg of this.state.endpoint_configs) {
      if (cfg.endpoint_url) selectedUrls.add(cfg.endpoint_url);
    }
    // Also include endpoints pre-selected during left-tree API connection setup
    for (const ep of this.connSelectedEndpoints) {
      const url = typeof ep === "string" ? ep : (ep.endpoint_url || ep.endpoint || "");
      if (url) selectedUrls.add(url);
    }

    for (const ep of this.discovered) {
      if (!ep || !ep.endpoint) continue;
      const method = (ep.method || "GET").toUpperCase();
      const isChecked = selectedUrls.has(ep.endpoint);
      const tr = document.createElement("tr");
      tr.className = `ep-row${isChecked ? " checked" : ""}`;
      tr.dataset.endpoint = ep.endpoint;
      tr.dataset.method = method;
      tr.innerHTML = `
        <td style="text-align:center;"><input type="checkbox" class="ep-check" ${isChecked ? "checked" : ""} /></td>
        <td class="mono">${escapeHtml(ep.endpoint)}</td>
        <td><span class="method-badge ${method.toLowerCase()}">${method}</span></td>
      `;
      // Row click toggles checkbox
      tr.addEventListener("click", (e) => {
        if (e.target.tagName === "INPUT") return; // checkbox handles itself
        const cb = tr.querySelector(".ep-check");
        cb.checked = !cb.checked;
        cb.dispatchEvent(new Event("change", { bubbles: true }));
      });
      // Checkbox change handler
      const cb = tr.querySelector(".ep-check");
      cb.addEventListener("change", () => {
        tr.classList.toggle("checked", cb.checked);
        this.handleEndpointToggle(ep.endpoint, method, cb.checked);
        this.updateSelectionCount();
        this.renderEndpoints();
        this.markDirty();
      });
      this.epTableBody.appendChild(tr);

      // If pre-selected from left-tree but not yet in endpoint_configs, add it now
      if (isChecked && !this.state.endpoint_configs.some(c => c.endpoint_url === ep.endpoint)) {
        this.state.endpoint_configs.push({
          ...DEFAULT_ENDPOINT(),
          endpoint_name: ep.endpoint,
          endpoint_url: ep.endpoint,
          http_method: method,
        });
      }
    }

    // Remove the empty default endpoint if we have real ones
    if (this.state.endpoint_configs.length > 1) {
      this.state.endpoint_configs = this.state.endpoint_configs.filter(c => c.endpoint_url || c.endpoint_name);
      if (!this.state.endpoint_configs.length) this.state.endpoint_configs.push(DEFAULT_ENDPOINT());
    }

    this.updateSelectionCount();
    this.renderEndpoints();
  }

  /** Add or remove an endpoint from endpoint_configs when toggled in the table */
  handleEndpointToggle(endpointUrl, method, checked) {
    if (checked) {
      // Add if not already present
      if (!this.state.endpoint_configs.some(c => c.endpoint_url === endpointUrl)) {
        const cfg = {
          ...DEFAULT_ENDPOINT(),
          endpoint_name: endpointUrl,
          endpoint_url: endpointUrl,
          http_method: method,
        };
        // Samsara-style APIs wrap records in {"data": [...]} — set default explode path
        if (isLocalMockBaseUrl(this.state.base_url)) {
          cfg.json_explode_rules = [{ path: "data" }];
        }
        this.state.endpoint_configs.push(cfg);
      }
    } else {
      // Remove
      this.state.endpoint_configs = this.state.endpoint_configs.filter(c => c.endpoint_url !== endpointUrl);
      if (!this.state.endpoint_configs.length) this.state.endpoint_configs.push(DEFAULT_ENDPOINT());
    }
  }

  /** Filter the endpoint table by search text and method */
  filterEndpointTable() {
    const term = (this.epSearch.value || "").toLowerCase();
    const methodFilter = (this.epMethodFilter.value || "").toUpperCase();
    this.epTableBody.querySelectorAll("tr.ep-row").forEach(tr => {
      const ep = (tr.dataset.endpoint || "").toLowerCase();
      const method = (tr.dataset.method || "").toUpperCase();
      const matchText = !term || ep.includes(term);
      const matchMethod = !methodFilter || method === methodFilter;
      tr.style.display = (matchText && matchMethod) ? "" : "none";
    });
  }

  /** Update the "N of M selected" counter */
  updateSelectionCount() {
    const total = this.epTableBody.querySelectorAll("tr.ep-row").length;
    const checked = this.epTableBody.querySelectorAll("tr.ep-row .ep-check:checked").length;
    this.epSelectionCount.textContent = `${checked} of ${total} endpoints selected`;
  }

  /** Discover endpoints AND infer auth/pagination from the raw OpenAPI spec */
  async discoverAndInfer() {
    const specUrl = (this.discoverSpecUrl.value || this.state.specification_url || "").trim();
    if (!specUrl) return;

    // First do normal endpoint discovery via backend
    await this.discoverEndpoints();

    // Then fetch the raw spec to infer auth and pagination
    try {
      this.statusText.textContent += " Inferring auth & pagination...";
      const res = await fetch(specUrl);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const spec = await res.json();
      const hints = inferFromSpec(spec);

      // Apply base URL if not set
      if (hints.baseUrl && !this.state.base_url) {
        this.state.base_url = hints.baseUrl;
        this.shadowRoot.querySelector("#baseUrl").value = hints.baseUrl;
      }

      // Apply auth if not already set
      if (hints.auth && !this.state.auth_ref.type) {
        this.state.auth_ref.type = hints.auth.type;
        if (hints.auth.location) this.state.auth_ref.location = hints.auth.location;
        if (hints.auth.param_name) this.state.auth_ref.param_name = hints.auth.param_name;
        if (hints.auth.header_name) this.state.auth_ref.header_name = hints.auth.header_name;
        this.shadowRoot.querySelector("#authType").value = this.state.auth_ref.type;
        this.shadowRoot.querySelector("#authLocation").value = this.state.auth_ref.location || "header";
        this.shadowRoot.querySelector("#authParamName").value = this.state.auth_ref.param_name || "";
        this.shadowRoot.querySelector("#authHeaderName").value = this.state.auth_ref.header_name || "";
      }

      // Apply pagination hints to selected endpoints with no strategy set
      if (hints.pagination) {
        for (const ep of this.state.endpoint_configs) {
          if (ep.pagination_strategy === "none") {
            ep.pagination_strategy = hints.pagination.strategy;
            if (hints.pagination.cursor_param) ep.cursor_param = hints.pagination.cursor_param;
            if (hints.pagination.token_param) ep.token_param = hints.pagination.token_param;
          }
        }
        this.renderEndpoints();
      }

      const parts = [];
      if (hints.auth) parts.push(`auth: ${hints.auth.type}`);
      if (hints.pagination) parts.push(`pagination: ${hints.pagination.strategy}`);
      if (hints.baseUrl) parts.push(`base: ${hints.baseUrl}`);
      this.statusText.textContent = `Inferred: ${parts.join(", ") || "no hints found"}.`;
      this.markDirty();
    } catch (e) {
      console.warn("Could not fetch raw spec for inference:", e);
      this.statusText.textContent += " (Could not infer auth/pagination from spec)";
    }
  }

  async previewSchema(index) {
    const endpoint = this.state.endpoint_configs[index];
    if (!endpoint) return;
    this.setEndpointTab(index, "schema");
    this.renderEndpoints();
    // Try to derive base_url from spec URL if not explicitly set
    const baseUrl = this.state.base_url || (() => {
      try {
        const u = new URL(this.state.specification_url || "");
        return `${u.protocol}//${u.host}`;
      } catch { return ""; }
    })();
    if (!baseUrl || !endpoint.endpoint_url) {
      this.statusText.textContent = "Base URL and endpoint URL required for preview.";
      return;
    }
    if (!this.state.base_url) this.state.base_url = baseUrl;
    const usingLocalMock = isLocalMockBaseUrl(this.state.base_url);
    if (usingLocalMock && !isLocalMockEndpoint(endpoint.endpoint_url)) {
      endpoint.schema_preview_status = localMockPreviewMessage(endpoint.endpoint_url);
      this.statusText.textContent = endpoint.schema_preview_status;
      this.renderEndpoints();
      return;
    }
    endpoint.schema_preview_status = "Sampling...";
    this.renderEndpoints();
    try {
      const result = await request("/previewApiSchemaInference", {
        method: "POST",
        body: {
          base_url: this.state.base_url,
          auth_ref: {
            type: this.state.auth_ref.type,
            secret_ref: this.state.auth_ref.secret_ref,
            ...(this.state.auth_ref.type === "bearer" ? { token: this.state.auth_ref.token } : {}),
            ...(this.state.auth_ref.type === "api-key" ? {
              key: this.state.auth_ref.token,
              location: this.state.auth_ref.location,
              param_name: this.state.auth_ref.param_name,
              header_name: this.state.auth_ref.header_name,
            } : {}),
          },
          endpoint_config: serializeEndpointConfig(endpoint),
        },
      });
      endpoint.inferred_fields = Array.isArray(result.inferred_fields) ? result.inferred_fields : [];
      endpoint.schema_recommendations = result.recommendations || null;
      // Switch to infer mode since we now have inferred fields
      if (endpoint.inferred_fields.length && endpoint.schema_mode === "manual") {
        endpoint.schema_mode = "infer";
      }
      if (!applyServerRecommendations(endpoint) && autoMarkWatermarkFields(endpoint.inferred_fields)) {
        const wmFields = endpoint.inferred_fields.filter(f => f.is_watermark && f.enabled !== false).map(f => f.column_name);
        if (wmFields.length && !endpoint.watermark_column) {
          endpoint.watermark_column = wmFields.join(",");
        }
      }
      if (!endpoint.primary_key_fields?.length) {
        autoApplyPrimaryKeyFields(endpoint);
      }
      if (Array.isArray(result.applied_json_explode_rules) && result.applied_json_explode_rules.length) {
        endpoint.json_explode_rules = result.applied_json_explode_rules;
      }
      const detectedPath = String(result.detected_records_path || "").trim();
      endpoint.schema_preview_status = detectedPath
        ? `Auto-detected record array ${detectedPath}. Previewed ${result.sampled_records || 0} records. ${inferredColumnsLabel(endpoint)} for one Bronze table.`
        : `Previewed ${result.sampled_records || 0} records. ${inferredColumnsLabel(endpoint)} for one Bronze table.`;
      this.statusText.textContent = detectedPath
        ? `Schema preview updated for ${endpoint.endpoint_name || endpoint.endpoint_url}: auto-detected ${detectedPath}, ${inferredColumnsLabel(endpoint)} for one Bronze table (${bronzeTableLabel(endpoint)}).`
        : `Schema preview updated for ${endpoint.endpoint_name || endpoint.endpoint_url}: ${inferredColumnsLabel(endpoint)} for one Bronze table (${bronzeTableLabel(endpoint)}).`;
      this.renderEndpoints();
      this.markDirty();
    } catch (error) {
      endpoint.schema_preview_status = usingLocalMock && String(error.message || "").includes("HTTP 404")
        ? localMockPreviewMessage(endpoint.endpoint_url)
        : (error.message || "Preview failed.");
      this.statusText.textContent = endpoint.schema_preview_status;
      this.renderEndpoints();
    }
  }

  payload() {
    return {
      id: this.state.id,
      api_name: this.state.api_name,
      source_system: this.state.source_system,
      base_url: this.state.base_url,
      specification_url: this.state.specification_url,
      auth_ref: {
        type: this.state.auth_ref.type,
        secret_ref: this.state.auth_ref.secret_ref,
        ...(this.state.auth_ref.type === "bearer" ? { token: this.state.auth_ref.token } : {}),
        ...(this.state.auth_ref.type === "api-key" ? {
          key: this.state.auth_ref.token,
          location: this.state.auth_ref.location,
          param_name: this.state.auth_ref.param_name,
          header_name: this.state.auth_ref.header_name,
        } : {}),
      },
      endpoint_configs: this.state.endpoint_configs.map(cfg => serializeEndpointConfig(cfg)),
    };
  }

  async save() {
    try {
      const data = await request("/saveApi", { method: "POST", body: this.payload() });
      setPanelItems(data);
      this.savedState = stableStateSnapshot(this.state);
      this.statusText.textContent = "Saved.";
      this.markDirty();
    } catch (error) {
      this.statusText.textContent = error.message || "Save failed.";
    }
  }

  async pollExecutionRun(runId, maxAttempts = 60, delayMs = 1000) {
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const run = await request(`/executionRuns/${encodeURIComponent(runId)}`);
      if (!["queued", "leased", "running", "recovering_orphan"].includes(run.status)) {
        return run;
      }
      await sleep(delayMs);
    }
    throw new Error(`Run ${runId} did not complete within ${Math.round((maxAttempts * delayMs) / 1000)} seconds.`);
  }

  applyExecutionRunResults(run) {
    const results = Array.isArray(run?.result_json?.results) ? run.result_json.results : [];
    const endpointMap = new Map(results.map((result) => [result.endpoint_name, result]));
    let watermarkProposed = false;
    this.state.endpoint_configs.forEach((cfg) => {
      cfg.latest_run_result = endpointMap.get(cfg.endpoint_name) || null;
      // Auto-mark watermark checkboxes and derive watermark_column after first run
      if (Array.isArray(cfg.inferred_fields) && cfg.inferred_fields.length) {
        if (autoMarkWatermarkFields(cfg.inferred_fields)) {
          const wmFields = cfg.inferred_fields.filter(f => f.is_watermark && f.enabled !== false).map(f => f.column_name);
          if (wmFields.length && !cfg.watermark_column) {
            cfg.watermark_column = wmFields.join(",");
            watermarkProposed = true;
          }
        }
        if (autoApplyPrimaryKeyFields(cfg)) {
          watermarkProposed = true;
        }
      }
    });
    if (watermarkProposed) this.markDirty();
    this.renderEndpoints();
    return results;
  }

  async run() {
    const rect = selectedRectangle();
    if (!rect?.id) {
      this.statusText.textContent = "Save first.";
      return;
    }
    try {
      this.runButton.disabled = true;

      // Pre-run warnings based on endpoint load_type / watermark_column
      const cfgs = this.state.endpoint_configs || [];
      const incrementalNoWatermark = cfgs.filter(
        (c) => (c.load_type || "full") !== "full" && !c.watermark_column
      );
      const allFull = cfgs.length > 0 && cfgs.every((c) => !c.load_type || c.load_type === "full");

      if (incrementalNoWatermark.length > 0) {
        const names = incrementalNoWatermark.map((c) => c.endpoint_name || "unnamed").join(", ");
        this.statusText.style.color = "#b8860b";
        this.statusText.textContent = `Warning: endpoint ${names} has ${incrementalNoWatermark.length > 1 ? "incremental loads" : "incremental load"} but no watermark column. Data may be duplicated.`;
        // Pause briefly so the user can see the warning before it gets overwritten
        await new Promise((r) => setTimeout(r, 1500));
      } else if (allFull) {
        this.statusText.style.color = "#6b7280";
        this.statusText.textContent = "Note: full load \u2014 all records will be re-fetched.";
        await new Promise((r) => setTimeout(r, 800));
      }

      const startTime = Date.now();
      const endpointName = this.shadowRoot.querySelector("#runEndpointName")?.value || undefined;
      const queued = await request("/runApiIngestion", {
        method: "POST",
        body: { id: rect.id, endpoint_name: endpointName },
      });
      this.statusText.textContent = `Run queued for ${endpointName || "all endpoints"}. Waiting for completion...`;
      const run = queued.run_id ? await this.pollExecutionRun(queued.run_id) : null;
      const results = run ? this.applyExecutionRunResults(run) : [];
      const loaded = results
        .filter((result) => result.status === "succeeded")
        .map((result) => {
          const bronzeTable = bronzeTableFromRunResult(result);
          const rowsWritten = Number(result.rows_written || 0);
          return bronzeTable
            ? `${result.endpoint_name}: ${rowsWritten} row${rowsWritten === 1 ? "" : "s"} into ${bronzeTable}`
            : `${result.endpoint_name}: ${rowsWritten} row${rowsWritten === 1 ? "" : "s"}`;
        });
      const durationSec = Math.round((Date.now() - startTime) / 1000);
      const durationTag = ` in ${durationSec}s`;
      this.statusText.style.color = "";
      this.statusText.textContent = loaded.length
        ? `Run completed${durationTag}. ${loaded.join("; ")}.`
        : `Run ${run?.status || "completed"}${durationTag}.`;
    } catch (error) {
      this.statusText.textContent = error.message || "Run failed.";
      this.statusText.style.color = "#d32f2f";
    } finally {
      this.runButton.disabled = false;
    }
  }
}

customElements.define("api-component", ApiComponent);
