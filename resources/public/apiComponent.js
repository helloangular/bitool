import EventHandler from "./library/eventHandler.js";
import { customConfirm, getPanelItems, request, setPanelItems } from "./library/utils.js";

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
  .title h2 {
    margin: 0;
    font-size: 28px;
    font-weight: 600;
  }
  .title p {
    margin: 4px 0 0 0;
    color: #5d665f;
    font-size: 14px;
  }
  .actions {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
  }
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
  .endpoint-list {
    display: grid;
    gap: 14px;
  }
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
  .endpoint-head strong {
    font-size: 16px;
  }
  .hint {
    color: #66736c;
    font-size: 12px;
  }
  .row {
    display: flex;
    gap: 10px;
    align-items: center;
    flex-wrap: wrap;
  }
  .row > * {
    flex: 1;
  }
  .inline {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .inline input[type="checkbox"] {
    width: auto;
  }
  .spec-tools {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    align-items: end;
  }
  .spec-tools .grow {
    flex: 1;
    min-width: 220px;
  }
  .status {
    min-height: 20px;
    color: #49564d;
    font-size: 13px;
    margin-top: 10px;
  }
  .mono {
    font-family: "SFMono-Regular", Consolas, monospace;
    font-size: 12px;
  }
</style>
<div class="shell">
  <div class="header">
    <div class="title">
      <h2>API Ingestion</h2>
      <p>Configure one API node as an endpoint bundle for Bronze ingestion.</p>
    </div>
    <div class="actions">
      <button id="runButton" class="secondary" type="button">Run</button>
      <button id="saveButton" type="button">Save</button>
      <button id="closeButton" class="secondary" type="button">Close</button>
    </div>
  </div>

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
        <input id="baseUrl" name="base_url" data-field="base_url" type="text" placeholder="https://api.samsara.com" />
      </div>
      <div>
        <label for="specificationUrl">Specification URL</label>
        <input id="specificationUrl" name="specification_url" data-field="specification_url" type="text" />
      </div>
    </div>
  </div>

  <div class="card">
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

  <div class="card">
    <div class="spec-tools">
      <div class="grow">
        <label for="discoverSpecUrl">Discover Endpoints From Spec</label>
        <input id="discoverSpecUrl" type="text" placeholder="Use specification URL above or paste another spec URL" />
      </div>
      <button id="discoverButton" class="secondary" type="button">Load Spec Endpoints</button>
      <select id="discoveredEndpoints">
        <option value="">Select discovered endpoint</option>
      </select>
      <button id="addDiscoveredButton" class="secondary" type="button">Add To Config</button>
      <button id="addEndpointButton" type="button">Add Blank Endpoint</button>
    </div>
    <div class="status" id="statusText"></div>
  </div>

  <div class="endpoint-list" id="endpointList"></div>
</div>
`;

const DEFAULT_ENDPOINT = () => ({
  endpoint_name: "",
  endpoint_url: "",
  http_method: "GET",
  load_type: "full",
  pagination_strategy: "none",
  pagination_location: "query",
  schema_mode: "manual",
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
  page_size: 100,
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
    ...cfg,
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

class ApiComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.savedState = null;
    this.discovered = [];
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this.bindElements();
    this.closeImmediately();
  }

  disconnectedCallback() {
    EventHandler.removeGroup("API");
  }

  attributeChangedCallback(name, _oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") {
      this.open();
    } else {
      this.close();
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.saveButton = sr.querySelector("#saveButton");
    this.runButton = sr.querySelector("#runButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.endpointList = sr.querySelector("#endpointList");
    this.statusText = sr.querySelector("#statusText");
    this.discoveredEndpoints = sr.querySelector("#discoveredEndpoints");
    this.discoverSpecUrl = sr.querySelector("#discoverSpecUrl");
  }

  initialState() {
    const rect = selectedRectangle() || {};
    return {
      id: rect.id || null,
      api_name: rect.api_name || "",
      source_system: rect.source_system || "",
      base_url: rect.base_url || "",
      specification_url: rect.specification_url || "",
      auth_ref: {
        type: rect.auth_ref?.type || "",
        secret_ref: rect.auth_ref?.secret_ref || "",
        token: rect.auth_ref?.token || rect.auth_ref?.key || "",
        location: rect.auth_ref?.location || "header",
        param_name: rect.auth_ref?.param_name || "api_key",
        header_name: rect.auth_ref?.header_name || "X-API-Key",
      },
      endpoint_configs: (rect.endpoint_configs || []).map((cfg) => ({
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
      })),
    };
  }

  open() {
    this.style.display = "block";
    this.state = this.initialState();
    this.savedState = JSON.stringify(this.state);
    if (!this.state.endpoint_configs.length) {
      this.state.endpoint_configs.push(DEFAULT_ENDPOINT());
    }
    this.render();
    this.setEvents();
  }

  closeImmediately() {
    this.style.display = "none";
  }

  async close() {
    if (this.style.display === "none") return;
    if (JSON.stringify(this.state) !== this.savedState) {
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
    EventHandler.on(this.shadowRoot.querySelector("#discoverButton"), "click", () => this.discoverEndpoints(), false, "API");
    EventHandler.on(this.shadowRoot.querySelector("#addDiscoveredButton"), "click", () => this.addDiscoveredEndpoint(), false, "API");
    this.shadowRoot.querySelectorAll("[data-field]").forEach((el) => {
      EventHandler.on(el, el.tagName === "TEXTAREA" || el.tagName === "INPUT" ? "input" : "change", (event) => {
        this.updateRootField(event.target.dataset.field, event.target.type === "checkbox" ? event.target.checked : event.target.value);
      }, false, "API");
    });
  }

  markDirty() {
    this.saveButton.disabled = JSON.stringify(this.state) === this.savedState;
  }

  updateRootField(field, value) {
    if (field.startsWith("auth_ref.")) {
      const subfield = field.replace("auth_ref.", "");
      this.state.auth_ref[subfield] = value;
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
    this.markDirty();
  }

  endpointHtml(cfg, index) {
    const name = cfg.endpoint_name || `Endpoint ${index + 1}`;
    const inferredRows = (cfg.inferred_fields || []).map((field, fieldIndex) => `
      <tr>
        <td><input data-inferred-endpoint="${index}" data-inferred-index="${fieldIndex}" data-inferred-key="enabled" type="checkbox" ${field.enabled !== false ? "checked" : ""} /></td>
        <td class="mono">${escapeHtml(field.path || "")}</td>
        <td><input data-inferred-endpoint="${index}" data-inferred-index="${fieldIndex}" data-inferred-key="column_name" type="text" value="${escapeHtml(field.column_name || "")}" /></td>
        <td class="mono">${escapeHtml(field.type || "STRING")}</td>
        <td><input data-inferred-endpoint="${index}" data-inferred-index="${fieldIndex}" data-inferred-key="override_type" type="text" value="${escapeHtml(field.override_type || "")}" placeholder="optional" /></td>
        <td>${Number(field.sample_coverage || 0).toFixed(2)}</td>
      </tr>
    `).join("");
    return `
      <div class="endpoint-card" data-index="${index}">
        <div class="endpoint-head">
          <strong>${name}</strong>
          <div class="row">
            <button class="secondary preview-schema" type="button">Preview Schema</button>
            <button class="secondary remove-endpoint" type="button">Remove</button>
          </div>
        </div>
        <div class="grid">
          <div>
            <label>Endpoint Name</label>
            <input data-endpoint="${index}" data-key="endpoint_name" type="text" value="${cfg.endpoint_name || ""}" />
          </div>
          <div>
            <label>Endpoint URL</label>
            <input data-endpoint="${index}" data-key="endpoint_url" type="text" value="${cfg.endpoint_url || ""}" />
          </div>
          <div>
            <label>HTTP Method</label>
            <select data-endpoint="${index}" data-key="http_method">
              ${["GET", "POST", "PUT", "PATCH", "DELETE"].map((method) => `<option value="${method}" ${method === (cfg.http_method || "GET") ? "selected" : ""}>${method}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Load Type</label>
            <select data-endpoint="${index}" data-key="load_type">
              ${["full", "incremental", "snapshot"].map((value) => `<option value="${value}" ${value === (cfg.load_type || "full") ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Pagination Strategy</label>
            <select data-endpoint="${index}" data-key="pagination_strategy">
              ${["none", "offset", "page", "cursor", "token", "time", "link-header"].map((value) => `<option value="${value}" ${value === (cfg.pagination_strategy || "none") ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Pagination Location</label>
            <select data-endpoint="${index}" data-key="pagination_location">
              ${["query", "body"].map((value) => `<option value="${value}" ${value === (cfg.pagination_location || "query") ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Primary Keys (CSV)</label>
            <input data-endpoint="${index}" data-key="primary_key_fields" type="text" value="${(cfg.primary_key_fields || []).join(", ")}" />
          </div>
          <div>
            <label>Selected Nodes (CSV)</label>
            <input data-endpoint="${index}" data-key="selected_nodes" type="text" value="${(cfg.selected_nodes || []).join(", ")}" />
          </div>
          <div>
            <label>Schema Mode</label>
            <select data-endpoint="${index}" data-key="schema_mode">
              ${["manual", "infer", "hybrid"].map((value) => `<option value="${value}" ${value === (cfg.schema_mode || "manual") ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Schema Evolution</label>
            <select data-endpoint="${index}" data-key="schema_evolution_mode">
              ${["none", "advisory", "additive"].map((value) => `<option value="${value}" ${value === (cfg.schema_evolution_mode || "advisory") ? "selected" : ""}>${value}</option>`).join("")}
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
            <input data-endpoint="${index}" data-key="bronze_table_name" type="text" value="${cfg.bronze_table_name || ""}" />
          </div>
          <div>
            <label>Silver Table</label>
            <input data-endpoint="${index}" data-key="silver_table_name" type="text" value="${cfg.silver_table_name || ""}" />
          </div>
          <div>
            <label>Watermark Column</label>
            <input data-endpoint="${index}" data-key="watermark_column" type="text" value="${cfg.watermark_column || ""}" />
          </div>
          <div>
            <label>Overlap Minutes</label>
            <input data-endpoint="${index}" data-key="watermark_overlap_minutes" type="number" value="${cfg.watermark_overlap_minutes ?? 0}" />
          </div>
          <div>
            <label>Page Size</label>
            <input data-endpoint="${index}" data-key="page_size" type="number" value="${cfg.page_size ?? 100}" />
          </div>
          <div>
            <label>Time Window Minutes</label>
            <input data-endpoint="${index}" data-key="time_window_minutes" type="number" value="${cfg.time_window_minutes ?? 60}" />
          </div>
          <div>
            <label>Cursor Field</label>
            <input data-endpoint="${index}" data-key="cursor_field" type="text" value="${cfg.cursor_field || ""}" />
          </div>
          <div>
            <label>Cursor Param</label>
            <input data-endpoint="${index}" data-key="cursor_param" type="text" value="${cfg.cursor_param || "cursor"}" />
          </div>
          <div>
            <label>Token Field</label>
            <input data-endpoint="${index}" data-key="token_field" type="text" value="${cfg.token_field || ""}" />
          </div>
          <div>
            <label>Token Param</label>
            <input data-endpoint="${index}" data-key="token_param" type="text" value="${cfg.token_param || "page_token"}" />
          </div>
          <div>
            <label>Time Field</label>
            <input data-endpoint="${index}" data-key="time_field" type="text" value="${cfg.time_field || ""}" />
          </div>
          <div>
            <label>Time Param</label>
            <input data-endpoint="${index}" data-key="time_param" type="text" value="${cfg.time_param || "updated_since"}" />
          </div>
          <div class="inline">
            <label>Enabled</label>
            <input data-endpoint="${index}" data-key="enabled" type="checkbox" ${cfg.enabled !== false ? "checked" : ""} />
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
        <div class="grid" style="margin-top:10px;">
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
            <label>Explode Rules JSON</label>
            <textarea data-endpoint="${index}" data-key="json_explode_rules">${PRETTY_JSON(cfg.json_explode_rules, [])}</textarea>
          </div>
        </div>
        <div style="margin-top:10px;">
          <div class="row" style="justify-content:space-between;">
            <div class="hint">Pagination fields are generic on purpose. Use JSON blocks for endpoint-specific query, header, or body parameters.</div>
            <div class="hint">${cfg.schema_preview_status ? escapeHtml(cfg.schema_preview_status) : ""}</div>
          </div>
          <div style="margin-top:10px; overflow:auto;">
            <table style="width:100%; border-collapse:collapse; font-size:12px;">
              <thead>
                <tr>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">On</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Path</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Column</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Type</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Override</th>
                  <th style="text-align:left; border-bottom:1px solid rgba(35,49,38,0.18); padding:6px;">Coverage</th>
                </tr>
              </thead>
              <tbody>
                ${inferredRows || `<tr><td colspan="6" class="hint" style="padding:8px;">No inferred fields yet. Use Preview Schema to sample the endpoint.</td></tr>`}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    `;
  }

  renderEndpoints() {
    this.endpointList.innerHTML = this.state.endpoint_configs.map((cfg, index) => this.endpointHtml(cfg, index)).join("");
    this.endpointList.querySelectorAll("[data-endpoint]").forEach((el) => {
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (event) => this.updateEndpointField(event), false, "API");
      if (el.type === "checkbox") {
        EventHandler.on(el, "change", (event) => this.updateEndpointField(event), false, "API");
      }
    });
    this.endpointList.querySelectorAll("[data-inferred-endpoint]").forEach((el) => {
      const eventName = el.type === "checkbox" ? "change" : "input";
      EventHandler.on(el, eventName, (event) => this.updateInferredField(event), false, "API");
    });
    this.endpointList.querySelectorAll(".remove-endpoint").forEach((btn) => {
      EventHandler.on(btn, "click", (event) => {
        const idx = Number(event.target.closest(".endpoint-card").dataset.index);
        this.state.endpoint_configs.splice(idx, 1);
        if (!this.state.endpoint_configs.length) this.state.endpoint_configs.push(DEFAULT_ENDPOINT());
        this.renderEndpoints();
        this.markDirty();
      }, false, "API");
    });
    this.endpointList.querySelectorAll(".preview-schema").forEach((btn) => {
      EventHandler.on(btn, "click", (event) => {
        const idx = Number(event.target.closest(".endpoint-card").dataset.index);
        this.previewSchema(idx);
      }, false, "API");
    });
    this.renderRunOptions();
  }

  renderRunOptions() {
    const existing = this.shadowRoot.querySelector("#runEndpointName");
    if (existing) existing.remove();
    const select = document.createElement("select");
    select.id = "runEndpointName";
    select.innerHTML = `<option value="">Run all enabled endpoints</option>${this.state.endpoint_configs.map((cfg) => `<option value="${cfg.endpoint_name || ""}">${cfg.endpoint_name || cfg.endpoint_url || "unnamed"}</option>`).join("")}`;
    this.runButton.before(select);
  }

  updateEndpointField(event) {
    const idx = Number(event.target.dataset.endpoint);
    const key = event.target.dataset.key;
    const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
    const endpoint = this.state.endpoint_configs[idx];
    if (!endpoint) return;

    if (key === "primary_key_fields" || key === "selected_nodes") {
      endpoint[key] = value.split(",").map((item) => item.trim()).filter(Boolean);
    } else if (key.startsWith("retry_policy.")) {
      const subkey = key.replace("retry_policy.", "");
      endpoint.retry_policy = endpoint.retry_policy || {};
      endpoint.retry_policy[subkey] = Number(value || 0);
    } else if (["query_params", "request_headers", "body_params", "json_explode_rules"].includes(key)) {
      endpoint[key] = value;
    } else if (["page_size", "time_window_minutes", "watermark_overlap_minutes", "sample_records", "max_inferred_columns"].includes(key)) {
      endpoint[key] = Number(value || 0);
    } else {
      endpoint[key] = value;
    }

    if (key === "endpoint_name") {
      this.renderEndpoints();
    }
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
    this.markDirty();
  }

  async discoverEndpoints() {
    const spec = (this.discoverSpecUrl.value || this.state.specification_url || "").trim();
    if (!spec) {
      this.statusText.textContent = "Enter a specification URL before discovery.";
      return;
    }
    try {
      this.statusText.textContent = "Loading endpoints from spec...";
      const endpoints = await request(`/getEndpoints?url=${encodeURIComponent(spec)}`);
      this.discovered = Array.isArray(endpoints) ? endpoints : [];
      this.discoveredEndpoints.innerHTML = `<option value="">Select discovered endpoint</option>${this.discovered.map((ep, index) => `<option value="${index}">${ep.method || "GET"} ${ep.endpoint}</option>`).join("")}`;
      this.statusText.textContent = `Loaded ${this.discovered.length} endpoints from spec.`;
    } catch (error) {
      this.statusText.textContent = error.message || "Failed to load endpoints from spec.";
    }
  }

  addDiscoveredEndpoint() {
    const idx = Number(this.discoveredEndpoints.value);
    const chosen = this.discovered[idx];
    if (!chosen) return;
    this.state.endpoint_configs.push({
      ...DEFAULT_ENDPOINT(),
      endpoint_name: chosen.endpoint,
      endpoint_url: chosen.endpoint,
      http_method: String(chosen.method || "GET").toUpperCase(),
    });
    this.renderEndpoints();
    this.markDirty();
  }

  async previewSchema(index) {
    const endpoint = this.state.endpoint_configs[index];
    if (!endpoint) return;
    if (!this.state.base_url || !endpoint.endpoint_url) {
      this.statusText.textContent = "Base URL and endpoint URL are required before schema preview.";
      return;
    }
    endpoint.schema_preview_status = "Sampling endpoint...";
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
      endpoint.schema_preview_status = `Previewed ${result.sampled_records || 0} records from HTTP ${result.http_status || "?"}.`;
      this.statusText.textContent = `Schema preview updated for ${endpoint.endpoint_name || endpoint.endpoint_url}.`;
      this.renderEndpoints();
      this.markDirty();
    } catch (error) {
      endpoint.schema_preview_status = error.message || "Schema preview failed.";
      this.statusText.textContent = endpoint.schema_preview_status;
      this.renderEndpoints();
    }
  }

  payload() {
    const endpoint_configs = this.state.endpoint_configs.map((cfg) => serializeEndpointConfig(cfg));

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
      endpoint_configs,
    };
  }

  async save() {
    try {
      const data = await request("/saveApi", { method: "POST", body: this.payload() });
      setPanelItems(data);
      this.savedState = JSON.stringify(this.state);
      this.statusText.textContent = "API node saved.";
      this.markDirty();
    } catch (error) {
      this.statusText.textContent = error.message || "Save failed.";
    }
  }

  async run() {
    const rect = selectedRectangle();
    if (!rect?.id) {
      this.statusText.textContent = "Save the API node before running ingestion.";
      return;
    }
    try {
      const endpointName = this.shadowRoot.querySelector("#runEndpointName")?.value || undefined;
      const result = await request("/runApiIngestion", {
        method: "POST",
        body: {
          id: rect.id,
          endpoint_name: endpointName,
        },
      });
      this.statusText.textContent = `Run finished: ${result.results?.map((item) => `${item.endpoint_name}:${item.status}`).join(", ") || "no results"}`;
    } catch (error) {
      this.statusText.textContent = error.message || "Run failed.";
    }
  }
}

customElements.define("api-component", ApiComponent);
