import EventHandler from "./library/eventHandler.js";
import { customConfirm, request } from "./library/utils.js";
import { aiAssistCSS, renderAiLoading, renderAiCard, callAiEndpoint } from "./aiAssistCard.js";

const template = document.createElement("template");
template.innerHTML = `
<style>
  :host {
    visibility: hidden;
    background: #eff4f0;
    color: #17251c;
    overflow: auto;
    font-family: Georgia, "Times New Roman", serif;
  }
  .shell {
    min-height: 100vh;
    padding: 24px;
    background:
      linear-gradient(135deg, rgba(53, 94, 59, 0.14), transparent 36%),
      linear-gradient(180deg, #f7fbf8 0%, #ebf3ee 100%);
  }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
  }
  .title h2 {
    margin: 0;
    font-size: 28px;
    font-weight: 600;
  }
  .title p {
    margin: 4px 0 0 0;
    color: #5c6c60;
    font-size: 14px;
  }
  .actions {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
  }
  button {
    border: 1px solid #203326;
    background: #203326;
    color: #fffdf8;
    padding: 10px 14px;
    cursor: pointer;
  }
  button.secondary {
    background: transparent;
    color: #203326;
  }
  .card {
    background: rgba(255, 255, 255, 0.88);
    border: 1px solid rgba(32, 51, 38, 0.15);
    box-shadow: 0 18px 42px rgba(32, 51, 38, 0.08);
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
    color: #526055;
    margin-bottom: 4px;
  }
  input, select, textarea {
    width: 100%;
    box-sizing: border-box;
    border: 1px solid rgba(32, 51, 38, 0.18);
    background: #fffefb;
    padding: 8px 10px;
    color: #17251c;
    font-size: 14px;
  }
  textarea {
    min-height: 92px;
    resize: vertical;
    font-family: "SFMono-Regular", Consolas, monospace;
  }
  .inline {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .inline input[type="checkbox"] {
    width: auto;
  }
  .status {
    min-height: 20px;
    color: #526055;
    font-size: 13px;
    margin-top: 10px;
  }
  .close-x {
    background: none;
    border: none;
    font-size: 22px;
    cursor: pointer;
    color: #5c6c60;
    padding: 0 4px;
    line-height: 1;
    margin-left: 8px;
  }
  .close-x:hover {
    color: #17251c;
  }
  ${aiAssistCSS}
</style>
<div class="shell">
  <div class="header">
      <div class="title">
        <h2>Target</h2>
      <p>Configure Bronze destination and optional warehouse-native Silver/Gold execution settings.</p>
      </div>
    <div class="actions">
      <button id="saveButton" type="button">Save</button>
      <button id="closeButton" class="secondary" type="button">Close</button>
      <button id="closeX" class="close-x" type="button">&times;</button>
    </div>
  </div>

  <div class="card">
    <div class="grid">
      <div id="targetKindRow">
        <label for="targetKind">Target Kind</label>
        <select id="targetKind" data-field="target_kind">
          <option value="databricks">databricks</option>
          <option value="bigquery">bigquery</option>
          <option value="postgresql">postgresql</option>
          <option value="mysql">mysql</option>
          <option value="snowflake">snowflake</option>
        </select>
      </div>
      <div id="connectionIdRow">
        <label for="connectionId">Connection ID</label>
        <select id="connectionId" data-field="connection_id"></select>
      </div>
      <div id="catalogRow">
        <label for="catalog">Catalog</label>
        <input id="catalog" data-field="catalog" type="text" />
      </div>
      <div id="schemaRow">
        <label for="schema">Schema</label>
        <input id="schema" data-field="schema" type="text" />
      </div>
      <div id="tableNameRow">
        <label for="tableName">Table Name</label>
        <input id="tableName" data-field="table_name" type="text" />
      </div>
      <div id="writeModeRow">
        <label for="writeMode">Write Mode</label>
        <select id="writeMode" data-field="write_mode">
          <option value="append">append</option>
          <option value="merge">merge</option>
          <option value="replace">replace</option>
          <option value="update">update</option>
          <option value="delete">delete</option>
          <option value="copy_into">copy_into (files)</option>
        </select>
        <button id="aiExplainWriteMode" type="button"
          style="margin-top:6px;padding:4px 12px;border:1px solid #7c5cfc;border-radius:6px;background:#f8f6ff;color:#7c5cfc;font-size:11px;font-weight:600;cursor:pointer;font-family:'DM Sans',sans-serif;">
          Explain write mode
        </button>
      </div>
      <div id="tableFormatRow">
        <label for="tableFormat">Table Format</label>
        <select id="tableFormat" data-field="table_format">
          <option value="delta">delta</option>
          <option value="parquet">parquet</option>
          <option value="table">table</option>
        </select>
      </div>
      <div id="partitionColumnsRow">
        <label for="partitionColumns">Partition Columns (CSV)</label>
        <input id="partitionColumns" data-field="partition_columns" type="text" />
      </div>
      <div id="mergeKeysRow">
        <label for="mergeKeys">Merge Keys (CSV)</label>
        <input id="mergeKeys" data-field="merge_keys" type="text" />
      </div>
      <div id="clusterByRow">
        <label for="clusterBy">Cluster By (CSV)</label>
        <input id="clusterBy" data-field="cluster_by" type="text" />
      </div>
      <div id="createTableRow" class="inline">
        <label>Create Table</label>
        <input id="createTable" data-field="create_table" type="checkbox" />
      </div>
      <div id="truncateRow" class="inline">
        <label>Truncate On Run</label>
        <input id="truncate" data-field="truncate" type="checkbox" />
      </div>
      <div id="triggerGoldOnSuccessRow" class="inline">
        <label>Trigger Gold On Success</label>
        <input id="triggerGoldOnSuccess" data-field="trigger_gold_on_success" type="checkbox" />
      </div>
    </div>
    <div id="aiWriteModeResult"></div>
  </div>

  <div class="card" id="snowflakeCard" style="display:none;">
    <h3 style="margin:0 0 10px 0;font-size:16px;font-weight:600;color:#203326;">Snowflake Options</h3>
    <div class="grid">
      <div>
        <label for="sfLoadMethod">Load Method</label>
        <select id="sfLoadMethod" data-field="sf_load_method">
          <option value="jdbc">JDBC Insert</option>
          <option value="put_copy">PUT + COPY INTO</option>
          <option value="merge">MERGE</option>
        </select>
      </div>
      <div>
        <label for="sfStageName">Stage Name</label>
        <input id="sfStageName" data-field="sf_stage_name" type="text" placeholder="@my_stage" />
      </div>
      <div>
        <label for="sfWarehouse">Warehouse</label>
        <input id="sfWarehouse" data-field="sf_warehouse" type="text" />
      </div>
      <div>
        <label for="sfFileFormat">File Format</label>
        <input id="sfFileFormat" data-field="sf_file_format" type="text" placeholder="CSV, JSON, PARQUET" />
      </div>
      <div>
        <label for="sfOnError">On Error</label>
        <select id="sfOnError" data-field="sf_on_error">
          <option value="ABORT_STATEMENT">ABORT_STATEMENT</option>
          <option value="CONTINUE">CONTINUE</option>
          <option value="SKIP_FILE">SKIP_FILE</option>
        </select>
      </div>
      <div class="inline">
        <label>Purge After Load</label>
        <input id="sfPurge" data-field="sf_purge" type="checkbox" />
      </div>
    </div>
  </div>

  <div class="card" id="warehouseJobsCard">
    <div class="grid">
      <div>
        <label for="bronzeJobId">Bronze Job ID</label>
        <input id="bronzeJobId" data-field="bronze_job_id" type="text" />
      </div>
      <div>
        <label for="silverJobId">Silver Job ID</label>
        <input id="silverJobId" data-field="silver_job_id" type="text" />
      </div>
      <div>
        <label for="goldJobId">Gold Job ID</label>
        <input id="goldJobId" data-field="gold_job_id" type="text" />
      </div>
    </div>
    <div class="grid" style="margin-top:12px;">
      <div>
        <label for="options">Target Options JSON</label>
        <textarea id="options" data-field="options"></textarea>
      </div>
      <div>
        <label for="bronzeJobParams">Bronze Job Params JSON</label>
        <textarea id="bronzeJobParams" data-field="bronze_job_params"></textarea>
      </div>
      <div>
        <label for="silverJobParams">Silver Job Params JSON</label>
        <textarea id="silverJobParams" data-field="silver_job_params"></textarea>
      </div>
      <div>
        <label for="goldJobParams">Gold Job Params JSON</label>
        <textarea id="goldJobParams" data-field="gold_job_params"></textarea>
      </div>
    </div>
    <div class="status" id="statusText"></div>
  </div>
</div>
`;

function currentRectangle() {
  const selectedId = window.data?.selectedRectangle;
  const items = window.data?.rectangles || window.data?.panelItems || [];
  return items.find((item) => String(item.id) === String(selectedId)) || null;
}

function parseJson(value) {
  const text = String(value || "").trim();
  if (!text) return {};
  return JSON.parse(text);
}

class TargetComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.savedState = null;
    this.connectionCatalog = null;
    this.connectionDetailCache = new Map();
    this.connectionDbType = null;
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this.bind();
    this.closeImmediately();
  }

  disconnectedCallback() {
    EventHandler.removeGroup("Target");
  }

  attributeChangedCallback(name, _oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") this.open();
    else this.close();
  }

  bind() {
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.closeX = this.shadowRoot.querySelector("#closeX");
    this.statusText = this.shadowRoot.querySelector("#statusText");
  }

  hydrate() {
    const rect = currentRectangle() || {};
    return {
      id: rect.id || null,
      target_kind: rect.target_kind || "databricks",
      connection_dbtype: rect.connection_dbtype || null,
      connection_id: rect.connection_id ?? rect.c ?? "",
      catalog: rect.catalog || "",
      schema: rect.schema || "",
      table_name: rect.table_name || "",
      write_mode: rect.write_mode === "overwrite" ? "replace" : (rect.write_mode || "append"),
      table_format: rect.table_format || "delta",
      partition_columns: Array.isArray(rect.partition_columns) ? rect.partition_columns : [],
      merge_keys: Array.isArray(rect.merge_keys) ? rect.merge_keys : [],
      cluster_by: Array.isArray(rect.cluster_by) ? rect.cluster_by : [],
      options: rect.options || {},
      bronze_job_id: rect.bronze_job_id || "",
      silver_job_id: rect.silver_job_id || "",
      gold_job_id: rect.gold_job_id || "",
      bronze_job_params: rect.bronze_job_params || {},
      silver_job_params: rect.silver_job_params || {},
      gold_job_params: rect.gold_job_params || {},
      trigger_gold_on_success: Boolean(rect.trigger_gold_on_success),
      create_table: Boolean(rect.create_table),
      truncate: Boolean(rect.truncate),
      sf_load_method: rect.sf_load_method || "jdbc",
      sf_stage_name: rect.sf_stage_name || "",
      sf_warehouse: rect.sf_warehouse || "",
      sf_file_format: rect.sf_file_format || "",
      sf_on_error: rect.sf_on_error || "ABORT_STATEMENT",
      sf_purge: Boolean(rect.sf_purge),
    };
  }

  effectiveDbType() {
    return (this.state?.connection_dbtype || this.state?.target_kind || "").toLowerCase();
  }

  setRowVisible(rowId, visible) {
    const el = this.shadowRoot.querySelector(`#${rowId}`);
    if (el) el.style.display = visible ? "" : "none";
  }

  normalizeStateForTargetKind() {
    const kind = (this.state.target_kind || "").toLowerCase();
    this.state.connection_dbtype = kind || null;

    if (!["databricks", "snowflake", "bigquery"].includes(kind)) {
      this.state.catalog = "";
    }

    if (kind !== "databricks") {
      this.state.bronze_job_id = "";
      this.state.silver_job_id = "";
      this.state.gold_job_id = "";
      this.state.bronze_job_params = {};
      this.state.silver_job_params = {};
      this.state.gold_job_params = {};
      if (this.state.table_format === "delta") {
        this.state.table_format = "table";
      }
      this.state.partition_columns = [];
      this.state.cluster_by = [];
    }

    if (kind !== "snowflake") {
      this.state.sf_load_method = "jdbc";
      this.state.sf_stage_name = "";
      this.state.sf_warehouse = "";
      this.state.sf_file_format = "";
      this.state.sf_on_error = "ABORT_STATEMENT";
      this.state.sf_purge = false;
    }
  }

  updateFieldVisibility() {
    const kind = this.effectiveDbType();
    const isDatabricks = kind === "databricks";
    const isSnowflake = kind === "snowflake";
    const isBigQuery = kind === "bigquery";
    const isPostgresLike = kind === "postgresql" || kind === "mysql";

    this.setRowVisible("catalogRow", isDatabricks || isSnowflake || isBigQuery);
    this.setRowVisible("tableFormatRow", isDatabricks);
    this.setRowVisible("partitionColumnsRow", isDatabricks);
    this.setRowVisible("clusterByRow", isDatabricks);
    this.setRowVisible("warehouseJobsCard", isDatabricks);
    this.setRowVisible("triggerGoldOnSuccessRow", isDatabricks);

    const mergeKeysVisible = isDatabricks || isPostgresLike || isSnowflake || isBigQuery;
    this.setRowVisible("mergeKeysRow", mergeKeysVisible);

    const schemaLabel = this.shadowRoot.querySelector('label[for="schema"]');
    if (schemaLabel) {
      schemaLabel.textContent = isBigQuery ? "Dataset / Schema" : "Schema";
    }

    const catalogLabel = this.shadowRoot.querySelector('label[for="catalog"]');
    if (catalogLabel) {
      catalogLabel.textContent = isBigQuery ? "Project / Catalog" : "Catalog";
    }

    this.toggleSnowflakeCard();
  }

  open() {
    this.style.visibility = "visible";
    this.state = this.hydrate();
    this.savedState = JSON.stringify(this.state);
    this.render();
    this.attachEvents();
    this.bootstrapConnectionDefaults();
  }

  closeImmediately() {
    this.style.visibility = "hidden";
  }

  async close() {
    if (this.style.visibility === "hidden") return;
    if (JSON.stringify(this.state) !== this.savedState) {
      const discard = await customConfirm("Discard unsaved target changes?");
      if (!discard) return;
    }
    this.closeImmediately();
  }

  attachEvents() {
    EventHandler.removeGroup("Target");
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "Target");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "Target");
    EventHandler.on(this.closeX, "click", () => this.setAttribute("visibility", "close"), false, "Target");
    this.shadowRoot.querySelectorAll("[data-field]").forEach((el) => {
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (event) => this.updateField(event), false, "Target");
      if (el.type === "checkbox") {
        EventHandler.on(el, "change", (event) => this.updateField(event), false, "Target");
      }
    });

    const aiBtn = this.shadowRoot.querySelector("#aiExplainWriteMode");
    if (aiBtn) {
      EventHandler.on(aiBtn, "click", () => this.aiExplainWriteMode(), false, "Target");
    }
  }

  async aiExplainWriteMode() {
    const resultEl = this.shadowRoot.querySelector("#aiWriteModeResult");
    if (!resultEl) return;
    resultEl.innerHTML = renderAiLoading("Explaining write mode...");
    try {
      const result = await callAiEndpoint("/aiExplainTargetStrategy", {
        target_config: {
          target_kind:       this.state.target_kind,
          write_mode:        this.state.write_mode,
          table_format:      this.state.table_format,
          merge_keys:        this.state.merge_keys,
          partition_columns: this.state.partition_columns,
          cluster_by:        this.state.cluster_by,
        },
      });
      resultEl.innerHTML = renderAiCard(result, { title: "Write Mode Strategy" });
    } catch (err) {
      resultEl.innerHTML = `<div class="ai-warnings">&#9888; ${err.message || "AI request failed"}</div>`;
    }
  }

  toggleSnowflakeCard() {
    const card = this.shadowRoot.querySelector("#snowflakeCard");
    if (card) card.style.display = this.state.target_kind === "snowflake" ? "block" : "none";
  }

  async listConnections() {
    if (this.connectionCatalog) return this.connectionCatalog;
    const data = await request("/listConnections");
    this.connectionCatalog = Array.isArray(data?.connections) ? data.connections : [];
    if (this.style.visibility === "visible") this.render();
    return this.connectionCatalog;
  }

  filteredConnections() {
    const connections = Array.isArray(this.connectionCatalog) ? this.connectionCatalog : [];
    return connections.filter((conn) => conn.dbtype === this.state.target_kind);
  }

  describeConnectionOption(conn) {
    const detail = this.connectionDetailCache.get(String(conn.conn_id));
    const parts = [conn.label || `Connection ${conn.conn_id}`];
    if (detail?.dbname) parts.push(detail.dbname);
    if (detail?.schema) parts.push(detail.schema);
    else if (detail?.host) parts.push(detail.host);
    return `${conn.conn_id} - ${parts.join(" / ")}`;
  }

  async prefetchConnectionDetailsForCurrentKind() {
    const options = this.filteredConnections();
    if (!options.length) return;
    await Promise.all(options.map((conn) => this.getConnectionDetail(conn.conn_id).catch(() => null)));
    if (this.style.visibility === "visible") this.render();
  }

  renderConnectionOptions() {
    const select = this.shadowRoot.querySelector("#connectionId");
    if (!select) return;

    const options = this.filteredConnections();
    const current = this.state.connection_id ? String(this.state.connection_id) : "";

    if (!options.length) {
      select.innerHTML = `<option value="">No saved ${this.state.target_kind} connections</option>`;
      select.value = "";
      return;
    }

    const optionMarkup = options.map((conn) => {
      const value = String(conn.conn_id);
      const selected = value === current ? " selected" : "";
      return `<option value="${value}"${selected}>${this.describeConnectionOption(conn)}</option>`;
    }).join("");

    select.innerHTML = `<option value="">Select connection</option>${optionMarkup}`;
    select.value = options.some((conn) => String(conn.conn_id) === current) ? current : "";
  }

  async getConnectionDetail(connId) {
    if (!connId) return null;
    const key = String(connId);
    if (this.connectionDetailCache.has(key)) return this.connectionDetailCache.get(key);
    const detail = await request(`/getConnectionDetail?conn_id=${encodeURIComponent(key)}`);
    this.connectionDetailCache.set(key, detail);
    return detail;
  }

  applyConnectionDetail(detail) {
    if (!detail) return;
    this.state.connection_id = detail.id ?? this.state.connection_id;
    this.state.connection_dbtype = detail.dbtype || this.state.connection_dbtype;
    const detailKind = (detail.dbtype || this.state.target_kind || "").toLowerCase();
    if (["databricks", "snowflake", "bigquery"].includes(detailKind)) {
      if (!this.state.catalog) this.state.catalog = detail.catalog || detail.dbname || "";
    } else {
      this.state.catalog = "";
    }
    if (!this.state.schema) this.state.schema = detail.schema || "";
    this.normalizeStateForTargetKind();
    this.render();
    this.saveButton.disabled = JSON.stringify(this.state) === this.savedState;
  }

  async syncConnectionFromTargetKind() {
    const connections = await this.listConnections();
    const matches = connections.filter((conn) => conn.dbtype === this.state.target_kind);
    if (!matches.length) {
      this.state.connection_id = "";
      // Preserve saved catalog/schema even when no connection is found
      this.render();
      this.statusText.textContent = `No saved ${this.state.target_kind} connection found.`;
      return;
    }

    const selected = matches.find((conn) => String(conn.conn_id) === String(this.state.connection_id)) || matches[0];
    const detail = await this.getConnectionDetail(selected.conn_id);
    this.applyConnectionDetail(detail);
    this.statusText.textContent = `Loaded connection ${selected.conn_id} for ${this.state.target_kind}.`;
  }

  async syncMetadataFromConnectionId() {
    if (!this.state.connection_id) {
      // Preserve saved catalog/schema when no connection is set
      this.render();
      this.saveButton.disabled = JSON.stringify(this.state) === this.savedState;
      return;
    }
    const detail = await this.getConnectionDetail(this.state.connection_id);
    this.applyConnectionDetail(detail);
    this.statusText.textContent = `Loaded catalog/schema from connection ${this.state.connection_id}.`;
  }

  async bootstrapConnectionDefaults() {
    try {
      await this.listConnections();
      await this.prefetchConnectionDetailsForCurrentKind();
      if (this.state.connection_id) await this.syncMetadataFromConnectionId();
      else await this.syncConnectionFromTargetKind();
    } catch (error) {
      this.statusText.textContent = error.message || "Could not load target connection defaults.";
    }
  }

  async updateField(event) {
    const field = event.target.dataset.field;
    const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
    if (["partition_columns", "merge_keys", "cluster_by"].includes(field)) {
      this.state[field] = value.split(",").map((item) => item.trim()).filter(Boolean);
    } else if (field === "connection_id") {
      this.state[field] = value ? Number(value) : "";
    } else {
      this.state[field] = value;
    }
    if (field === "target_kind") {
      this.normalizeStateForTargetKind();
      this.toggleSnowflakeCard();
      try {
        await this.prefetchConnectionDetailsForCurrentKind();
        await this.syncConnectionFromTargetKind();
      } catch (error) {
        this.statusText.textContent = error.message || "Could not load connection for target kind.";
      }
    } else if (field === "connection_id") {
      try {
        await this.syncMetadataFromConnectionId();
      } catch (error) {
        this.statusText.textContent = error.message || "Could not load connection metadata.";
      }
    }
    this.saveButton.disabled = JSON.stringify(this.state) === this.savedState;
  }

  render() {
    const sr = this.shadowRoot;
    sr.querySelector("#targetKind").value = this.state.target_kind;
    this.renderConnectionOptions();
    sr.querySelector("#catalog").value = this.state.catalog;
    sr.querySelector("#schema").value = this.state.schema;
    sr.querySelector("#tableName").value = this.state.table_name;
    sr.querySelector("#writeMode").value = this.state.write_mode;
    sr.querySelector("#tableFormat").value = this.state.table_format;
    sr.querySelector("#partitionColumns").value = this.state.partition_columns.join(", ");
    sr.querySelector("#mergeKeys").value = this.state.merge_keys.join(", ");
    sr.querySelector("#clusterBy").value = this.state.cluster_by.join(", ");
    sr.querySelector("#options").value = JSON.stringify(this.state.options, null, 2);
    sr.querySelector("#bronzeJobId").value = this.state.bronze_job_id;
    sr.querySelector("#silverJobId").value = this.state.silver_job_id;
    sr.querySelector("#goldJobId").value = this.state.gold_job_id;
    sr.querySelector("#bronzeJobParams").value = JSON.stringify(this.state.bronze_job_params, null, 2);
    sr.querySelector("#silverJobParams").value = JSON.stringify(this.state.silver_job_params, null, 2);
    sr.querySelector("#goldJobParams").value = JSON.stringify(this.state.gold_job_params, null, 2);
    sr.querySelector("#triggerGoldOnSuccess").checked = this.state.trigger_gold_on_success;
    sr.querySelector("#createTable").checked = this.state.create_table;
    sr.querySelector("#truncate").checked = this.state.truncate;
    sr.querySelector("#sfLoadMethod").value = this.state.sf_load_method;
    sr.querySelector("#sfStageName").value = this.state.sf_stage_name;
    sr.querySelector("#sfWarehouse").value = this.state.sf_warehouse;
    sr.querySelector("#sfFileFormat").value = this.state.sf_file_format;
    sr.querySelector("#sfOnError").value = this.state.sf_on_error;
    sr.querySelector("#sfPurge").checked = this.state.sf_purge;
    this.updateFieldVisibility();
    this.saveButton.disabled = true;
  }

  payload() {
    return {
      id: this.state.id,
      target_kind: this.state.target_kind,
      connection_id: this.state.connection_id,
      catalog: this.state.catalog,
      schema: this.state.schema,
      table_name: this.state.table_name,
      write_mode: this.state.write_mode,
      table_format: this.state.table_format,
      partition_columns: this.state.partition_columns,
      merge_keys: this.state.merge_keys,
      cluster_by: this.state.cluster_by,
      options: parseJson(this.shadowRoot.querySelector("#options").value),
      bronze_job_id: this.state.bronze_job_id,
      silver_job_id: this.state.silver_job_id,
      gold_job_id: this.state.gold_job_id,
      bronze_job_params: parseJson(this.shadowRoot.querySelector("#bronzeJobParams").value),
      silver_job_params: parseJson(this.shadowRoot.querySelector("#silverJobParams").value),
      gold_job_params: parseJson(this.shadowRoot.querySelector("#goldJobParams").value),
      trigger_gold_on_success: this.state.trigger_gold_on_success,
      create_table: this.state.create_table,
      truncate: this.state.truncate,
      sf_load_method: this.state.sf_load_method,
      sf_stage_name: this.state.sf_stage_name,
      sf_warehouse: this.state.sf_warehouse,
      sf_file_format: this.state.sf_file_format,
      sf_on_error: this.state.sf_on_error,
      sf_purge: this.state.sf_purge,
    };
  }

  async save() {
    try {
      const response = await request("/saveTarget", { method: "POST", body: this.payload() });
      const rects = window.data?.rectangles || window.data?.panelItems || [];
      const idx = rects.findIndex((item) => String(item.id) === String(response.id));
      if (idx >= 0) rects[idx] = { ...rects[idx], ...response };
      this.state = this.hydrate();
      this.savedState = JSON.stringify(this.state);
      this.statusText.textContent = "Target saved.";
      this.render();
    } catch (error) {
      this.statusText.textContent = error.message || "Save failed.";
    }
  }
}

customElements.define("target-component", TargetComponent);
