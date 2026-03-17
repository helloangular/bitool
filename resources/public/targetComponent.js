import EventHandler from "./library/eventHandler.js";
import { customConfirm, request } from "./library/utils.js";

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
</style>
<div class="shell">
  <div class="header">
    <div class="title">
      <h2>Target</h2>
      <p>Configure Bronze destination and optional Databricks Silver/Gold job triggers.</p>
    </div>
    <div class="actions">
      <button id="saveButton" type="button">Save</button>
      <button id="closeButton" class="secondary" type="button">Close</button>
    </div>
  </div>

  <div class="card">
    <div class="grid">
      <div>
        <label for="targetKind">Target Kind</label>
        <select id="targetKind" data-field="target_kind">
          <option value="databricks">databricks</option>
          <option value="postgresql">postgresql</option>
          <option value="mysql">mysql</option>
          <option value="snowflake">snowflake</option>
        </select>
      </div>
      <div>
        <label for="connectionId">Connection ID</label>
        <input id="connectionId" data-field="connection_id" type="number" />
      </div>
      <div>
        <label for="catalog">Catalog</label>
        <input id="catalog" data-field="catalog" type="text" />
      </div>
      <div>
        <label for="schema">Schema</label>
        <input id="schema" data-field="schema" type="text" />
      </div>
      <div>
        <label for="tableName">Table Name</label>
        <input id="tableName" data-field="table_name" type="text" />
      </div>
      <div>
        <label for="writeMode">Write Mode</label>
        <select id="writeMode" data-field="write_mode">
          <option value="append">append</option>
          <option value="merge">merge</option>
          <option value="overwrite">overwrite</option>
        </select>
      </div>
      <div>
        <label for="tableFormat">Table Format</label>
        <select id="tableFormat" data-field="table_format">
          <option value="delta">delta</option>
          <option value="parquet">parquet</option>
          <option value="table">table</option>
        </select>
      </div>
      <div>
        <label for="partitionColumns">Partition Columns (CSV)</label>
        <input id="partitionColumns" data-field="partition_columns" type="text" />
      </div>
      <div>
        <label for="mergeKeys">Merge Keys (CSV)</label>
        <input id="mergeKeys" data-field="merge_keys" type="text" />
      </div>
      <div>
        <label for="clusterBy">Cluster By (CSV)</label>
        <input id="clusterBy" data-field="cluster_by" type="text" />
      </div>
      <div class="inline">
        <label>Create Table</label>
        <input id="createTable" data-field="create_table" type="checkbox" />
      </div>
      <div class="inline">
        <label>Truncate On Run</label>
        <input id="truncate" data-field="truncate" type="checkbox" />
      </div>
      <div class="inline">
        <label>Trigger Gold On Success</label>
        <input id="triggerGoldOnSuccess" data-field="trigger_gold_on_success" type="checkbox" />
      </div>
    </div>
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

  <div class="card">
    <div class="grid">
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
    this.statusText = this.shadowRoot.querySelector("#statusText");
  }

  hydrate() {
    const rect = currentRectangle() || {};
    return {
      id: rect.id || null,
      target_kind: rect.target_kind || "databricks",
      connection_id: rect.connection_id ?? rect.c ?? "",
      catalog: rect.catalog || "",
      schema: rect.schema || "",
      table_name: rect.table_name || "",
      write_mode: rect.write_mode || "append",
      table_format: rect.table_format || "delta",
      partition_columns: Array.isArray(rect.partition_columns) ? rect.partition_columns : [],
      merge_keys: Array.isArray(rect.merge_keys) ? rect.merge_keys : [],
      cluster_by: Array.isArray(rect.cluster_by) ? rect.cluster_by : [],
      options: rect.options || {},
      silver_job_id: rect.silver_job_id || "",
      gold_job_id: rect.gold_job_id || "",
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

  open() {
    this.style.visibility = "visible";
    this.state = this.hydrate();
    this.savedState = JSON.stringify(this.state);
    this.render();
    this.attachEvents();
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
    this.shadowRoot.querySelectorAll("[data-field]").forEach((el) => {
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (event) => this.updateField(event), false, "Target");
      if (el.type === "checkbox") {
        EventHandler.on(el, "change", (event) => this.updateField(event), false, "Target");
      }
    });
  }

  toggleSnowflakeCard() {
    const card = this.shadowRoot.querySelector("#snowflakeCard");
    if (card) card.style.display = this.state.target_kind === "snowflake" ? "block" : "none";
  }

  updateField(event) {
    const field = event.target.dataset.field;
    const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
    if (["partition_columns", "merge_keys", "cluster_by"].includes(field)) {
      this.state[field] = value.split(",").map((item) => item.trim()).filter(Boolean);
    } else if (field === "connection_id") {
      this.state[field] = value ? Number(value) : "";
    } else {
      this.state[field] = value;
    }
    if (field === "target_kind") this.toggleSnowflakeCard();
    this.saveButton.disabled = JSON.stringify(this.state) === this.savedState;
  }

  render() {
    const sr = this.shadowRoot;
    sr.querySelector("#targetKind").value = this.state.target_kind;
    sr.querySelector("#connectionId").value = this.state.connection_id;
    sr.querySelector("#catalog").value = this.state.catalog;
    sr.querySelector("#schema").value = this.state.schema;
    sr.querySelector("#tableName").value = this.state.table_name;
    sr.querySelector("#writeMode").value = this.state.write_mode;
    sr.querySelector("#tableFormat").value = this.state.table_format;
    sr.querySelector("#partitionColumns").value = this.state.partition_columns.join(", ");
    sr.querySelector("#mergeKeys").value = this.state.merge_keys.join(", ");
    sr.querySelector("#clusterBy").value = this.state.cluster_by.join(", ");
    sr.querySelector("#options").value = JSON.stringify(this.state.options, null, 2);
    sr.querySelector("#silverJobId").value = this.state.silver_job_id;
    sr.querySelector("#goldJobId").value = this.state.gold_job_id;
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
    this.toggleSnowflakeCard();
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
      silver_job_id: this.state.silver_job_id,
      gold_job_id: this.state.gold_job_id,
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
