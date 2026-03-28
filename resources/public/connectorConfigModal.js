import EventHandler from "./library/eventHandler.js";
import { request } from "./library/utils.js";
import { updateTreeItems, findItemData, mapItems, addTableMetadata } from "./library/tree-store.js";

const FORMS = {
  kafka_stream: {
    title: "Kafka Stream Connection",
    subtitle: "Continuous stream ingest with offset-after-commit semantics",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "bootstrap_servers", label: "Bootstrap Servers", type: "text", required: true, placeholder: "broker1:9092,broker2:9092" },
      { name: "security_protocol", label: "Security Protocol", type: "select", options: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"], value: "PLAINTEXT" },
      { name: "sasl_mechanism", label: "SASL Mechanism", type: "select", options: ["", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"], value: "" },
      { name: "sasl_username", label: "SASL Username", type: "text" },
      { name: "sasl_password", label: "SASL Password", type: "password" },
      { name: "consumer_group_id", label: "Consumer Group ID", type: "text", required: true, placeholder: "bitool-ingest-group" },
      { name: "schema_registry_url", label: "Schema Registry URL", type: "text", placeholder: "http://registry:8081 (optional)" },
      { name: "auto_offset_reset", label: "Auto Offset Reset", type: "select", options: ["earliest", "latest"], value: "earliest" },
      { name: "max_poll_records", label: "Max Poll Records", type: "number", value: "500" },
      { name: "session_timeout_ms", label: "Session Timeout (ms)", type: "number", value: "30000" },
      { name: "heartbeat_interval_ms", label: "Heartbeat Interval (ms)", type: "number", value: "10000" },
    ],
    treeParent: "Kafka Stream",
    saveUrl: "/saveConnectorConnection",
    nodetype: "kafka-source",
  },
  kafka_consumer: {
    title: "Kafka Consumer Connection",
    subtitle: "Batch poll consumer for bounded topic reads",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "bootstrap_servers", label: "Bootstrap Servers", type: "text", required: true, placeholder: "broker1:9092,broker2:9092" },
      { name: "security_protocol", label: "Security Protocol", type: "select", options: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"], value: "PLAINTEXT" },
      { name: "sasl_mechanism", label: "SASL Mechanism", type: "select", options: ["", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"], value: "" },
      { name: "sasl_username", label: "SASL Username", type: "text" },
      { name: "sasl_password", label: "SASL Password", type: "password" },
      { name: "consumer_group_id", label: "Consumer Group ID", type: "text", required: true, placeholder: "bitool-batch-group" },
      { name: "auto_offset_reset", label: "Auto Offset Reset", type: "select", options: ["earliest", "latest"], value: "earliest" },
      { name: "max_poll_records", label: "Max Poll Records", type: "number", value: "500" },
      { name: "poll_timeout_ms", label: "Poll Timeout (ms)", type: "number", value: "5000" },
      { name: "max_poll_cycles", label: "Max Poll Cycles (0=unlimited)", type: "number", value: "0" },
    ],
    treeParent: "Kafka Consumer",
    saveUrl: "/saveConnectorConnection",
    nodetype: "kafka-source",
  },
  local_files: {
    title: "Local / Mounted File Connection",
    subtitle: "Local filesystem, NFS mount, or shared volume paths",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "transport", label: "Transport", type: "select", options: ["local"], value: "local" },
      { name: "base_path", label: "Base Path", type: "text", required: true, placeholder: "/data/ingest/ or /mnt/share/" },
      { name: "file_pattern", label: "File Pattern", type: "text", placeholder: "*.csv, *.jsonl, *.dat" },
      { name: "format", label: "Default Format", type: "select", options: ["csv", "jsonl", "fixed_width", "parquet"], value: "csv" },
      { name: "delimiter", label: "CSV Delimiter", type: "text", value: ",", placeholder: "," },
      { name: "has_header", label: "CSV Has Header", type: "checkbox", value: true },
      { name: "encoding", label: "Encoding", type: "select", options: ["UTF-8", "ISO-8859-1", "Windows-1252"], value: "UTF-8" },
      { name: "poll_interval_seconds", label: "Poll Interval (sec)", type: "number", value: "60" },
    ],
    treeParent: "Files",
    saveUrl: "/saveConnectorConnection",
    nodetype: "file-source",
  },
  remote_files: {
    title: "Remote File Connection (S3 / Azure Blob / SFTP)",
    subtitle: "Cloud storage or SFTP file sources",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "transport", label: "Transport", type: "select", options: ["s3", "azure_blob", "sftp"], value: "s3" },
      { name: "base_path", label: "Bucket / Container / Path", type: "text", required: true, placeholder: "s3://bucket/prefix/ or /remote/path/" },
      { name: "remote_base_url", label: "Remote Base URL", type: "text", placeholder: "https://storage.example.com (optional)" },
      { name: "access_key", label: "Access Key / Username", type: "text" },
      { name: "secret_key", label: "Secret Key / Password", type: "password" },
      { name: "region", label: "Region", type: "text", placeholder: "us-east-1" },
      { name: "file_pattern", label: "File Pattern", type: "text", placeholder: "*.csv, *.jsonl" },
      { name: "format", label: "Default Format", type: "select", options: ["csv", "jsonl", "fixed_width", "parquet"], value: "csv" },
      { name: "encoding", label: "Encoding", type: "select", options: ["UTF-8", "ISO-8859-1", "Windows-1252"], value: "UTF-8" },
    ],
    treeParent: "Files",
    saveUrl: "/saveConnectorConnection",
    nodetype: "file-source",
  },
  mainframe_files: {
    title: "Mainframe File Connection",
    subtitle: "EBCDIC fixed-width files with COBOL copybook layout",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "transport", label: "Transport", type: "select", options: ["local", "sftp", "s3"], value: "local" },
      { name: "base_path", label: "Base Path", type: "text", required: true, placeholder: "/data/mainframe/ or sftp://host/path/" },
      { name: "access_key", label: "Username / Access Key", type: "text" },
      { name: "secret_key", label: "Password / Secret Key", type: "password" },
      { name: "encoding", label: "Encoding", type: "select", options: ["EBCDIC", "IBM037", "CP037", "IBM1047", "CP1047", "UTF-8"], value: "EBCDIC" },
      { name: "format", label: "Format", type: "select", options: ["fixed_width"], value: "fixed_width" },
      { name: "copybook", label: "COBOL Copybook", type: "textarea", placeholder: "05 CUSTOMER-ID    PIC 9(8).\n05 CUSTOMER-NAME  PIC X(30).\n05 BALANCE        PIC S9(7)V99 COMP-3." },
      { name: "record_length", label: "Record Length (bytes)", type: "number", placeholder: "Auto-detect from copybook" },
      { name: "skip_header_bytes", label: "Skip Header Bytes", type: "number", value: "0" },
    ],
    treeParent: "Mainframe",
    saveUrl: "/saveConnectorConnection",
    nodetype: "file-source",
  },

  // ── Database connections ──
  postgresql: {
    title: "PostgreSQL Connection",
    subtitle: "Connect to a PostgreSQL database",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "host", label: "Host", type: "text", required: true, placeholder: "localhost" },
      { name: "port", label: "Port", type: "number", value: "5432", required: true },
      { name: "dbname", label: "Database", type: "text", required: true },
      { name: "schema", label: "Schema", type: "text", value: "public" },
      { name: "username", label: "Username", type: "text", required: true },
      { name: "password", label: "Password", type: "password", required: true },
    ],
    treeParent: "RDBMS",
    saveUrl: "/saveDbConnection",
    testUrl: "/testDbConnection",
    dbtype: "postgresql",
  },
  oracle: {
    title: "Oracle Connection",
    subtitle: "Connect to an Oracle database",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "host", label: "Host", type: "text", required: true },
      { name: "port", label: "Port", type: "number", value: "1521", required: true },
      { name: "sid", label: "SID", type: "text", placeholder: "ORCL (use SID or Service Name)" },
      { name: "service", label: "Service Name", type: "text", placeholder: "service.name" },
      { name: "username", label: "Username", type: "text", required: true },
      { name: "password", label: "Password", type: "password", required: true },
    ],
    treeParent: "RDBMS",
    saveUrl: "/saveDbConnection",
    testUrl: "/testDbConnection",
    dbtype: "oracle",
  },
  sql_server: {
    title: "SQL Server Connection",
    subtitle: "Connect to a Microsoft SQL Server database",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "host", label: "Host", type: "text", required: true },
      { name: "port", label: "Port", type: "number", value: "1433", required: true },
      { name: "dbname", label: "Database", type: "text", required: true },
      { name: "schema", label: "Schema", type: "text", value: "dbo" },
      { name: "username", label: "Username", type: "text", required: true },
      { name: "password", label: "Password", type: "password", required: true },
    ],
    treeParent: "RDBMS",
    saveUrl: "/saveDbConnection",
    testUrl: "/testDbConnection",
    dbtype: "sqlserver",
  },
  snowflake: {
    title: "Snowflake Connection",
    subtitle: "Connect to Snowflake data warehouse",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "host", label: "Account URL", type: "text", required: true, placeholder: "xyz12345.snowflakecomputing.com" },
      { name: "dbname", label: "Database", type: "text", required: true },
      { name: "schema", label: "Schema", type: "text", value: "public" },
      { name: "warehouse", label: "Warehouse", type: "text", required: true, placeholder: "COMPUTE_WH" },
      { name: "role", label: "Role", type: "text", placeholder: "SYSADMIN" },
      { name: "username", label: "Username", type: "text", required: true },
      { name: "password", label: "Password", type: "password", required: true },
    ],
    treeParent: "Snowflake",
    saveUrl: "/saveDbConnection",
    testUrl: "/testDbConnection",
    dbtype: "snowflake",
  },
  databricks: {
    title: "Databricks Connection",
    subtitle: "Connect to Databricks SQL warehouse or cluster",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "host", label: "Server Hostname", type: "text", required: true, placeholder: "adb-123456.azuredatabricks.net" },
      { name: "port", label: "Port", type: "number", value: "443" },
      { name: "http_path", label: "HTTP Path", type: "text", required: true, placeholder: "/sql/1.0/warehouses/abc123" },
      { name: "catalog", label: "Catalog", type: "text", required: true, placeholder: "main" },
      { name: "schema", label: "Schema", type: "text", value: "default" },
      { name: "token", label: "Personal Access Token", type: "password", required: true },
    ],
    treeParent: "Databricks",
    saveUrl: "/saveDbConnection",
    testUrl: "/testDbConnection",
    dbtype: "databricks",
  },
  bigquery: {
    title: "Google BigQuery Connection",
    subtitle: "Connect to Google Cloud BigQuery",
    fields: [
      { name: "connection_name", label: "Connection Name", type: "text", required: true },
      { name: "host", label: "Project ID", type: "text", required: true, placeholder: "my-gcp-project" },
      { name: "dbname", label: "Dataset", type: "text", required: true, placeholder: "my_dataset" },
      { name: "schema", label: "Location", type: "text", value: "US", placeholder: "US, EU, etc." },
      { name: "token", label: "Service Account JSON Key", type: "textarea", required: true, placeholder: '{"type":"service_account","project_id":"...",...}' },
    ],
    treeParent: "GCP",
    saveUrl: "/saveDbConnection",
    testUrl: "/testDbConnection",
    dbtype: "bigquery",
  },
};

function buildTemplate() {
  const t = document.createElement("template");
  t.innerHTML = `
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  :host { display: none; }
  :host([open]) { display: block; }
  * { box-sizing: border-box; }
  .backdrop {
    position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
    background: rgba(26,29,38,0.25); backdrop-filter: blur(3px);
    z-index: 9000;
    display: flex; align-items: center; justify-content: center;
  }
  .modal {
    background: #fff; width: 540px; max-width: 92vw; max-height: 86vh;
    overflow-y: auto; border-radius: 16px;
    box-shadow: 0 16px 48px rgba(0,0,0,0.14), 0 0 0 1px rgba(0,0,0,0.04);
    font-family: 'DM Sans', -apple-system, sans-serif; color: #1a1d26;
  }
  .modal::-webkit-scrollbar { width: 5px; }
  .modal::-webkit-scrollbar-thumb { background: #e2e4ea; border-radius: 3px; }
  .modal-header {
    padding: 20px 24px 14px; border-bottom: 1px solid #eceef2;
    display: flex; justify-content: space-between; align-items: flex-start;
    background: linear-gradient(135deg, #eef2ff 0%, #f5f6f8 100%);
    border-radius: 16px 16px 0 0;
  }
  .modal-header h3 { margin: 0; font-size: 16px; font-weight: 700; color: #1a1d26; }
  .modal-header p { margin: 4px 0 0; font-size: 11.5px; color: #8b91a3; font-weight: 500; }
  .modal-header button {
    width: 28px; height: 28px; border-radius: 6px; border: 1px solid #e2e4ea;
    background: #f5f6f8; color: #5c6070; font-size: 16px;
    cursor: pointer; display: flex; align-items: center; justify-content: center;
    transition: 0.2s;
  }
  .modal-header button:hover { background: #e2e4ea; color: #1a1d26; }
  .modal-body { padding: 18px 24px; }
  .field { margin-bottom: 14px; }
  .field label {
    display: block; font-size: 11.5px; font-weight: 600; color: #5c6070;
    margin-bottom: 5px; letter-spacing: 0.2px;
  }
  .field label .req { color: #e5484d; }
  .field input, .field select, .field textarea {
    width: 100%; border: 1px solid #e2e4ea; border-radius: 8px;
    background: #fff; padding: 9px 12px; font-size: 13px; color: #1a1d26;
    font-family: 'DM Sans', -apple-system, sans-serif;
    transition: 0.2s;
  }
  .field input:focus, .field select:focus, .field textarea:focus {
    outline: none; border-color: #6366f1; box-shadow: 0 0 0 3px rgba(99,102,241,0.15);
  }
  .field input::placeholder, .field textarea::placeholder { color: #8b91a3; }
  .field textarea {
    min-height: 90px; resize: vertical;
    font-family: 'JetBrains Mono', monospace; font-size: 12px;
  }
  .field .checkbox-row { display: flex; align-items: center; gap: 8px; }
  .field .checkbox-row input { width: auto; accent-color: #6366f1; }
  .modal-footer {
    padding: 14px 24px 18px; border-top: 1px solid #eceef2;
    display: flex; justify-content: flex-end; gap: 8px;
    background: #fafbfc;
    border-radius: 0 0 16px 16px;
  }
  button.primary {
    background: #6366f1; color: #fff; border: 1px solid #6366f1;
    padding: 8px 20px; cursor: pointer; font-size: 12.5px; font-weight: 600;
    font-family: 'DM Sans', sans-serif; border-radius: 8px; transition: 0.2s;
  }
  button.primary:hover { background: #4f46e5; border-color: #4f46e5; transform: translateY(-1px); box-shadow: 0 4px 16px rgba(99, 102, 241, 0.12); }
  button.secondary {
    background: #fff; color: #5c6070; border: 1px solid #e2e4ea;
    padding: 8px 20px; cursor: pointer; font-size: 12.5px; font-weight: 500;
    font-family: 'DM Sans', sans-serif; border-radius: 8px; transition: 0.2s;
  }
  button.secondary:hover { background: #f0f1f4; color: #1a1d26; border-color: #8b91a3; }
  button:disabled { opacity: 0.4; cursor: not-allowed; }
  .status { font-size: 11.5px; color: #8b91a3; min-height: 18px; margin-top: 4px; padding: 0 24px; font-weight: 500; }
  .status.error { color: #e5484d; }
  .status.success { color: #0fa968; }
</style>
<div class="backdrop">
  <div class="modal">
    <div class="modal-header">
      <div>
        <h3 id="modalTitle"></h3>
        <p id="modalSubtitle"></p>
      </div>
      <button id="closeX" type="button">&times;</button>
    </div>
    <div class="modal-body" id="modalBody"></div>
    <div class="status" id="statusText"></div>
    <div class="modal-footer">
      <button id="testBtn" class="secondary" type="button">Test Connection</button>
      <button id="saveBtn" class="primary" type="button">Save</button>
      <button id="cancelBtn" class="secondary" type="button">Cancel</button>
    </div>
  </div>
</div>
`;
  return t;
}

class ConnectorConfigModal extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._formType = null;
    this._treeLabel = null;
  }

  static get observedAttributes() { return ["open"]; }

  connectedCallback() {
    this.shadowRoot.appendChild(buildTemplate().content.cloneNode(true));
    this._bindElements();
    this._attachEvents();
  }

  disconnectedCallback() {
    EventHandler.removeGroup("ConnModal");
  }

  _bindElements() {
    const q = (s) => this.shadowRoot.querySelector(s);
    this.$title = q("#modalTitle");
    this.$subtitle = q("#modalSubtitle");
    this.$body = q("#modalBody");
    this.$status = q("#statusText");
    this.$saveBtn = q("#saveBtn");
    this.$testBtn = q("#testBtn");
    this.$cancelBtn = q("#cancelBtn");
    this.$closeX = q("#closeX");
    this.$backdrop = q(".backdrop");
  }

  _attachEvents() {
    EventHandler.removeGroup("ConnModal");
    const on = (el, ev, fn) => EventHandler.on(el, ev, fn, false, "ConnModal");
    on(this.$closeX, "click", () => this.close());
    on(this.$cancelBtn, "click", () => this.close());
    on(this.$saveBtn, "click", () => this._save());
    on(this.$testBtn, "click", () => this._test());
    on(this.$backdrop, "click", (e) => { if (e.target === this.$backdrop) this.close(); });
  }

  open(formType, treeLabel) {
    const config = FORMS[formType];
    if (!config) { console.error("Unknown connector form type:", formType); return; }
    this._formType = formType;
    this._treeLabel = treeLabel;
    this.$title.textContent = config.title;
    this.$subtitle.textContent = config.subtitle;
    this._renderFields(config.fields);
    this.$status.textContent = "";
    this.$status.className = "status";
    this.setAttribute("open", "");
  }

  close() {
    this.removeAttribute("open");
    this._formType = null;
    this._editConnId = null;
  }

  async openForEdit(formType, treeLabel, connId) {
    const config = FORMS[formType];
    if (!config) { console.error("Unknown connector form type:", formType); return; }
    this._formType = formType;
    this._treeLabel = treeLabel;
    this._editConnId = connId;
    this.$title.textContent = config.title + " (Edit)";
    this.$subtitle.textContent = config.subtitle;
    this._renderFields(config.fields);
    this.$status.textContent = "Loading...";
    this.$status.className = "status";
    this.setAttribute("open", "");

    try {
      const data = await request(`/getConnectionDetail?conn_id=${connId}`);
      for (const f of config.fields) {
        const el = this.$body.querySelector(`[name="${f.name}"]`);
        if (!el) continue;
        const val = data[f.name];
        if (val != null) el.value = val;
      }
      this.$status.textContent = "";
    } catch (e) {
      this.$status.textContent = "Failed to load: " + (e.message || e);
      this.$status.className = "status error";
    }
  }

  _renderFields(fields) {
    this.$body.innerHTML = "";
    for (const f of fields) {
      const div = document.createElement("div");
      div.className = "field";

      if (f.type === "checkbox") {
        div.innerHTML = `
          <div class="checkbox-row">
            <input type="checkbox" id="f_${f.name}" name="${f.name}" ${f.value ? "checked" : ""} />
            <label for="f_${f.name}">${f.label}</label>
          </div>`;
      } else {
        const req = f.required ? '<span class="req">*</span>' : "";
        let input;
        if (f.type === "select") {
          const opts = (f.options || []).map((o) =>
            `<option value="${o}" ${o === (f.value || "") ? "selected" : ""}>${o || "(none)"}</option>`
          ).join("");
          input = `<select id="f_${f.name}" name="${f.name}">${opts}</select>`;
        } else if (f.type === "textarea") {
          input = `<textarea id="f_${f.name}" name="${f.name}" placeholder="${f.placeholder || ""}">${f.value || ""}</textarea>`;
        } else {
          input = `<input type="${f.type || "text"}" id="f_${f.name}" name="${f.name}"
                     value="${f.value || ""}" placeholder="${f.placeholder || ""}"
                     ${f.required ? "required" : ""} />`;
        }
        div.innerHTML = `<label for="f_${f.name}">${f.label} ${req}</label>${input}`;
      }
      this.$body.appendChild(div);
    }
  }

  _collectValues() {
    const values = { connector_type: this._formType };
    this.$body.querySelectorAll("input, select, textarea").forEach((el) => {
      if (el.type === "checkbox") values[el.name] = el.checked;
      else if (el.type === "number") values[el.name] = el.value ? Number(el.value) : null;
      else values[el.name] = el.value;
    });
    return values;
  }

  async _save() {
    const config = FORMS[this._formType];
    if (!config) return;
    const values = this._collectValues();

    // Basic required validation
    for (const f of config.fields) {
      if (f.required && !values[f.name]) {
        this.$status.textContent = `${f.label} is required.`;
        this.$status.className = "status error";
        return;
      }
    }

    this.$saveBtn.disabled = true;
    this.$status.textContent = "Saving...";
    this.$status.className = "status";

    try {
      if (config.dbtype) values.dbtype = config.dbtype;

      if (this._editConnId) {
        // Update existing connection
        values.conn_id = this._editConnId;
        await request("/updateDbConnection", { method: "POST", body: values });
        this.$status.textContent = "Connection updated.";
        this.$status.className = "status success";
        setTimeout(() => this.close(), 800);
      } else {
        // Create new connection
        const data = await request(config.saveUrl, { method: "POST", body: values });

        // Update tree sidebar
        const parentLabel = this._treeLabel || config.treeParent;
        updateTreeItems((treeItems) => {
          if (!Array.isArray(treeItems)) return treeItems;
          const parent = findItemData(parentLabel, treeItems);
          if (!parent) return treeItems;
          const connId = data?.["conn-id"] || data?.connection_id || "";
          const newItem = data?.["tree-data"] || { label: values.connection_name, items: [] };
          newItem.conn_id = connId;
          if (config.dbtype) newItem.dbtype = config.dbtype;
          if (config.nodetype) newItem.nodetype = config.nodetype;
          return mapItems(parent, treeItems, addTableMetadata(newItem, connId, values.connection_name));
        });

        const tree = document.querySelector("tree-component");
        if (tree?.renderItems) tree.renderItems();

        this.$status.textContent = "Connection saved.";
        this.$status.className = "status success";
        setTimeout(() => this.close(), 800);
      }
    } catch (e) {
      this.$status.textContent = "Save failed: " + (e.message || e);
      this.$status.className = "status error";
    } finally {
      this.$saveBtn.disabled = false;
    }
  }

  async _test() {
    const values = this._collectValues();
    this.$testBtn.disabled = true;
    this.$status.textContent = "Testing connection...";
    this.$status.className = "status";

    try {
      const config = FORMS[this._formType];
      if (config?.dbtype) values.dbtype = config.dbtype;
      const testUrl = config?.testUrl || "/testConnectorConnection";
      await request(testUrl, { method: "POST", body: values });
      this.$status.textContent = "Connection successful.";
      this.$status.className = "status success";
    } catch (e) {
      this.$status.textContent = "Connection failed: " + (e.message || e);
      this.$status.className = "status error";
    } finally {
      this.$testBtn.disabled = false;
    }
  }
}

customElements.define("connector-config-modal", ConnectorConfigModal);
