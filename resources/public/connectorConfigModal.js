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
    treeParent: "Kafka",
    saveUrl: "/saveConnectorConnection",
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
    treeParent: "Kafka",
    saveUrl: "/saveConnectorConnection",
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
  },
};

function buildTemplate() {
  const t = document.createElement("template");
  t.innerHTML = `
<style>
  :host { display: none; }
  :host([open]) { display: block; }
  .backdrop {
    position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
    background: rgba(0,0,0,0.4); z-index: 9000;
    display: flex; align-items: center; justify-content: center;
  }
  .modal {
    background: #fff; width: 520px; max-width: 92vw; max-height: 86vh;
    overflow-y: auto; box-shadow: 0 12px 40px rgba(0,0,0,0.18);
    font-family: Georgia, "Times New Roman", serif; color: #1a1a2e;
  }
  .modal-header {
    padding: 18px 20px 12px; border-bottom: 1px solid #e5e7eb;
    display: flex; justify-content: space-between; align-items: flex-start;
  }
  .modal-header h3 { margin: 0; font-size: 18px; font-weight: 700; }
  .modal-header p { margin: 4px 0 0; font-size: 12px; color: #6b7280; }
  .modal-header button {
    background: none; border: none; font-size: 20px; cursor: pointer;
    color: #6b7280; padding: 0 4px; line-height: 1;
  }
  .modal-body { padding: 16px 20px; }
  .field { margin-bottom: 12px; }
  .field label {
    display: block; font-size: 12px; font-weight: 600; color: #374151;
    margin-bottom: 3px;
  }
  .field label .req { color: #dc2626; }
  .field input, .field select, .field textarea {
    width: 100%; box-sizing: border-box; border: 1px solid #d1d5db;
    background: #fff; padding: 7px 10px; font-size: 13px; color: #1a1a2e;
    font-family: inherit;
  }
  .field input:focus, .field select:focus, .field textarea:focus {
    outline: none; border-color: #2563eb; box-shadow: 0 0 0 2px rgba(37,99,235,0.1);
  }
  .field textarea {
    min-height: 90px; resize: vertical;
    font-family: "SFMono-Regular", Consolas, monospace; font-size: 12px;
  }
  .field .checkbox-row { display: flex; align-items: center; gap: 8px; }
  .field .checkbox-row input { width: auto; }
  .modal-footer {
    padding: 12px 20px 16px; border-top: 1px solid #e5e7eb;
    display: flex; justify-content: flex-end; gap: 8px;
  }
  button.primary {
    background: #203326; color: #fffdf8; border: 1px solid #203326;
    padding: 8px 18px; cursor: pointer; font-size: 13px; font-family: inherit;
  }
  button.secondary {
    background: transparent; color: #203326; border: 1px solid #203326;
    padding: 8px 18px; cursor: pointer; font-size: 13px; font-family: inherit;
  }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  .status { font-size: 12px; color: #6b7280; min-height: 18px; margin-top: 4px; padding: 0 20px; }
  .status.error { color: #dc2626; }
  .status.success { color: #16a34a; }
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
      const data = await request(config.saveUrl, { method: "POST", body: values });

      // Update tree sidebar
      const parentLabel = this._treeLabel || config.treeParent;
      updateTreeItems((treeItems) => {
        if (!Array.isArray(treeItems)) return treeItems;
        const parent = findItemData(parentLabel, treeItems);
        if (!parent) return treeItems;
        const newItem = data?.["tree-data"] || {
          label: values.connection_name,
          conn_id: data?.["conn-id"] || data?.connection_id,
          items: [],
        };
        const connId = data?.["conn-id"] || data?.connection_id || "";
        return mapItems(parent, treeItems, addTableMetadata(newItem, connId, values.connection_name));
      });

      // Re-render tree
      const tree = document.querySelector("tree-component");
      if (tree?.renderItems) tree.renderItems();

      this.$status.textContent = "Connection saved.";
      this.$status.className = "status success";
      setTimeout(() => this.close(), 800);
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
      await request("/testConnectorConnection", { method: "POST", body: values });
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
