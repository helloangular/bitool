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
  .title h2 { margin: 0; font-size: 28px; font-weight: 600; }
  .title p { margin: 4px 0 0 0; color: #5c6c60; font-size: 14px; }
  .actions { display: flex; gap: 10px; flex-wrap: wrap; }
  button {
    border: 1px solid #203326;
    background: #203326;
    color: #fffdf8;
    padding: 10px 14px;
    cursor: pointer;
  }
  button.secondary { background: transparent; color: #203326; }
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
  label { display: block; font-size: 13px; color: #526055; margin-bottom: 4px; }
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
  .inline { display: flex; align-items: center; gap: 8px; }
  .inline input[type="checkbox"] { width: auto; }
  .status { min-height: 20px; color: #526055; font-size: 13px; margin-top: 10px; }
  h3 { margin: 0 0 10px 0; font-size: 16px; font-weight: 600; color: #203326; }
  .file-entry { border: 1px solid rgba(32,51,38,0.12); padding: 14px; margin-bottom: 10px; }
  .file-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
  .file-header span { font-weight: 600; font-size: 14px; }
  button.remove-file { background: #c00; border-color: #c00; padding: 4px 10px; font-size: 12px; }
  button.add-file { background: transparent; color: #203326; border: 1px dashed #203326; padding: 6px 14px; font-size: 13px; }
  .copybook-group { display: none; }
  .copybook-group.visible { display: block; }
</style>
<div class="shell">
  <div class="header">
    <div class="title">
      <h2>File Source</h2>
      <p>Configure file or mainframe file ingestion.</p>
    </div>
    <div class="actions">
      <button id="saveButton" type="button">Save</button>
      <button id="closeButton" class="secondary" type="button">Close</button>
    </div>
  </div>

  <div class="card">
    <h3>Connection</h3>
    <div class="grid">
      <div>
        <label for="sourceSystem">Source System</label>
        <input id="sourceSystem" data-field="source_system" type="text" />
      </div>
      <div>
        <label for="connectionId">Connection ID</label>
        <input id="connectionId" data-field="connection_id" type="number" />
      </div>
      <div>
        <label for="basePath">Base Path</label>
        <input id="basePath" data-field="base_path" type="text" placeholder="/data/incoming" />
      </div>
      <div>
        <label for="transport">Transport</label>
        <select id="transport" data-field="transport">
          <option value="local">local</option>
          <option value="s3">s3</option>
          <option value="sftp">sftp</option>
          <option value="azure_blob">azure_blob</option>
        </select>
      </div>
    </div>
  </div>

  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
      <h3 style="margin:0;">File Configs</h3>
      <button id="addFileBtn" class="add-file" type="button">+ Add File</button>
    </div>
    <div id="fileContainer"></div>
  </div>

  <div class="status" id="statusText"></div>
</div>
`;

function currentRectangle() {
  const selectedId = window.data?.selectedRectangle;
  const items = window.data?.rectangles || window.data?.panelItems || [];
  return items.find((item) => String(item.id) === String(selectedId)) || null;
}

function defaultFileConfig() {
  return {
    path: "",
    endpoint_name: "",
    enabled: true,
    transport: "local",
    format: "csv",
    delimiter: ",",
    has_header: true,
    encoding: "UTF-8",
    primary_key_fields: [],
    watermark_column: "",
    bronze_table_name: "",
    schema_mode: "manual",
    copybook: "",
    batch_flush_rows: 1000,
  };
}

class FileSourceComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.savedState = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this.bind();
    this.closeImmediately();
  }

  disconnectedCallback() { EventHandler.removeGroup("FileSource"); }

  attributeChangedCallback(name, _oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") this.open();
    else this.close();
  }

  bind() {
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.addFileBtn = this.shadowRoot.querySelector("#addFileBtn");
    this.fileContainer = this.shadowRoot.querySelector("#fileContainer");
    this.statusText = this.shadowRoot.querySelector("#statusText");
  }

  hydrate() {
    const rect = currentRectangle() || {};
    return {
      id: rect.id || null,
      source_system: rect.source_system || "file",
      connection_id: rect.connection_id ?? "",
      base_path: rect.base_path || "",
      transport: rect.transport || "local",
      file_configs: Array.isArray(rect.file_configs) && rect.file_configs.length
        ? rect.file_configs.map((fc) => ({ ...defaultFileConfig(), ...fc }))
        : [defaultFileConfig()],
    };
  }

  open() {
    this.style.visibility = "visible";
    this.state = this.hydrate();
    this.savedState = JSON.stringify(this.state);
    this.render();
    this.attachEvents();
  }

  closeImmediately() { this.style.visibility = "hidden"; }

  async close() {
    if (this.style.visibility === "hidden") return;
    if (JSON.stringify(this.state) !== this.savedState) {
      const discard = await customConfirm("Discard unsaved file source changes?");
      if (!discard) return;
    }
    this.closeImmediately();
  }

  markDirty() {
    this.saveButton.disabled = JSON.stringify(this.state) === this.savedState;
  }

  attachEvents() {
    EventHandler.removeGroup("FileSource");
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "FileSource");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "FileSource");
    EventHandler.on(this.addFileBtn, "click", () => {
      this.state.file_configs.push(defaultFileConfig());
      this.renderFiles();
      this.markDirty();
    }, false, "FileSource");

    this.shadowRoot.querySelectorAll("[data-field]").forEach((el) => {
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (e) => {
        const field = e.target.dataset.field;
        const value = e.target.value;
        if (field === "connection_id") {
          this.state[field] = value ? Number(value) : "";
        } else {
          this.state[field] = value;
        }
        this.markDirty();
      }, false, "FileSource");
    });
  }

  attachFileEvents() {
    this.fileContainer.querySelectorAll("[data-file-idx]").forEach((el) => {
      const idx = Number(el.dataset.fileIdx);
      const field = el.dataset.fileField;
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (e) => {
        const fc = this.state.file_configs[idx];
        if (!fc) return;
        const val = e.target.type === "checkbox" ? e.target.checked : e.target.value;
        if (field === "primary_key_fields") {
          fc[field] = val.split(",").map((s) => s.trim()).filter(Boolean);
        } else if (field === "batch_flush_rows") {
          fc[field] = val ? Number(val) : 0;
        } else {
          fc[field] = val;
        }
        // Toggle copybook visibility when format changes
        if (field === "format") {
          const copybookGroup = this.fileContainer.querySelector(`[data-copybook-group="${idx}"]`);
          if (copybookGroup) {
            copybookGroup.classList.toggle("visible", val === "fixed_width");
          }
        }
        this.markDirty();
      }, false, "FileSource");
    });

    this.fileContainer.querySelectorAll("[data-remove-file]").forEach((btn) => {
      EventHandler.on(btn, "click", () => {
        const idx = Number(btn.dataset.removeFile);
        this.state.file_configs.splice(idx, 1);
        this.renderFiles();
        this.markDirty();
      }, false, "FileSource");
    });
  }

  render() {
    const sr = this.shadowRoot;
    sr.querySelector("#sourceSystem").value = this.state.source_system;
    sr.querySelector("#connectionId").value = this.state.connection_id;
    sr.querySelector("#basePath").value = this.state.base_path;
    sr.querySelector("#transport").value = this.state.transport;
    this.renderFiles();
    this.saveButton.disabled = true;
  }

  renderFiles() {
    const html = this.state.file_configs.map((fc, idx) => `
      <div class="file-entry">
        <div class="file-header">
          <span>File ${idx + 1}</span>
          <button class="remove-file" data-remove-file="${idx}" type="button">Remove</button>
        </div>
        <div class="grid">
          <div>
            <label>Path</label>
            <input data-file-idx="${idx}" data-file-field="path" type="text" value="${fc.path || ""}" />
          </div>
          <div>
            <label>Endpoint Name</label>
            <input data-file-idx="${idx}" data-file-field="endpoint_name" type="text" value="${fc.endpoint_name || ""}" />
          </div>
          <div>
            <label>Format</label>
            <select data-file-idx="${idx}" data-file-field="format">
              ${["csv", "jsonl", "fixed_width", "parquet"].map((v) => `<option value="${v}" ${fc.format === v ? "selected" : ""}>${v}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Delimiter</label>
            <input data-file-idx="${idx}" data-file-field="delimiter" type="text" value="${fc.delimiter || ","}" maxlength="1" />
          </div>
          <div class="inline">
            <label>Has Header</label>
            <input data-file-idx="${idx}" data-file-field="has_header" type="checkbox" ${fc.has_header ? "checked" : ""} />
          </div>
          <div>
            <label>Encoding</label>
            <input data-file-idx="${idx}" data-file-field="encoding" type="text" value="${fc.encoding || "UTF-8"}" />
          </div>
          <div>
            <label>Primary Key Fields (CSV)</label>
            <input data-file-idx="${idx}" data-file-field="primary_key_fields" type="text" value="${(fc.primary_key_fields || []).join(", ")}" />
          </div>
          <div>
            <label>Watermark Column</label>
            <input data-file-idx="${idx}" data-file-field="watermark_column" type="text" value="${fc.watermark_column || ""}" />
          </div>
          <div>
            <label>Bronze Table Name</label>
            <input data-file-idx="${idx}" data-file-field="bronze_table_name" type="text" value="${fc.bronze_table_name || ""}" />
          </div>
          <div>
            <label>Batch Flush Rows</label>
            <input data-file-idx="${idx}" data-file-field="batch_flush_rows" type="number" value="${fc.batch_flush_rows}" />
          </div>
        </div>
        <div class="copybook-group ${fc.format === "fixed_width" ? "visible" : ""}" data-copybook-group="${idx}" style="margin-top:10px;">
          <label>COBOL Copybook</label>
          <textarea data-file-idx="${idx}" data-file-field="copybook" placeholder="05 NAME PIC X(20).&#10;05 AMOUNT PIC 9(8)V99.">${fc.copybook || ""}</textarea>
        </div>
      </div>
    `).join("");
    this.fileContainer.innerHTML = html;
    this.attachFileEvents();
  }

  payload() {
    return {
      id: this.state.id,
      source_system: this.state.source_system,
      connection_id: this.state.connection_id,
      base_path: this.state.base_path,
      transport: this.state.transport,
      file_configs: this.state.file_configs.map((fc) => ({
        ...fc,
        paths: fc.path ? [fc.path] : [],
      })),
    };
  }

  async save() {
    try {
      const response = await request("/saveFileSource", { method: "POST", body: this.payload() });
      const rects = window.data?.rectangles || window.data?.panelItems || [];
      const idx = rects.findIndex((item) => String(item.id) === String(response.id));
      if (idx >= 0) rects[idx] = { ...rects[idx], ...response };
      this.state = this.hydrate();
      this.savedState = JSON.stringify(this.state);
      this.statusText.textContent = "File source saved.";
      this.render();
    } catch (error) {
      this.statusText.textContent = error.message || "Save failed.";
    }
  }
}

customElements.define("file-source-component", FileSourceComponent);
