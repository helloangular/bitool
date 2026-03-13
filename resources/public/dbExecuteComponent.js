import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class DbExecuteComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.selectedRectangle = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() { this.loadTemplate(); }

  disconnectedCallback() { EventHandler.removeGroup("DbExecute"); }

  async loadTemplate() {
    try {
      const resp = await fetch("dbExecuteComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("DbExecuteComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput      = sr.querySelector("#nodeName");
    this.connectionId  = sr.querySelector("#connectionId");
    this.operation     = sr.querySelector("#operation");
    this.sqlTemplate   = sr.querySelector("#sqlTemplate");
    this.resultMode    = sr.querySelector("#resultMode");
    this.resultModeRow = sr.querySelector("#resultModeRow");
    this.colsPreview   = sr.querySelector("#colsPreview");
    this.saveBtn       = sr.querySelector("#saveBtn");
    this.closeBtn      = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,    "click", () => this.save(),           false, "DbExecute");
    EventHandler.on(this.closeBtn,   "click", () => this.close(),          false, "DbExecute");
    this.closeBtn.addEventListener("click", () => this.close());
    EventHandler.on(this.operation,  "change", () => this.updateResultModeVisibility(), false, "DbExecute");
    EventHandler.on(this.sqlTemplate, "blur",  () => this.previewColumns(), false, "DbExecute");
  }

  async loadConnections() {
    try {
      const conns = await request("/getConn");
      this.connectionId.innerHTML = "";
      (conns || []).forEach(c => {
        const opt = document.createElement("option");
        opt.value = c.id;
        opt.textContent = c.name;
        this.connectionId.appendChild(opt);
      });
    } catch (_) {}
  }

  updateResultModeVisibility() {
    this.resultModeRow.style.display = this.operation.value === "SELECT" ? "" : "none";
  }

  previewColumns() {
    const sql = this.sqlTemplate.value;
    const op  = this.operation.value;
    if (op !== "SELECT") {
      this.colsPreview.textContent = "affected_rows (integer)";
      return;
    }
    const m = sql.replace(/\/\*.*?\*\//gs, "").match(/^\s*select\s+([\s\S]+?)\s+from\s+/i);
    if (!m) { this.colsPreview.textContent = "— could not parse —"; return; }
    const cols = m[1].split(",").map(c => c.trim()).filter(Boolean);
    this.colsPreview.textContent = cols.join(", ") || "— no columns —";
  }

  attributeChangedCallback(name, _old, newValue) {
    if (name === "visibility") {
      newValue === "open" ? this.open() : this.close();
    }
  }

  open() {
    this.style.display = "block";
    this.loadConnections();
    this.selectedRectangle = window.data?.rectangles?.find(r => r.id === window.data?.selectedRectangle);
    if (!this.selectedRectangle) return;
    const rect = this.selectedRectangle;
    this.nodeNameInput.value     = rect.name || "";
    this.operation.value    = rect.operation || "SELECT";
    this.sqlTemplate.value  = rect.sql_template || "";
    this.resultMode.value   = rect.result_mode || "single";
    if (rect.connection_id) this.connectionId.value = rect.connection_id;
    this.updateResultModeVisibility();
    this.previewColumns();
    this.state = new StateManager({ name: rect.name, sql_template: rect.sql_template, operation: rect.operation });
  }

  close() {
    this.style.display = "none";
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    request("/saveDx", {
      method: "POST",
      body: {
        id:            rect.id,
        name:          this.nodeNameInput.value,
        connection_id: this.connectionId.value,
        operation:     this.operation.value,
        sql_template:  this.sqlTemplate.value,
        result_mode:   this.resultMode.value
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    }).catch(err => {
      alert("Save failed: " + (err.message || err));
    });
  }
}

customElements.define("db-execute-component", DbExecuteComponent);
