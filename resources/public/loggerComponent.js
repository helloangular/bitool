import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class LoggerComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.selectedRectangle = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() { this.loadTemplate(); }


  async loadTemplate() {
    try {
      const resp = await fetch("loggerComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("LoggerComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput       = sr.querySelector("#nodeName");
    this.logLevel       = sr.querySelector("#logLevel");
    this.fieldsList     = sr.querySelector("#fieldsList");
    this.destination    = sr.querySelector("#destination");
    this.externalUrlRow = sr.querySelector("#externalUrlRow");
    this.externalUrl    = sr.querySelector("#externalUrl");
    this.format         = sr.querySelector("#format");
    this.saveBtn        = sr.querySelector("#saveBtn");
    this.closeBtn       = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,     "click",  () => this.save(),                 false, "Logger");
    EventHandler.on(this.closeBtn,    "click",  () => this.close(),                false, "Logger");
    this.closeBtn.addEventListener("click", () => this.close());
    EventHandler.on(this.destination, "change", () => this.toggleExternalUrl(),    false, "Logger");
  }

  toggleExternalUrl() {
    this.externalUrlRow.style.display = this.destination.value === "external" ? "" : "none";
  }

  attributeChangedCallback(name, _old, newValue) {
    if (name === "visibility") {
      newValue === "open" ? this.open() : this.close();
    }
  }

  open() {
    this.style.display = "block";
    this.selectedRectangle = window.data?.rectangles?.find(r => r.id === window.data?.selectedRectangle);
    if (!this.selectedRectangle) return;
    const rect = this.selectedRectangle;
    this.nodeNameInput.value    = rect.name || "";
    this.logLevel.value    = rect.log_level || "INFO";
    this.destination.value = rect.destination || "console";
    this.externalUrl.value = rect.external_url || "";
    this.format.value      = rect.format || "json";
    this.toggleExternalUrl();
    this.renderFields(rect.items || [], rect.fields_to_log || []);
    this.state = new StateManager({ name: rect.name });
  }

  renderFields(items, selectedFields) {
    this.fieldsList.innerHTML = "";
    items.forEach(col => {
      const lbl = document.createElement("label");
      const cb  = document.createElement("input");
      cb.type    = "checkbox";
      cb.value   = col.column_name;
      cb.checked = selectedFields.length === 0 || selectedFields.includes(col.column_name);
      lbl.appendChild(cb);
      const span = document.createElement("span");
      span.textContent = " " + col.column_name;
      lbl.appendChild(span);
      this.fieldsList.appendChild(lbl);
    });
  }

  getSelectedFields() {
    return Array.from(this.fieldsList.querySelectorAll("input[type=checkbox]:checked")).map(cb => cb.value);
  }

  close() {
    this.style.display = "none";
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    request("/saveLg", {
      method: "POST",
      body: {
        id:             rect.id,
        name:           this.nodeNameInput.value,
        log_level:      this.logLevel.value,
        fields_to_log:  this.getSelectedFields(),
        destination:    this.destination.value,
        external_url:   this.externalUrl.value,
        format:         this.format.value
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    });
  }
}

customElements.define("logger-component", LoggerComponent);
