import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class CorsComponent extends HTMLElement {
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
      const resp = await fetch("corsComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("CorsComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput         = sr.querySelector("#nodeName");
    this.allowedOrigins   = sr.querySelector("#allowedOrigins");
    this.methodsGroup     = sr.querySelector("#methodsGroup");
    this.allowedHeaders   = sr.querySelector("#allowedHeaders");
    this.allowCredentials = sr.querySelector("#allowCredentials");
    this.maxAge           = sr.querySelector("#maxAge");
    this.saveBtn          = sr.querySelector("#saveBtn");
    this.closeBtn         = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,  "click", () => this.save(),  false, "Cors");
    EventHandler.on(this.closeBtn, "click", () => this.close(), false, "Cors");
    this.closeBtn.addEventListener("click", () => this.close());
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
    this.nodeNameInput.value       = rect.name || "";
    this.allowedOrigins.value = (rect.allowed_origins || []).join("\n");
    this.allowedHeaders.value = (rect.allowed_headers || []).join(", ");
    this.allowCredentials.checked = !!rect.allow_credentials;
    this.maxAge.value         = String(rect.max_age ?? 86400);
    const methods = rect.allowed_methods || ["GET", "POST", "OPTIONS"];
    this.methodsGroup.querySelectorAll("input[type=checkbox]").forEach(cb => {
      cb.checked = methods.includes(cb.value);
    });
    this.state = new StateManager({ name: rect.name });
  }

  close() {
    this.style.display = "none";
  }

  getMethods() {
    return Array.from(this.methodsGroup.querySelectorAll("input[type=checkbox]:checked")).map(cb => cb.value);
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    request("/saveCr", {
      method: "POST",
      body: {
        id:                rect.id,
        name:              this.nodeNameInput.value,
        allowed_origins:   this.allowedOrigins.value.split("\n").map(s => s.trim()).filter(Boolean),
        allowed_methods:   this.getMethods(),
        allowed_headers:   this.allowedHeaders.value.split(",").map(s => s.trim()).filter(Boolean),
        allow_credentials: this.allowCredentials.checked,
        max_age:           this.maxAge.value
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    });
  }
}

customElements.define("cors-component", CorsComponent);
