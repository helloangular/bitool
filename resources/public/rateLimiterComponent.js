import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class RateLimiterComponent extends HTMLElement {
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
      const resp = await fetch("rateLimiterComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("RateLimiterComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput        = sr.querySelector("#nodeName");
    this.maxRequests     = sr.querySelector("#maxRequests");
    this.windowSeconds   = sr.querySelector("#windowSeconds");
    this.burst           = sr.querySelector("#burst");
    this.keyType         = sr.querySelector("#keyType");
    this.customHeaderRow = sr.querySelector("#customHeaderRow");
    this.customHeader    = sr.querySelector("#customHeader");
    this.saveBtn         = sr.querySelector("#saveBtn");
    this.closeBtn        = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,  "click",  () => this.save(),  false, "RateLimiter");
    EventHandler.on(this.closeBtn, "click",  () => this.close(), false, "RateLimiter");
    this.closeBtn.addEventListener("click", () => this.close());
    EventHandler.on(this.keyType,  "change", () => this.toggleCustomHeader(), false, "RateLimiter");
  }

  toggleCustomHeader() {
    this.customHeaderRow.style.display = this.keyType.value === "custom" ? "" : "none";
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
    this.nodeNameInput.value      = rect.name || "";
    this.maxRequests.value   = String(rect.max_requests ?? 100);
    this.windowSeconds.value = String(rect.window_seconds ?? 60);
    this.burst.value         = String(rect.burst ?? 0);
    this.keyType.value       = rect.key_type || "ip";
    this.toggleCustomHeader();
    this.state = new StateManager({ name: rect.name, max_requests: rect.max_requests, window_seconds: rect.window_seconds });
  }

  close() {
    this.style.display = "none";
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    const keyType = this.keyType.value;
    const resolvedKey = keyType === "custom"
      ? `header:${this.customHeader.value}`
      : keyType;
    request("/saveRl", {
      method: "POST",
      body: {
        id:             rect.id,
        name:           this.nodeNameInput.value,
        max_requests:   this.maxRequests.value,
        window_seconds: this.windowSeconds.value,
        burst:          this.burst.value,
        key_type:       resolvedKey
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    });
  }
}

customElements.define("rate-limiter-component", RateLimiterComponent);
