import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class CircuitBreakerComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.selectedRectangle = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() { this.loadTemplate(); }

  disconnectedCallback() { EventHandler.removeGroup("CircuitBreaker"); }

  async loadTemplate() {
    try {
      const resp = await fetch("circuitBreakerComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("CircuitBreakerComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput          = sr.querySelector("#nodeName");
    this.failureThreshold  = sr.querySelector("#failureThreshold");
    this.resetTimeout      = sr.querySelector("#resetTimeout");
    this.fallbackResponse  = sr.querySelector("#fallbackResponse");
    this.saveBtn           = sr.querySelector("#saveBtn");
    this.closeBtn          = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,  "click", () => this.save(),  false, "CircuitBreaker");
    EventHandler.on(this.closeBtn, "click", () => this.close(), false, "CircuitBreaker");
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
    this.nodeNameInput.value         = rect.name || "";
    this.failureThreshold.value = String(rect.failure_threshold ?? 5);
    this.resetTimeout.value     = String(rect.reset_timeout ?? 30);
    this.fallbackResponse.value = rect.fallback_response || '{"error": "service unavailable"}';
    this.state = new StateManager({ name: rect.name });
  }

  close() {
    this.style.display = "none";
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    request("/saveCi", {
      method: "POST",
      body: {
        id:                rect.id,
        name:              this.nodeNameInput.value,
        failure_threshold: this.failureThreshold.value,
        reset_timeout:     this.resetTimeout.value,
        fallback_response: this.fallbackResponse.value
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    });
  }
}

customElements.define("circuit-breaker-component", CircuitBreakerComponent);
