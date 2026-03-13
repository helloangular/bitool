import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class EventEmitterComponent extends HTMLElement {
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
      const resp = await fetch("eventEmitterComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("EventEmitterComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput    = sr.querySelector("#nodeName");
    this.brokerUrl   = sr.querySelector("#brokerUrl");
    this.topic       = sr.querySelector("#topic");
    this.keyTemplate = sr.querySelector("#keyTemplate");
    this.colsHint    = sr.querySelector("#colsHint");
    this.format      = sr.querySelector("#format");
    this.saveBtn     = sr.querySelector("#saveBtn");
    this.closeBtn    = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,  "click", () => this.save(),  false, "EventEmitter");
    EventHandler.on(this.closeBtn, "click", () => this.close(), false, "EventEmitter");
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
    this.nodeNameInput.value    = rect.name || "";
    this.brokerUrl.value   = rect.broker_url || "";
    this.topic.value       = rect.topic || "";
    this.keyTemplate.value = rect.key_template || "";
    this.format.value      = rect.format || "json";
    this.renderColsHint(rect.items || []);
    this.state = new StateManager({ name: rect.name, topic: rect.topic });
  }

  renderColsHint(items) {
    this.colsHint.innerHTML = "";
    items.forEach(col => {
      const chip = document.createElement("span");
      chip.textContent = col.column_name;
      chip.style.cssText = "background:#eef;border:1px solid #ccf;border-radius:3px;padding:1px 6px;cursor:pointer;font-size:11px;margin-right:4px;";
      EventHandler.on(chip, "click", () => {
        this.keyTemplate.value += `{{${col.column_name}}}`;
      }, false, "EventEmitter");
      this.colsHint.appendChild(chip);
    });
  }

  close() {
    this.style.display = "none";
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    request("/saveEv", {
      method: "POST",
      body: {
        id:           rect.id,
        name:         this.nodeNameInput.value,
        broker_url:   this.brokerUrl.value,
        topic:        this.topic.value,
        key_template: this.keyTemplate.value,
        format:       this.format.value
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    });
  }
}

customElements.define("event-emitter-component", EventEmitterComponent);
