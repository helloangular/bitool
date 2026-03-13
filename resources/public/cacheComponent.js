import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class CacheComponent extends HTMLElement {
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
      const resp = await fetch("cacheComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("CacheComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput   = sr.querySelector("#nodeName");
    this.cacheKey   = sr.querySelector("#cacheKey");
    this.colsHint   = sr.querySelector("#colsHint");
    this.ttlSeconds = sr.querySelector("#ttlSeconds");
    this.strategy   = sr.querySelector("#strategy");
    this.saveBtn    = sr.querySelector("#saveBtn");
    this.closeBtn   = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,  "click", () => this.save(),  false, "Cache");
    EventHandler.on(this.closeBtn, "click", () => this.close(), false, "Cache");
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
    this.nodeNameInput.value   = rect.name || "";
    this.cacheKey.value   = rect.cache_key || "";
    this.ttlSeconds.value = String(rect.ttl_seconds ?? 300);
    this.strategy.value   = rect.strategy || "read-through";
    this.renderColsHint(rect.items || []);
    this.state = new StateManager({ name: rect.name, cache_key: rect.cache_key });
  }

  renderColsHint(items) {
    this.colsHint.innerHTML = "";
    items.forEach(col => {
      const chip = document.createElement("span");
      chip.className = "col-chip";
      chip.textContent = col.column_name;
      chip.style.cssText = "background:#eef;border:1px solid #ccf;border-radius:3px;padding:1px 6px;cursor:pointer;font-size:11px;";
      EventHandler.on(chip, "click", () => {
        this.cacheKey.value += `{{${col.column_name}}}`;
      }, false, "Cache");
      this.colsHint.appendChild(chip);
    });
  }

  close() {
    this.style.display = "none";
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    request("/saveCq", {
      method: "POST",
      body: {
        id:          rect.id,
        name:        this.nodeNameInput.value,
        cache_key:   this.cacheKey.value,
        ttl_seconds: this.ttlSeconds.value,
        strategy:    this.strategy.value
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    });
  }
}

customElements.define("cache-component", CacheComponent);
