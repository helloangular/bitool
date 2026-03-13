import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class WebhookComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.selectedRectangle = null;
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  connectedCallback() {
    this.loadTemplate();
  }

  disconnectedCallback() {
  }

  async loadTemplate() {
    try {
      const resp = await fetch("webhookComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("WebhookComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.saveButton = sr.querySelector("#saveButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.webhookPathInput = sr.querySelector("#webhookPath");
    this.payloadFormatSelect = sr.querySelector("#payloadFormat");
    this.secretHeaderInput = sr.querySelector("#secretHeader");
    this.secretValueInput = sr.querySelector("#secretValue");
    this.registeredUrlDiv = sr.querySelector("#registeredUrl");
  }

  attributeChangedCallback(name, oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") this.open();
    else this.close();
  }

  open() {
    this.style.display = "block";
    this.updateSelectedRectangle();
    this.initializeState();
    this.populateFields();
    this.setupEventListeners();
  }

  close() {
    this.style.display = "none";
  }

  updateSelectedRectangle() {
    const id = window.data?.selectedRectangle;
    this.selectedRectangle = (window.data?.rectangles || []).find(
      (r) => String(r.id) === String(id)
    );
  }

  initializeState() {
    const rect = this.selectedRectangle || {};
    this.state = new StateManager({
      webhook_path:   rect.webhook_path || "",
      payload_format: rect.payload_format || "json",
      secret_header:  rect.secret_header || "",
      // secret_value is never returned from server; always starts blank in UI
      secret_value:   "",
    });
  }

  populateFields() {
    if (!this.webhookPathInput) return;
    const rect = this.selectedRectangle || {};
    this.webhookPathInput.value = rect.webhook_path || "";
    this.payloadFormatSelect.value = rect.payload_format || "json";
    this.secretHeaderInput.value = rect.secret_header || "";
    // Never pre-fill secret — show placeholder hint if one is stored server-side
    this.secretValueInput.value = "";
    this.secretValueInput.placeholder = rect.secret_set
      ? "Secret stored — enter new value to replace"
      : "HMAC secret";
    this.updateRegisteredUrl(rect.webhook_path || "");
    this.saveButton.disabled = true;
  }

  setupEventListeners() {
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "Webhook");
    EventHandler.on(this.closeButton, "click", () => {
      this.setAttribute("visibility", "close");
    }, false, "Webhook");

    EventHandler.on(this.webhookPathInput, "input", () => {
      this.state.updateField("webhook_path", this.webhookPathInput.value);
      this.updateRegisteredUrl(this.webhookPathInput.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Webhook");

    EventHandler.on(this.payloadFormatSelect, "change", () => {
      this.state.updateField("payload_format", this.payloadFormatSelect.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Webhook");

    EventHandler.on(this.secretHeaderInput, "input", () => {
      this.state.updateField("secret_header", this.secretHeaderInput.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Webhook");

    EventHandler.on(this.secretValueInput, "input", () => {
      this.state.updateField("secret_value", this.secretValueInput.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Webhook");
  }

  updateRegisteredUrl(path) {
    if (!this.registeredUrlDiv) return;
    if (!path) {
      this.registeredUrlDiv.style.display = "none";
      return;
    }
    const gid = window.data?.gid || "<graph-id>";
    this.registeredUrlDiv.textContent = `/api/v1/ep/${gid}${path}`;
    this.registeredUrlDiv.style.display = "block";
  }

  async save() {
    if (!this.selectedRectangle) return;
    try {
      const values = {
        id:             this.selectedRectangle.id,
        webhook_path:   this.state.current.webhook_path,
        payload_format: this.state.current.payload_format,
        secret_header:  this.state.current.secret_header,
        secret_value:   this.state.current.secret_value,
      };
      const data = await request("/saveWh", { method: "POST", body: values });
      const idx = (window.data?.rectangles || []).findIndex(
        (r) => String(r.id) === String(this.selectedRectangle.id)
      );
      if (idx !== -1) window.data.rectangles[idx] = data;
      this.selectedRectangle = data;
      this.state.commit();
      this.saveButton.disabled = true;
    } catch (err) {
      console.error("WebhookComponent: save failed", err);
    }
  }
}

customElements.define("webhook-component", WebhookComponent);
