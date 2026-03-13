import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class ResponseBuilderComponent extends HTMLElement {
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
      const resp = await fetch("responseBuilderComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("ResponseBuilderComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.saveButton = sr.querySelector("#saveButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.statusCodeSelect = sr.querySelector("#statusCode");
    this.responseTypeSelect = sr.querySelector("#responseType");
    this.headersInput = sr.querySelector("#headers");
    this.addFieldBtn = sr.querySelector("#addFieldBtn");
    this.fieldMappingsBody = sr.querySelector("#fieldMappingsBody");
    this.availableVarsCard = sr.querySelector("#availableVarsCard");
    this.availableVarsList = sr.querySelector("#availableVarsList");
    this.sourceColumnOptions = sr.querySelector("#sourceColumnOptions");
  }

  attributeChangedCallback(name, oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") {
      this.open();
    } else {
      this.close();
    }
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
      status_code: rect.status_code || "200",
      response_type: rect.response_type || "json",
      headers: rect.headers || "",
      template: rect.template || [],
    });
  }

  populateFields() {
    if (!this.statusCodeSelect) return;
    const rect = this.selectedRectangle || {};

    this.statusCodeSelect.value = rect.status_code || "200";
    this.responseTypeSelect.value = rect.response_type || "json";
    this.headersInput.value = rect.headers || "";
    this.renderAvailableVariables(rect.items || []);
    this.renderFieldMappings(rect.template || []);
    this.saveButton.disabled = true;
  }

  setupEventListeners() {

    EventHandler.on(this.saveButton, "click", () => this.save(), false, "ResponseBuilder");
    EventHandler.on(this.closeButton, "click", () => {
      this.setAttribute("visibility", "close");
    }, false, "ResponseBuilder");

    EventHandler.on(this.statusCodeSelect, "change", () => {
      this.state.updateField("status_code", this.statusCodeSelect.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "ResponseBuilder");

    EventHandler.on(this.responseTypeSelect, "change", () => {
      this.state.updateField("response_type", this.responseTypeSelect.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "ResponseBuilder");

    EventHandler.on(this.headersInput, "input", () => {
      this.state.updateField("headers", this.headersInput.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "ResponseBuilder");

    EventHandler.on(this.addFieldBtn, "click", () => this.addFieldMapping(), false, "ResponseBuilder");
  }

  // --- Field Mappings ---

  getVariableEntries(items) {
    const byName = new Map();
    (items || []).forEach((item) => {
      const names = [item?.business_name, item?.technical_name];
      names.forEach((raw) => {
        const name = (raw || "").trim();
        if (!name || byName.has(name)) return;
        byName.set(name, { name, dataType: item?.data_type || "" });
      });
    });
    return Array.from(byName.values());
  }

  renderAvailableVariables(items) {
    if (!this.availableVarsCard || !this.availableVarsList || !this.sourceColumnOptions) return;
    const entries = this.getVariableEntries(items);

    this.availableVarsList.innerHTML = "";
    this.sourceColumnOptions.innerHTML = "";

    if (entries.length === 0) {
      this.availableVarsCard.style.display = "none";
      return;
    }

    entries.forEach((entry) => {
      const chip = document.createElement("span");
      chip.style.cssText = "padding:2px 8px;border:1px solid #d6e4ff;border-radius:12px;background:#fff;font-size:12px;font-family:monospace;";
      chip.textContent = entry.dataType ? `${entry.name} (${entry.dataType})` : entry.name;
      this.availableVarsList.appendChild(chip);

      const option = document.createElement("option");
      option.value = entry.name;
      this.sourceColumnOptions.appendChild(option);
    });

    this.availableVarsCard.style.display = "block";
  }

  addFieldMapping() {
    const current = [...(this.state.current.template || [])];
    current.push({ output_key: "", source_column: "" });
    this.state.updateField("template", current);
    this.renderFieldMappings(current);
    this.saveButton.disabled = !this.state.isDirty();
  }

  renderFieldMappings(mappings) {
    if (!this.fieldMappingsBody) return;
    this.fieldMappingsBody.innerHTML = "";
    (mappings || []).forEach((m, i) => {
      const tr = document.createElement("tr");
      tr.style.borderBottom = "1px solid #eee";

      const makeInput = (value, field, placeholder) => {
        const input = document.createElement("input");
        input.type = "text";
        input.value = value || "";
        input.dataset.idx = i;
        input.dataset.field = field;
        input.className = `rb-input rb-${field}`;
        input.placeholder = placeholder;
        input.style.cssText = "width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;";
        if (field === "source_column" && this.sourceColumnOptions) {
          input.setAttribute("list", "sourceColumnOptions");
        }
        return input;
      };

      const td1 = document.createElement("td");
      td1.appendChild(makeInput(m.output_key, "output_key", "e.g. userId"));

      const td2 = document.createElement("td");
      td2.appendChild(makeInput(m.source_column, "source_column", "e.g. id"));

      const removeBtn = document.createElement("button");
      removeBtn.dataset.idx = i;
      removeBtn.className = "remove-btn";
      removeBtn.style.cssText = "cursor:pointer;border:none;background:none;color:red;";
      removeBtn.textContent = "x";
      const td3 = document.createElement("td");
      td3.appendChild(removeBtn);

      tr.appendChild(td1);
      tr.appendChild(td2);
      tr.appendChild(td3);
      this.fieldMappingsBody.appendChild(tr);
    });

    this.fieldMappingsBody.querySelectorAll("input").forEach((el) => {
      el.addEventListener("input", () => {
        const idx = parseInt(el.dataset.idx);
        const field = el.dataset.field;
        const current = [...(this.state.current.template || [])];
        if (current[idx]) {
          current[idx] = { ...current[idx], [field]: el.value };
          this.state.updateField("template", current);
          this.saveButton.disabled = !this.state.isDirty();
        }
      });
    });

    this.fieldMappingsBody.querySelectorAll(".remove-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        const current = [...(this.state.current.template || [])];
        current.splice(idx, 1);
        this.state.updateField("template", current);
        this.renderFieldMappings(current);
        this.saveButton.disabled = !this.state.isDirty();
      });
    });
  }

  // --- Save ---

  async save() {
    if (!this.selectedRectangle) return;
    try {
      const values = {
        id: this.selectedRectangle.id,
        status_code: this.state.current.status_code,
        response_type: this.state.current.response_type,
        headers: this.state.current.headers,
        template: this.state.current.template,
      };
      const data = await request("/saveResponseBuilder", { method: "POST", body: values });
      const idx = (window.data?.rectangles || []).findIndex(
        (r) => String(r.id) === String(this.selectedRectangle.id)
      );
      if (idx !== -1) {
        window.data.rectangles[idx] = data;
      }
      this.selectedRectangle = data;
      this.state.commit();
      this.saveButton.disabled = true;
    } catch (err) {
      console.error("ResponseBuilderComponent: save failed", err);
    }
  }
}

customElements.define("response-builder-component", ResponseBuilderComponent);
