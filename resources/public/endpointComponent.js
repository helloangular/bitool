import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class EndpointComponent extends HTMLElement {
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
    EventHandler.removeGroup("Endpoint");
  }

  async loadTemplate() {
    try {
      const resp = await fetch("endpointComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
      this.close();
    } catch (err) {
      console.error("EndpointComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.saveButton = sr.querySelector("#saveButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.httpMethodSelect = sr.querySelector("#httpMethod");
    this.routePathInput = sr.querySelector("#routePath");
    this.descriptionInput = sr.querySelector("#description");
    this.responseFormatSelect = sr.querySelector("#responseFormat");
    this.parsePathBtn = sr.querySelector("#parsePathBtn");
    this.addQueryParamBtn = sr.querySelector("#addQueryParamBtn");
    this.addBodyFieldBtn = sr.querySelector("#addBodyFieldBtn");
    this.pathParamsBody = sr.querySelector("#pathParamsBody");
    this.queryParamsBody = sr.querySelector("#queryParamsBody");
    this.bodySchemaBody = sr.querySelector("#bodySchemaBody");
    this.bodySchemaSection = sr.querySelector("#bodySchemaSection");
    this.testBtn = sr.querySelector("#testBtn");
    this.testResult = sr.querySelector("#testResult");
    this.testParamsContainer = sr.querySelector("#testParamsContainer");
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
    EventHandler.removeGroup("Endpoint");
  }

  updateSelectedRectangle() {
    const id = window.data?.selectedRectangle;
    this.selectedRectangle = (window.data?.rectangles || []).find(
      (r) => String(r.id) === String(id)
    );
  }

  normalizeHttpMethod(method) {
    const allowed = ["GET", "POST", "PUT", "DELETE", "PATCH"];
    const normalized = String(method || "").toUpperCase();
    return allowed.includes(normalized) ? normalized : "GET";
  }

  normalizePathParams(params) {
    return (params || []).map((p) => ({
      param_name: p?.param_name || p?.name || "",
      data_type: p?.data_type || p?.type || "varchar",
      description: p?.description || "",
    }));
  }

  normalizeQueryParams(params) {
    return (params || []).map((p) => ({
      param_name: p?.param_name || p?.name || "",
      data_type: p?.data_type || p?.type || "varchar",
      required: p?.required === true || p?.required === "true",
      default_value: p?.default_value ?? p?.default ?? "",
    }));
  }

  normalizeBodySchema(fields) {
    return (fields || []).map((f) => ({
      field_name: f?.field_name || f?.name || "",
      data_type: f?.data_type || f?.type || "varchar",
      required: f?.required === true || f?.required === "true",
      description: f?.description || "",
    }));
  }

  normalizeRect(rect) {
    const responseFormat = String(rect?.response_format || "json").toLowerCase();
    const allowedFormats = new Set(["json", "csv", "edn"]);
    return {
      http_method: this.normalizeHttpMethod(rect?.http_method),
      route_path: rect?.route_path || "",
      path_params: this.normalizePathParams(rect?.path_params),
      query_params: this.normalizeQueryParams(rect?.query_params),
      body_schema: this.normalizeBodySchema(rect?.body_schema),
      response_format: allowedFormats.has(responseFormat) ? responseFormat : "json",
      description: rect?.description || "",
    };
  }

  initializeState() {
    const rect = this.normalizeRect(this.selectedRectangle || {});
    this.state = new StateManager({
      http_method: rect.http_method,
      route_path: rect.route_path,
      path_params: rect.path_params,
      query_params: rect.query_params,
      body_schema: rect.body_schema,
      response_format: rect.response_format,
      description: rect.description,
    });
  }

  populateFields() {
    if (!this.httpMethodSelect) return;
    const rect = this.normalizeRect(this.selectedRectangle || {});

    this.httpMethodSelect.value = rect.http_method;
    this.routePathInput.value = rect.route_path;
    this.descriptionInput.value = rect.description;
    this.responseFormatSelect.value = rect.response_format;

    this.toggleBodySchema();
    this.renderPathParams(rect.path_params);
    this.renderQueryParams(rect.query_params);
    this.renderBodySchema(rect.body_schema);
    this.saveButton.disabled = true;
  }

  setupEventListeners() {
    EventHandler.removeGroup("Endpoint");

    EventHandler.on(this.saveButton, "click", () => this.save(), false, "Endpoint");
    EventHandler.on(this.closeButton, "click", () => {
      this.setAttribute("visibility", "close");
    }, false, "Endpoint");

    EventHandler.on(this.httpMethodSelect, "change", () => {
      this.state.updateField("http_method", this.httpMethodSelect.value);
      this.toggleBodySchema();
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Endpoint");

    EventHandler.on(this.routePathInput, "input", () => {
      this.state.updateField("route_path", this.routePathInput.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Endpoint");

    EventHandler.on(this.descriptionInput, "input", () => {
      this.state.updateField("description", this.descriptionInput.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Endpoint");

    EventHandler.on(this.responseFormatSelect, "change", () => {
      this.state.updateField("response_format", this.responseFormatSelect.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Endpoint");

    EventHandler.on(this.parsePathBtn, "click", () => this.parsePathParams(), false, "Endpoint");
    EventHandler.on(this.addQueryParamBtn, "click", () => this.addQueryParam(), false, "Endpoint");
    EventHandler.on(this.addBodyFieldBtn, "click", () => this.addBodyField(), false, "Endpoint");
    EventHandler.on(this.testBtn, "click", () => this.testEndpoint(), false, "Endpoint");
  }

  toggleBodySchema() {
    const method = this.httpMethodSelect?.value || "GET";
    const show = ["POST", "PUT", "PATCH"].includes(method);
    if (this.bodySchemaSection) {
      this.bodySchemaSection.style.display = show ? "block" : "none";
    }
  }

  // --- Path Parameters ---

  parsePathParams() {
    const route = this.routePathInput.value || "";
    const matches = route.match(/:(\w+)/g) || [];
    const params = matches.map((m) => ({
      param_name: m.substring(1),
      data_type: "varchar",
      description: "",
    }));
    this.state.updateField("path_params", params);
    this.renderPathParams(params);
    this.saveButton.disabled = !this.state.isDirty();
  }

  renderPathParams(params) {
    if (!this.pathParamsBody) return;
    this.pathParamsBody.innerHTML = "";
    (params || []).forEach((p, i) => {
      const tr = document.createElement("tr");
      tr.style.borderBottom = "1px solid #eee";
      tr.innerHTML = `
        <td><input type="text" value="${p.param_name || ""}" data-idx="${i}" data-field="param_name"
          style="width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;" /></td>
        <td><select data-idx="${i}" data-field="data_type" style="padding:3px;border:1px solid #ddd;border-radius:3px;">
          <option value="varchar" ${p.data_type === "varchar" ? "selected" : ""}>varchar</option>
          <option value="integer" ${p.data_type === "integer" ? "selected" : ""}>integer</option>
          <option value="uuid" ${p.data_type === "uuid" ? "selected" : ""}>uuid</option>
        </select></td>
        <td><input type="text" value="${p.description || ""}" data-idx="${i}" data-field="description"
          style="width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;" /></td>
        <td><button data-idx="${i}" class="remove-btn" style="cursor:pointer;border:none;background:none;color:red;">x</button></td>
      `;
      this.pathParamsBody.appendChild(tr);
    });

    this.pathParamsBody.querySelectorAll("input, select").forEach((el) => {
      el.addEventListener("change", () => {
        const idx = parseInt(el.dataset.idx);
        const field = el.dataset.field;
        const current = [...(this.state.current.path_params || [])];
        if (current[idx]) {
          current[idx] = { ...current[idx], [field]: el.value };
          this.state.updateField("path_params", current);
          this.saveButton.disabled = !this.state.isDirty();
        }
      });
    });

    this.pathParamsBody.querySelectorAll(".remove-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        const current = [...(this.state.current.path_params || [])];
        current.splice(idx, 1);
        this.state.updateField("path_params", current);
        this.renderPathParams(current);
        this.saveButton.disabled = !this.state.isDirty();
      });
    });
  }

  // --- Query Parameters ---

  addQueryParam() {
    const current = [...(this.state.current.query_params || [])];
    current.push({ param_name: "", data_type: "varchar", required: false, default_value: "" });
    this.state.updateField("query_params", current);
    this.renderQueryParams(current);
    this.saveButton.disabled = !this.state.isDirty();
  }

  renderQueryParams(params) {
    if (!this.queryParamsBody) return;
    this.queryParamsBody.innerHTML = "";
    (params || []).forEach((p, i) => {
      const tr = document.createElement("tr");
      tr.style.borderBottom = "1px solid #eee";
      tr.innerHTML = `
        <td><input type="text" value="${p.param_name || ""}" data-idx="${i}" data-field="param_name"
          style="width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;" /></td>
        <td><select data-idx="${i}" data-field="data_type" style="padding:3px;border:1px solid #ddd;border-radius:3px;">
          <option value="varchar" ${p.data_type === "varchar" ? "selected" : ""}>varchar</option>
          <option value="integer" ${p.data_type === "integer" ? "selected" : ""}>integer</option>
          <option value="boolean" ${p.data_type === "boolean" ? "selected" : ""}>boolean</option>
        </select></td>
        <td><input type="checkbox" data-idx="${i}" data-field="required" ${p.required ? "checked" : ""} /></td>
        <td><input type="text" value="${p.default_value || ""}" data-idx="${i}" data-field="default_value"
          style="width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;" /></td>
        <td><button data-idx="${i}" class="remove-btn" style="cursor:pointer;border:none;background:none;color:red;">x</button></td>
      `;
      this.queryParamsBody.appendChild(tr);
    });

    this.queryParamsBody.querySelectorAll("input, select").forEach((el) => {
      el.addEventListener("change", () => {
        const idx = parseInt(el.dataset.idx);
        const field = el.dataset.field;
        const current = [...(this.state.current.query_params || [])];
        if (current[idx]) {
          const val = field === "required" ? el.checked : el.value;
          current[idx] = { ...current[idx], [field]: val };
          this.state.updateField("query_params", current);
          this.saveButton.disabled = !this.state.isDirty();
        }
      });
    });

    this.queryParamsBody.querySelectorAll(".remove-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        const current = [...(this.state.current.query_params || [])];
        current.splice(idx, 1);
        this.state.updateField("query_params", current);
        this.renderQueryParams(current);
        this.saveButton.disabled = !this.state.isDirty();
      });
    });
  }

  // --- Body Schema ---

  addBodyField() {
    const current = [...(this.state.current.body_schema || [])];
    current.push({ field_name: "", data_type: "varchar", required: false, description: "" });
    this.state.updateField("body_schema", current);
    this.renderBodySchema(current);
    this.saveButton.disabled = !this.state.isDirty();
  }

  renderBodySchema(fields) {
    if (!this.bodySchemaBody) return;
    this.bodySchemaBody.innerHTML = "";
    (fields || []).forEach((f, i) => {
      const tr = document.createElement("tr");
      tr.style.borderBottom = "1px solid #eee";
      tr.innerHTML = `
        <td><input type="text" value="${f.field_name || ""}" data-idx="${i}" data-field="field_name"
          style="width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;" /></td>
        <td><select data-idx="${i}" data-field="data_type" style="padding:3px;border:1px solid #ddd;border-radius:3px;">
          <option value="varchar" ${f.data_type === "varchar" ? "selected" : ""}>varchar</option>
          <option value="integer" ${f.data_type === "integer" ? "selected" : ""}>integer</option>
          <option value="boolean" ${f.data_type === "boolean" ? "selected" : ""}>boolean</option>
          <option value="text" ${f.data_type === "text" ? "selected" : ""}>text</option>
          <option value="json" ${f.data_type === "json" ? "selected" : ""}>json</option>
        </select></td>
        <td><input type="checkbox" data-idx="${i}" data-field="required" ${f.required ? "checked" : ""} /></td>
        <td><input type="text" value="${f.description || ""}" data-idx="${i}" data-field="description"
          style="width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;" /></td>
        <td><button data-idx="${i}" class="remove-btn" style="cursor:pointer;border:none;background:none;color:red;">x</button></td>
      `;
      this.bodySchemaBody.appendChild(tr);
    });

    this.bodySchemaBody.querySelectorAll("input, select").forEach((el) => {
      el.addEventListener("change", () => {
        const idx = parseInt(el.dataset.idx);
        const field = el.dataset.field;
        const current = [...(this.state.current.body_schema || [])];
        if (current[idx]) {
          const val = field === "required" ? el.checked : el.value;
          current[idx] = { ...current[idx], [field]: val };
          this.state.updateField("body_schema", current);
          this.saveButton.disabled = !this.state.isDirty();
        }
      });
    });

    this.bodySchemaBody.querySelectorAll(".remove-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        const current = [...(this.state.current.body_schema || [])];
        current.splice(idx, 1);
        this.state.updateField("body_schema", current);
        this.renderBodySchema(current);
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
        http_method: this.state.current.http_method,
        route_path: this.state.current.route_path,
        path_params: this.state.current.path_params,
        query_params: this.state.current.query_params,
        body_schema: this.state.current.body_schema,
        response_format: this.state.current.response_format,
        description: this.state.current.description,
      };
      const data = await request("/saveEndpoint", { method: "POST", body: values });
      // Update local rectangle data
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
      console.error("EndpointComponent: save failed", err);
    }
  }

  // --- Test ---

  // Convert schema array to {key: ""} runtime map.
  // Path/query rows use param_name; body rows use field_name.
  _schemaToParams(schema, keyField) {
    return Object.fromEntries(
      (schema || []).map(p => [p[keyField] || "", ""]).filter(([k]) => k)
    );
  }

  async testEndpoint() {
    const method = this.state.current.http_method || "GET";
    const route = this.state.current.route_path || "/";
    this.testResult.style.display = "block";
    this.testResult.textContent = `Testing ${method} ${route} ...`;
    try {
      const data = await request("/testEndpoint", {
        method: "POST",
        body: {
          id: this.selectedRectangle?.id,
          test_path_params:  this._schemaToParams(this.state.current.path_params,  "param_name"),
          test_query_params: this._schemaToParams(this.state.current.query_params, "param_name"),
          test_body:         this._schemaToParams(this.state.current.body_schema,  "field_name"),
        },
      });
      this.testResult.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
      this.testResult.textContent = `Error: ${err.message}`;
    }
  }
}

customElements.define("endpoint-component", EndpointComponent);
