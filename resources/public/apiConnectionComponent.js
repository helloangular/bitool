import EventHandler from "./library/eventHandler.js";
import { createElement, customConfirm, getPanelItems, populateTable, request } from "./library/utils.js";
import StateManager from "./library/state-manager.js";

class ApiConnectionComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });

    // DOM refs (populated in connectedCallback)
    this.closeButton = null;
    this.saveButton = null;
    this.apiName = null;
    this.endpointUrl = null;
    this.submitBtn = null;
    this.tokenGroup = null;
    this.authSelect = null;
    this.authFields = null;
    this.endpointBody = null;

    // state
    this.state = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() {
    this.loadTemplate();
  }

  async loadTemplate() {
    try {
      const response = await fetch("apiConnectionComponent.html");
      if (!response.ok) throw new Error(`Failed to load template: ${response.status}`);
      const templateHTML = await response.text();
      const template = document.createElement("template");
      template.innerHTML = templateHTML;
      this.shadowRoot.appendChild(template.content.cloneNode(true));

      // bind DOM after template is attached
      this.closeButton = this.shadowRoot.querySelector("#closeButton");
      this.saveButton = this.shadowRoot.querySelector("#saveButton");
      this.apiName = this.shadowRoot.getElementById("apiName");
      this.endpointUrl = this.shadowRoot.getElementById("endpointUrl");
      this.submitBtn = this.shadowRoot.getElementById("submitBtn");
      this.tokenGroup = this.shadowRoot.getElementById("tokenGroup");
      this.authSelect = this.shadowRoot.getElementById("authSelect");
      this.authFields = this.shadowRoot.querySelectorAll(".auth-fields");
      this.endpointBody = this.shadowRoot.getElementById("endpointsBody");
      this.getEndpointBtn = this.shadowRoot.getElementById("getEndpoints");
      this.endpointSearch = this.shadowRoot.getElementById("endpointSearch");
      this.fetchMethodDropdown = this.shadowRoot.getElementById("fetchMethod");
      this.customRateLimit = this.shadowRoot.getElementById("customRateLimit");
      this.addParamBtn = this.shadowRoot.getElementById("addParamBtn");
      this.expressionBox = this.shadowRoot.getElementById("expressionBox");

      this.close();
    } catch (error) {
      console.error("Error loading API connection template:", error);
    }
  }

  disconnectedCallback() { EventHandler.removeGroup("API"); }

  attributeChangedCallback(name, oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") {
      this.open();
      this.resetFields();
      this.initializeState();
      this.setupEventListeners();
    } else {
      this.close();
      if (this.style.visibility === "hidden") EventHandler.removeGroup("API");
    }
  }

  setupEventListeners() {
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "API");
    // buttons & inputs
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "API");
    EventHandler.on(this.getEndpointBtn, "click", () => this.handleGetEndpointBtnClick(), false, "API");
    EventHandler.on(this.endpointSearch, "input", () => this.searchEndpoints(), false, "API");
    EventHandler.on(this.fetchMethodDropdown, "change", () => this.filterEndpointsByMethod(), false, "API");
    
    // select & checkbox
    EventHandler.on(this.authSelect, 'change', (e) => this.handleAuthSelectChange(e), false, "API");
    EventHandler.on(this.customRateLimit, 'change', (e) => this.handleRateLimitCheckboxChange(e.target.checked), false, "API");

    // track inputs (all inputs/selects inside shadow root)
    this.trackInputs();

    // delegate endpoint table interactions (row checkbox/select/table_name inputs)
    EventHandler.on(this.endpointBody, "input", (e) => this.onEndpointBodyInput(e), false, "API");
    EventHandler.on(this.endpointBody, "click", (e) => this.onEndpointBodyClick(e), false, "API");
  }

  /** Initialize StateManager with default fields */
  initializeState() {
    this.state = new StateManager({
      api_name: "",
      specification_url: "",
      endpoint_url: [],
      authentication: "",
      username: "",
      password: "",
      bearer_token: "",
      api_key: "",
      key_name: "",
      client_id: "",
      client_secret: "",
      auth_url: "",
      token_url: "",
      redirect_url: "",
      scope: "",
      header_name: "",
      header_value: "",
      access_key: "",
      secret_key: "",
      algorithm: "",
      custom_rate_limit: false
    });
    this.saveButton.disabled = true;
  }

  open() {
    this.style.visibility = "visible";
    this.reset();
  }

  reset() { if (this.saveButton) this.saveButton.disabled = true; }

  async close() {
    if (this.state && this.state.isDirty()) {
      const wantToSave = await customConfirm("Looks like you’ve made changes. Would you like to save them before leaving this panel?");
      if (!wantToSave) this.style.visibility = "hidden";
      return;
    }
    this.style.visibility = "hidden";
  }

  /** Save current values (validates required fields) */
  async save() {
    const values = this.getValues();
    const validation = this.validateNotEmpty(values);
    if (!validation.valid) return alert(`This field should not be empty: ${validation.emptyFields.join(", ")}`);

    try {
      this.dispatchEvent(new CustomEvent("api-connection-saved", {
        detail: values,
        bubbles: true,
        composed: true
      }));
      this.state.commit();
      this.reset();
      this.setAttribute('visibility', 'close');
    } catch (err) {
      console.error(err);
      alert("Save failed.");
    }
  }

  validateNotEmpty(data) {
    const empty = [];
    for (const k in data) {
      const v = data[k];
      if (v === null || v === undefined || v === "") empty.push(k);
    }
    return { valid: empty.length === 0, emptyFields: empty };
  }

  /** Track all inputs/selects and update state via StateManager */
  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("input, select");
    inputs.forEach((el) => {
      const name = el.name || el.id;
      if (!name) return;      

      if (el.tagName === "INPUT") {
        EventHandler.on(el, "input", (e) => {
          const val = e.target.type === "checkbox" ? e.target.checked : e.target.value;
          this.state.updateField(e.target.name, val);
          this.saveButton.disabled = !this.state.isDirty();
        }, false, "API");
      } else {
        EventHandler.on(el, "change", (e) => {
          const val = e.target.selectedOptions ? e.target.selectedOptions[0].value : e.target.value;
          this.state.updateField(e.target.name, val);
          this.saveButton.disabled = !this.state.isDirty();
        }, false, "API");
      }
    });
  }

  setDirty() { if (this.saveButton) this.saveButton.disabled = !this.state.isDirty(); }

  /** Gather values to send to backend. Includes panel id if present */
  getValues() {
    const panel = getPanelItems();
    const found = panel.find((r) => r.id === window.data.selectedRectangle) || null;
    const id = found ? found.id : "0";
    return { id, ...this.state.getUpdatedFieldsWithRequiredFields(["api_name", "specification_url", "endpoint_url", "truncate", "create_table"], false) };
  }

  /** Add/remove rate limit inputs dynamically (keeps it simple and safe) */
  handleRateLimitCheckboxChange(checked) {
    const parent = this.shadowRoot.getElementById("customRateLimit").parentElement;
    const existing = parent.querySelector('.rateLimitInput');
    if (checked && !existing) {
      const input = createElement("input", { attrs: { type: "text", name: "rateLimit", placeholder: "1000" }, className: "rateLimitInput", style: { width: '100px', marginTop: '8px' } });
      const label = createElement("label", { text: "requests/hour", style: { marginLeft: '8px' } });
      parent.appendChild(input);
      parent.appendChild(label);
      EventHandler.on(input, 'input', (e) => { this.state.updateField('rateLimit', e.target.value); this.setDirty(); }, false, 'API');
    } else if (!checked && existing) {
      existing.nextSibling?.remove();
      existing.remove();
    }
  }

  handleAuthSelectChange(e) {
    this.authFields.forEach(div => div.classList.remove('active'));
    if (e.target.value) this.shadowRoot.getElementById(e.target.value)?.classList.add('active');
  }

  /**
   * Fetch endpoints from backend (backend handles spec parsing)
   */
  async handleGetEndpointBtnClick() {
    const spec = this.endpointUrl.value?.trim();
    if (!spec) return alert('Enter specification URL');

    try {
      const json = await request(`/getEndpoints?url=${encodeURIComponent(spec)}`);
      this.renderEndpointRows(json || []);
    } catch (err) {
      console.error(err);
      alert('Failed to fetch endpoints');
    }
  }

  renderEndpointRows(list) {
    this.endpointBody.innerHTML = '';
    for (const obj of list) {
      if (!obj || !obj.endpoint) continue;
      const tr = document.createElement('tr');
      tr.dataset.endpoint = obj.endpoint;
      tr.innerHTML = `
        <td class="endpoint">${obj.endpoint}</td>
        <td><span class="method">${obj.method || ''}</span></td>
        <td><input type="text" name="table_name" placeholder="Table Name" /></td>
        <td style="text-align:center"><input type="checkbox" name="endpoint_url" class="row-check" value="${obj.endpoint}" /></td>
      `;
      this.endpointBody.appendChild(tr);
    }
  }

  searchEndpoints() {
    const term = (this.endpointSearch.value || '').toLowerCase();
    Array.from(this.endpointBody.children).forEach(tr => {
      const endpoint = tr.querySelector('.endpoint')?.textContent?.toLowerCase() || '';
      const method = tr.querySelector('.method')?.textContent?.toLowerCase() || '';
      tr.style.display = (endpoint.includes(term) || method.includes(term)) ? '' : 'none';
    });
  }

  filterEndpointsByMethod() {
    const methodFilter = (this.fetchMethodDropdown.value || '').toLowerCase();
    Array.from(this.endpointBody.children).forEach(tr => {
      const method = tr.querySelector('.method')?.textContent?.toLowerCase() || '';
      tr.style.display = method.includes(methodFilter) ? '' : 'none';
    });
  }
  
  resetFields() {
    try {
      this.endpointBody.innerHTML = '';
      this.apiName.value = '';
      this.endpointUrl.value = '';
      this.authFields.forEach((f) => f.classList.remove('active'));
      const rateBox = this.shadowRoot.getElementById('customRateLimit');
      if (rateBox && rateBox.checked) {
        rateBox.checked = false;
        const existing = rateBox.parentElement.querySelector('.rateLimitInput');
        existing?.nextSibling?.remove();
        existing?.remove();
      }
    } catch (err) {
      console.error('resetFields error', err);
    }
  }

  /** Endpoint table delegates */
  onEndpointBodyInput(e) {
    const tr = e.target.closest('tr'); if (!tr) return;
    if (e.target.name === 'table_name') {
      const checkbox = tr.querySelector('.row-check');
      if (!checkbox || !checkbox.checked) return;
      const endpoint = checkbox.value;
      const current = this.normalizeEndpointList(this.state.current?.endpoint_url);
      const next = current.map((item) => {
        if (item.endpoint_url === endpoint) return { ...item, table_name: e.target.value };
        return item;
      });
      this.state.updateField('endpoint_url', next);
    }
    this.setDirty();
  }

  onEndpointBodyClick(e) {
    // clicking radio selects endpoint_url value in state
    const tr = e.target.closest('tr'); if (!tr) return;
    const checkbox = tr.querySelector('.row-check');
    if (checkbox && (e.target === checkbox || e.target.closest('td'))) {
      if (e.target !== checkbox) checkbox.checked = !checkbox.checked;
      const endpoint = checkbox.value;
      const tableNameInput = tr.querySelector('input[name="table_name"]');
      const tableName = tableNameInput ? tableNameInput.value : '';
      const current = this.normalizeEndpointList(this.state.current?.endpoint_url);
      if (checkbox.checked) {
        const exists = current.find((item) => item.endpoint_url === endpoint);
        if (exists) {
          const next = current.map((item) => {
            if (item.endpoint_url === endpoint) return { ...item, table_name: tableName };
            return item;
          });
          this.state.updateField('endpoint_url', next);
        } else {
          this.state.updateField('endpoint_url', [...current, { endpoint_url: endpoint, table_name: tableName }]);
        }
      } else {
        const next = current.filter((item) => item.endpoint_url !== endpoint);
        this.state.updateField('endpoint_url', next);
      }
      this.setDirty();
    }
  }

  normalizeEndpointList(list) {
    if (!Array.isArray(list)) return [];
    return list.map((item) => {
      if (typeof item === 'string') return { endpoint_url: item, table_name: '' };
      return item;
    });
  }
}

customElements.define('api-connection-component', ApiConnectionComponent);
