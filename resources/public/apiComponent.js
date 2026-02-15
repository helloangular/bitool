import EventHandler from "./library/eventHandler.js";
import { createElement, customConfirm, getPanelItems, populateTable, request } from "./library/utils.js";
import StateManager from "./library/state-manager.js";

const template = document.createElement("template");
template.innerHTML = `
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<link rel="stylesheet" href="./styles/apiComponent.css" />
<div class="side-panel">
  <div class="panel-header">
    <button class="save" id="saveButton">Save</button>
    <button class="close" id="closeButton">Close</button>
  </div>

  <div class="panel-item">
    <div class="form-group">
      <label for="apiName">Api Name</label>
      <input type="text" name="api_name" id="apiName" placeholder="Enter api name">
    </div>

    <div class="form-group">
      <label for="connections">Connection</label>
      <select name="connection">
        <option value="MySQL">MySQL</option>
        <option value="PostGres">PostGres</option>
      </select>
    </div>

    <div>
      <label for="truncate">Truncate</label>
      <input type="checkbox" name="truncate" />
      <label for="create_table">Create Table</label>
      <input type="checkbox" name="create_table" />
    </div>

    <div class="card">
      <div class="api-input">
        <label for="endpointUrl" style="font-weight:600;margin-right:6px">API Spec</label>
        <input id="endpointUrl" name="specification_url" type="text" value="" />
        <button class="btn" id="getEndpoints">Get Endpoints</button>
      </div>

      <div class="endpoint-table-container">
        <div style="max-height:250px;overflow-y:auto;">
          <table aria-label="endpoints table">
            <thead style="position:sticky;top:0;background:#fff;z-index:1">
              <th>
                <label style="font-size:14px;font-weight:600">End Points: </label>
                <input type="text" id="endpointSearch" placeholder="Search endpoints..." />
                <select name="fetch_method" id="fetchMethod">
                  <option value="GET">GET</option>
                  <option value="POST">POST</option>
                  <option value="PUT">PUT</option>
                  <option value="DELETE">DELETE</option>
                </select>
              </th>
              <th></th>
              <th>Table Names</th>
              <th></th>
            </thead>
            <tbody id="endpointsBody"></tbody>
          </table>
        </div>
      </div>

      <div class="bottom-actions">
        <div style="display:flex;gap:8px">
          <button class="secondary-btn" id="extract">Extract</button>
          <button class="btn" id="getSchema">Get Schema</button>
        </div>
      </div>
    </div>

    <div class="form-group" id="tokenGroup">
      <label for="token">Personal Access Token(s)*</label>
      <select id="authSelect" name="authentication">
        <option value="">-- Select Auth Type --</option>
        <option value="noauth">No Auth</option>
        <option value="basic">Basic Auth</option>
        <option value="bearer">Bearer Token</option>
        <option value="apikey">API Key</option>
        <option value="oauth">OAuth2</option>
        <option value="customheader">Custom Header</option>
        <option value="hmac">HMAC / Signed Request</option>
      </select>

      <div class="auth-fields" id="noauth"> <p>No authentication required.</p> </div>
      <div class="auth-fields" id="basic"> <input type="text" name="username" placeholder="Username"> <input type="password" name="password" placeholder="Password"> </div>
      <div class="auth-fields" id="bearer"> <input type="text" name="bearer_token" placeholder="Bearer Token"> </div>
      <div class="auth-fields" id="apikey"> <input type="text" name="api_key" placeholder="API Key"> <input type="text" name="key_name" placeholder="Key Name (optional)"> <select name="key_location"><option value="header">Header</option><option value="query">Query</option></select></div>
      <div class="auth-fields" id="oauth"> <input type="text" name="client_id" placeholder="Client ID"> <input type="text" name="client_secret" placeholder="Client Secret"> <input type="text" name="auth_url" placeholder="Auth URL"> <input type="text" name="token_url" placeholder="Token URL"> <input type="text" name="redirect_url" placeholder="Redirect URI"> <input type="text" name="scope" placeholder="Scope (space-separated)"> </div>
      <div class="auth-fields" id="customheader"> <input type="text" name="header_name" placeholder="Header Name"> <input type="text" name="header_value" placeholder="Header Value"> </div>
      <div class="auth-fields" id="hmac"> <input type="text" name="access_key" placeholder="Access Key"> <input type="text" name="secret_key" placeholder="Secret Key"> <input type="text" name="algorithm" placeholder="Algorithm (e.g. SHA256)"> </div>
    </div>

    <div>
      <label for="customRateLimit">Custom Rate Limit</label>
      <input type="checkbox" name="custom_rate_limit" id="customRateLimit" /><br/>
    </div>

    <button type="button" class="submit" id="submitBtn">Submit</button>
  </div>

  <div class="panel-content">
    <div class="tree-column">
      <smart-tree id="jsonTree" selection-mode="checkBox" selection-target="all"></smart-tree>
    </div>
    <div class="details-column" id="selectedDisplay">No nodes selected</div>
  </div>

  <div id="expressionBox" style="padding:10px;">
    <button class="btn" id="addParamBtn">Add Params</button>
  </div>
</div>

<div class="wrapper" id="extractDialog" style="display:none;">
  <header class="header">
    <h1>Test Extract</h1>
    <button class="extract-btn" id="extractCloseBtn">✕ Close</button>
  </header>
  <section class="controls">
    <div class="field"><label>Records per Page</label><select id="recordPerPage"><option>5</option><option>10</option><option>50</option><option>100</option></select></div>
    <div class="field"><label>Pages</label><select id="pages"><option>1</option><option>2</option><option>5</option><option>10</option></select></div>
    <button class="extract-btn" id="extractDialogBtn">Extract</button>
  </section>
  <section class="table-section" style="overflow-y:scroll;max-height:320px;min-height:300px;"><table><thead style="position:sticky;top:0;"><tr></tr></thead><tbody></tbody></table></section>
</div>
`;

class ApiComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });

    // DOM refs (populated in connectedCallback)
    this.closeButton = null;
    this.saveButton = null;
    this.tree = null;
    this.selectedDisplay = null;
    this.apiName = null;
    this.endpointUrl = null;
    this.submitBtn = null;
    this.tokenGroup = null;
    this.authSelect = null;
    this.authFields = null;
    this.endpointBody = null;

    // state
    this.state = null;
    this.suppress = false; // used for tree selection handling
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));

    // bind DOM
    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.tree = this.shadowRoot.querySelector("#jsonTree");
    this.selectedDisplay = this.shadowRoot.getElementById("selectedDisplay");
    this.apiName = this.shadowRoot.getElementById("apiName");
    this.endpointUrl = this.shadowRoot.getElementById("endpointUrl");
    this.submitBtn = this.shadowRoot.getElementById("submitBtn");
    this.tokenGroup = this.shadowRoot.getElementById("tokenGroup");
    this.authSelect = this.shadowRoot.getElementById("authSelect");
    this.authFields = this.shadowRoot.querySelectorAll(".auth-fields");
    this.endpointBody = this.shadowRoot.getElementById("endpointsBody");
    this.getEndpointBtn = this.shadowRoot.getElementById("getEndpoints");
    this.extractBtn = this.shadowRoot.getElementById("extract");
    this.getSchemaBtn = this.shadowRoot.getElementById("getSchema");
    this.endpointSearch = this.shadowRoot.getElementById("endpointSearch");
    this.fetchMethodDropdown = this.shadowRoot.getElementById("fetchMethod");
    this.extractDialogCloseBtn = this.shadowRoot.getElementById("extractCloseBtn");
    this.extractDialog = this.shadowRoot.getElementById("extractDialog");
    this.extractDialogBtn = this.shadowRoot.getElementById("extractDialogBtn");
    this.customRateLimit = this.shadowRoot.getElementById("customRateLimit");
    this.addParamBtn = this.shadowRoot.getElementById("addParamBtn");
    this.expressionBox = this.shadowRoot.getElementById("expressionBox");

    this.close();
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
    EventHandler.on(this.extractDialogCloseBtn, "click", () => (this.extractDialog.style.display = "none"), false, "API");
    // buttons & inputs
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "API");
    EventHandler.on(this.submitBtn, "click", () => this.onSubmit(), false, "API");
    EventHandler.on(this.getEndpointBtn, "click", () => this.handleGetEndpointBtnClick(), false, "API");
    EventHandler.on(this.extractBtn, "click", () => this.openExtractDialog(), false, "API");
    EventHandler.on(this.getSchemaBtn, "click", () => this.getSchemaOfFirstSelectedUrl(), false, "API");
    EventHandler.on(this.endpointSearch, "input", () => this.searchEndpoints(), false, "API");
    EventHandler.on(this.fetchMethodDropdown, "change", () => this.filterEndpointsByMethod(), false, "API");
    EventHandler.on(this.extractDialogBtn, "click", () => this.extractDataFromApi(), false, "API");
    EventHandler.on(this.addParamBtn, "click", () => this.addParams(), false, "API");

    // select & checkbox
    EventHandler.on(this.authSelect, 'change', (e) => this.handleAuthSelectChange(e), false, "API");
    EventHandler.on(this.customRateLimit, 'change', (e) => this.handleRateLimitCheckboxChange(e.target.checked), false, "API");

    // tree selection change
    EventHandler.on(this.tree, "change", (e) => this.onTreeChange(e), false, "API");

    // track inputs (all inputs/selects inside shadow root)
    this.trackInputs();

    // delegate endpoint table interactions (row radio/select/table_name inputs)
    EventHandler.on(this.endpointBody, "input", (e) => this.onEndpointBodyInput(e), false, "API");
    EventHandler.on(this.endpointBody, "click", (e) => this.onEndpointBodyClick(e), false, "API");
    EventHandler.on(this.expressionBox, "input", (e) => this.handleClickInExpressionBox(e), false, "API");
    EventHandler.on(this.expressionBox, "click", (e) => this.handleClickInExpressionBox(e), false, "API");
  }

  /** Initialize StateManager with default fields */
  initializeState() {
    this.state = new StateManager({
      api_name: "",
      truncate: false,
      create_table: false,
      specification_url: "",
      endpoint_url: "",
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
      custom_rate_limit: false,
      selected_nodes: [],
      table_name: "",
      params: []
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
      await request("/saveApi", { method: "POST", body: values });
      this.state.commit();
      this.reset();
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
    const id = found ? found.id : null;
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
      tr.innerHTML = `
        <td class="endpoint">${obj.endpoint}</td>
        <td><span class="method">${obj.method || ''}</span></td>
        <td><input type="text" name="table_name" placeholder="Table Name" /></td>
        <td style="text-align:center"><input type="radio" name="endpoint_url" class="row-check" value="${obj.endpoint}" /></td>
      `;
      this.endpointBody.appendChild(tr);
    }
  }

  openExtractDialog() {
    this.extractDialog.style.display = 'block';
    const thead = this.extractDialog.querySelector('thead tr');
    thead.innerHTML = '';
    (this.tree.getSelectedValues() || []).forEach(label => {
      thead.innerHTML += `<th>${label}</th>`;
    });
  }

  async getSchemaOfFirstSelectedUrl() {
    for (const tr of Array.from(this.endpointBody.children)) {
      const radio = tr.querySelector('.row-check');
      if (radio && radio.checked) {
        const url = tr.querySelector('.endpoint').textContent;
        const method = tr.querySelector('.method').textContent;
        const qs = new URLSearchParams({ spec: this.endpointUrl.value, url, method }).toString();
        try {
          const json = await request(`/getEndpointSchema?${qs}`, { method: 'GET' });
          this.createTree(json || []);
        } catch (err) {
          console.error(err);
          alert('Failed to load schema');
        }
        break;
      }
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

  async extractDataFromApi() {
    try {
      const required = this.state.getUpdatedFieldsWithRequiredFields(['endpoint_url', 'specification_url']);
      const body = {
        endpoint_url: required.endpoint_url,
        specification_url: required.specification_url,
        record_per_page: this.shadowRoot.getElementById('recordPerPage').value,
        page: this.shadowRoot.getElementById('pages').value
      };
      const data = await request('/extract', { method: 'POST', body });
      populateTable(this.extractDialog.querySelector('table'), data || []);
    } catch (err) {
      console.error(err);
      alert('Extract failed');
    }
  }

  addParams() {
    const expressionBox = this.shadowRoot.getElementById("expressionBox");
    const div = createElement("div", {
      attrs: { id: 'exp'+(expressionBox.children.length-1).toString().padStart(2,'0') },
      style: { marginTop: "10px"},
      children: [
        { tag: "label", options: { text: "Params: " } },
        { tag: "input", options: { attrs: { type: "text", name: "label", placeholder: "Enter params name..." }, style: { margin: "10px 0" } }},
        { tag: "expression-component" },
        { tag: "button", options: { text: "Delete", className: "delete-btn" } }
      ]
    });
    
    expressionBox.appendChild(div);
    this.state.updateField("params", { name: "", expression: "" }, { type: "push" });
    this.setDirty();
  }

  handleClickInExpressionBox(e) {
    const id = e.target.parentElement.id;
    const index = Number(id.replace(/\D/g, ""));

    console.log(e.target.tagName);
    if (e.target.tagName === "INPUT") {
      this.state.updateField("params", { name: e.target.value || "" }, { type: "updateObjectInArray", index });
    } else if (e.target.tagName === "BUTTON" && e.target.classList.contains("delete-btn")) {
      e.target.parentElement.remove();
      this.state.updateField("params", null, { type: "splice", index });
    } else if (e.target.tagName === "EXPRESSION-COMPONENT") {
      const parent = e.target.parentElement;
      const expression = parent.querySelector("expression-component");
      this.state.updateField("params", { expression: expression.expressionArea.getTextContent() }, { type: "updateObjectInArray", index });
    }
    this.setDirty();
  }

  async onSubmit() {
    const spec = this.endpointUrl.value?.trim();
    if (!spec) return alert('Enter endpoint url.');
    try {
      const data = await request(`/getEndpoints?spec=${encodeURIComponent(spec)}`);
      this.createTree(data || []);
    } catch (err) {
      console.error(err);
      alert('Failed to create tree');
    }
  }

  resetFields() {
    try {
      this.tree.dataSource = [];
      this.selectedDisplay.textContent = 'No nodes selected.';
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

  createTree(data) { this.tree.dataSource = this.convertToTreeItems(data || []); }

  onTreeChange(event) {
    if (!event?.detail) return;
    // update display with label paths
    const idxs = event.detail.selectedIndexes || [];
    const labels = idxs.map(i => this.getLabelPathByIndex(this.tree.dataSource[0], i)).filter(Boolean).map(p => p.replace('Root', '$').replaceAll('[]', ''));
    this.selectedDisplay.textContent = labels.length ? labels.join('\n') : 'No nodes selected.';
    this.state.updateField('selected_nodes', labels);
    this.setDirty();

    // handle parent selection propagation (user action only)
    if (this.suppress) return;
    const titleLabel = event.detail.item?.titleLabel || null;
    if (!titleLabel) return;

    const item = this.findTree(this.tree.dataSource, titleLabel);
    if (!item?.items?.length) return;

    const added = event.detail.selectedIndexes.filter(i => !event.detail.oldSelectedIndexes.includes(i));
    const removed = event.detail.oldSelectedIndexes.filter(i => !event.detail.selectedIndexes.includes(i));

    this.suppress = true;
    try {
      if (added.length > 0) {
        const parentPath = added[0];
        for (let i = 0; i < item.items.length; i++) this.tree.select(`${parentPath}.${i}`);
      } else if (removed.length > 0) {
        const parentPath = removed[0];
        for (let i = 0; i < item.items.length; i++) this.tree.unselect(`${parentPath}.${i}`);
      }
    } finally {
      this.suppress = false;
    }
  }

  convertToTreeItems(jsonNode) {
    return (jsonNode || []).map(node => ({ label: node.name, expanded: true, selected: false, items: node.children && node.children.length ? this.convertToTreeItems(node.children) : undefined }));
  }

  findTree(tree, label) {
    for (const item of tree || []) {
      if (item.label === label) return item;
      if (item.items) {
        const r = this.findTree(item.items, label); if (r) return r;
      }
    }
    return null;
  }

  getLabelPathByIndex(node, indexPath) {
    if (!node || !indexPath) return null;
    const indices = String(indexPath).split('.').map(Number);
    let current = node; const labels = [current.label];
    indices.shift(); // skip root index
    for (const i of indices) {
      if (!current.items || !current.items[i]) return null;
      current = current.items[i]; labels.push(current.label);
    }
    return labels.join('.');
  }

  /** Endpoint table delegates */
  onEndpointBodyInput(e) {
    const tr = e.target.closest('tr'); if (!tr) return;
    const endpointText = tr.querySelector('.endpoint')?.textContent || '';
    if (e.target.name === 'table_name') {
      this.state.updateField('table_name', e.target.value);
    } else {
      this.state.updateField(e.target.name, endpointText);
    }
    this.setDirty();
  }

  onEndpointBodyClick(e) {
    // clicking radio selects endpoint_url value in state
    const tr = e.target.closest('tr'); if (!tr) return;
    const radio = tr.querySelector('.row-check');
    if (radio && (e.target === radio || e.target.closest('td'))) {
      radio.checked = true;
      this.state.updateField('endpoint_url', radio.value);
      this.setDirty();
    }
  }
}

customElements.define('api-component', ApiComponent);