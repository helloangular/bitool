import { request } from "./library/utils.js";

function buildTemplate() {
  const t = document.createElement("template");
  t.innerHTML = `
<style>
  :host { display: block; font-family: Georgia, "Times New Roman", serif; color: #1a1a2e; }
  * { box-sizing: border-box; }
  .empty { text-align: center; padding: 40px 20px; color: #9ca3af; }
  .empty h3 { color: #6b7280; margin-bottom: 8px; }

  /* Cards */
  .card {
    background: rgba(255,255,255,0.92); border: 1px solid rgba(0,0,0,0.08);
    box-shadow: 0 4px 12px rgba(0,0,0,0.04); padding: 18px; margin-bottom: 14px;
  }
  .card h3 { margin: 0 0 12px; font-size: 16px; font-weight: 600; }
  .card h4 { margin: 0 0 8px; font-size: 14px; font-weight: 600; color: #374151; }
  .flex-between { display: flex; justify-content: space-between; align-items: center; }

  /* Badges */
  .badge {
    display: inline-block; padding: 2px 8px; font-size: 11px; font-weight: 600;
    border-radius: 10px; text-transform: uppercase; letter-spacing: 0.3px;
  }
  .badge-draft { background: #fef3c7; color: #92400e; }
  .badge-published { background: #a7f3d0; color: #064e3b; }
  .badge-archived { background: #e5e7eb; color: #6b7280; }

  /* Buttons */
  button {
    border: 1px solid #203326; background: #203326; color: #fffdf8;
    padding: 8px 14px; cursor: pointer; font-size: 13px; font-family: inherit;
  }
  button.secondary { background: transparent; color: #203326; }
  button.small { padding: 5px 10px; font-size: 12px; }
  button.primary-blue { background: #2563eb; border-color: #2563eb; color: #fff; }
  button.danger { background: #dc2626; border-color: #dc2626; color: #fff; }
  button.success { background: #16a34a; border-color: #16a34a; color: #fff; }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  .actions { display: flex; gap: 8px; flex-wrap: wrap; }

  /* Forms */
  label { display: block; font-size: 12px; color: #6b7280; margin-bottom: 3px; font-weight: 500; }
  input, select {
    width: 100%; border: 1px solid #d1d5db; background: #fff; padding: 7px 10px;
    color: #1a1a2e; font-size: 13px; font-family: inherit;
  }
  input:focus, select:focus { outline: none; border-color: #2563eb; box-shadow: 0 0 0 2px rgba(37,99,235,0.1); }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }

  /* Tables */
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; background: #f9fafb; border-bottom: 2px solid #e5e7eb; font-weight: 600; color: #374151; font-size: 12px; }
  td { padding: 8px 10px; border-bottom: 1px solid #f3f4f6; color: #374151; }
  tr:hover td { background: #f9fafb; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover td { background: #eff6ff; }

  /* Status */
  .status-bar { min-height: 24px; color: #6b7280; font-size: 13px; margin-top: 8px; }
  .status-bar.error { color: #dc2626; }
  .status-bar.success { color: #16a34a; }

  /* Detail sections */
  .entity-card {
    border: 1px solid #e5e7eb; border-radius: 6px; padding: 14px; margin-bottom: 12px;
    background: #fafbfc;
  }
  .entity-card h4 { margin: 0 0 6px; font-size: 14px; }
  .entity-kind { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.3px; padding: 2px 6px; border-radius: 4px; }
  .kind-fact { background: #dbeafe; color: #1e40af; }
  .kind-dimension { background: #fef3c7; color: #92400e; }
  .kind-entity { background: #e0e7ff; color: #3730a3; }
  .kind-mart { background: #d1fae5; color: #065f46; }

  .col-role { font-size: 10px; padding: 1px 5px; border-radius: 3px; font-weight: 600; }
  .role-business_key { background: #fef3c7; color: #92400e; }
  .role-timestamp { background: #dbeafe; color: #1e40af; }
  .role-measure { background: #d1fae5; color: #065f46; }
  .role-measure_candidate { background: #d1fae5; color: #065f46; }
  .role-attribute { background: #f3f4f6; color: #6b7280; }
  .role-time_dimension { background: #e0e7ff; color: #3730a3; }

  .rel-arrow { color: #2563eb; font-weight: 600; }
  .confidence-bar { display: inline-block; height: 6px; border-radius: 3px; background: #e5e7eb; width: 60px; vertical-align: middle; }
  .confidence-fill { display: block; height: 100%; border-radius: 3px; background: #16a34a; }

  .back-link { cursor: pointer; color: #2563eb; font-size: 13px; margin-bottom: 12px; display: inline-block; }
  .back-link:hover { text-decoration: underline; }

  .toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }
  .toolbar select, .toolbar input { width: auto; min-width: 140px; }
</style>

<div>
  <div class="status-bar" id="status"></div>

  <!-- LIST VIEW -->
  <div id="listView">
    <div class="card">
      <div class="flex-between">
        <h3>Semantic Models</h3>
        <div class="actions">
          <button id="generateBtn" class="primary-blue small" type="button">+ Generate Model</button>
          <button id="refreshBtn" class="secondary small" type="button">Refresh</button>
        </div>
      </div>
      <div class="toolbar" style="margin-top:12px;">
        <div>
          <label for="connFilter">Connection ID</label>
          <input id="connFilter" type="number" placeholder="1" style="width:80px;" />
        </div>
        <div>
          <label for="schemaFilter">Schema</label>
          <input id="schemaFilter" type="text" placeholder="public" style="width:120px;" />
        </div>
        <div>
          <label for="statusFilter">Status</label>
          <select id="statusFilter">
            <option value="">All</option>
            <option value="draft">Draft</option>
            <option value="published">Published</option>
            <option value="archived">Archived</option>
          </select>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Schema</th>
            <th>Version</th>
            <th>Status</th>
            <th>Confidence</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody id="modelsBody"></tbody>
      </table>
      <div id="modelsEmpty" class="empty" style="display:none;">
        <h3>No semantic models yet</h3>
        <p>Generate one from your Silver/Gold proposals to get started.</p>
      </div>
    </div>

    <!-- Generate form -->
    <div class="card" id="generateCard" style="display:none;">
      <h3>Generate Semantic Model</h3>
      <p style="font-size:13px;color:#6b7280;margin:0 0 12px;">
        Assembles a semantic model from existing Silver/Gold proposals, schema context, and discovered joins.
      </p>
      <div class="grid">
        <div>
          <label for="genConnId">Connection ID</label>
          <input id="genConnId" type="number" placeholder="1" />
        </div>
        <div>
          <label for="genSchema">Schema</label>
          <input id="genSchema" type="text" placeholder="public" />
        </div>
        <div>
          <label for="genGraphIds">Graph IDs (comma-separated)</label>
          <input id="genGraphIds" type="text" placeholder="1, 2" />
        </div>
      </div>
      <div class="actions" style="margin-top:12px;">
        <button id="submitGenBtn" class="primary-blue small" type="button">Generate</button>
        <button id="cancelGenBtn" class="secondary small" type="button">Cancel</button>
      </div>
    </div>
  </div>

  <!-- DETAIL VIEW -->
  <div id="detailView" style="display:none;">
    <span class="back-link" id="backToList">&larr; Back to Models</span>
    <div class="card">
      <div class="flex-between">
        <div>
          <h3 id="detailName">Model</h3>
          <div style="font-size:12px;color:#6b7280;" id="detailMeta"></div>
        </div>
        <div class="actions">
          <button id="publishBtn" class="success small" type="button">Publish</button>
          <button id="archiveBtn" class="danger small" type="button">Archive</button>
          <button id="versionsBtn" class="secondary small" type="button">Versions</button>
        </div>
      </div>
    </div>

    <!-- Entities -->
    <div class="card">
      <h3>Entities</h3>
      <div id="entitiesContainer"></div>
    </div>

    <!-- Relationships -->
    <div class="card">
      <h3>Relationships</h3>
      <div id="relsContainer"></div>
    </div>

    <!-- Calculated Measures -->
    <div class="card">
      <div class="flex-between">
        <h3>Calculated Measures</h3>
        <button id="addCalcBtn" class="primary-blue small" type="button">+ Add Measure</button>
      </div>
      <div id="calcContainer"></div>
      <div id="addCalcForm" style="display:none;margin-top:12px;border-top:1px solid #e5e7eb;padding-top:12px;">
        <div class="grid">
          <div><label>Name</label><input id="calcName" type="text" placeholder="cost_per_mile" /></div>
          <div><label>Entity</label><input id="calcEntity" type="text" placeholder="trips" /></div>
          <div><label>Aggregation</label><select id="calcAgg"><option value="row">Row-level</option><option value="post">Post-aggregate</option></select></div>
        </div>
        <div style="margin-top:8px;"><label>Expression</label><input id="calcExpr" type="text" placeholder="fuel_cost / NULLIF(miles, 0)" style="font-family:monospace;" /></div>
        <div style="margin-top:8px;"><label>Description (optional)</label><input id="calcDesc" type="text" /></div>
        <div class="actions" style="margin-top:8px;">
          <button id="submitCalcBtn" class="primary-blue small" type="button">Save</button>
          <button id="cancelCalcBtn" class="secondary small" type="button">Cancel</button>
        </div>
      </div>
    </div>

    <!-- Restricted Measures -->
    <div class="card">
      <div class="flex-between">
        <h3>Restricted Measures</h3>
        <button id="addRestrBtn" class="primary-blue small" type="button">+ Add Restricted</button>
      </div>
      <div id="restrContainer"></div>
      <div id="addRestrForm" style="display:none;margin-top:12px;border-top:1px solid #e5e7eb;padding-top:12px;">
        <div class="grid">
          <div><label>Name</label><input id="restrName" type="text" placeholder="emea_revenue" /></div>
          <div><label>Entity</label><input id="restrEntity" type="text" placeholder="orders" /></div>
          <div><label>Base Measure</label><input id="restrBase" type="text" placeholder="SUM(revenue)" style="font-family:monospace;" /></div>
        </div>
        <div class="grid" style="margin-top:8px;">
          <div><label>Filter Column</label><input id="restrFilterCol" type="text" placeholder="region" /></div>
          <div><label>Filter Values (comma-sep)</label><input id="restrFilterVals" type="text" placeholder="EMEA, APAC" /></div>
          <div><label>Via Relationship (optional)</label><input id="restrVia" type="text" placeholder="" /></div>
        </div>
        <div class="actions" style="margin-top:8px;">
          <button id="submitRestrBtn" class="primary-blue small" type="button">Save</button>
          <button id="cancelRestrBtn" class="secondary small" type="button">Cancel</button>
        </div>
      </div>
    </div>

    <!-- Hierarchies -->
    <div class="card">
      <div class="flex-between">
        <h3>Hierarchies</h3>
        <button id="addHierBtn" class="primary-blue small" type="button">+ Add Hierarchy</button>
      </div>
      <div id="hierContainer"></div>
      <div id="addHierForm" style="display:none;margin-top:12px;border-top:1px solid #e5e7eb;padding-top:12px;">
        <div class="grid">
          <div><label>Name</label><input id="hierName" type="text" placeholder="geography" /></div>
          <div><label>Entity</label><input id="hierEntity" type="text" placeholder="locations" /></div>
          <div><label>Levels (comma-sep columns)</label><input id="hierLevels" type="text" placeholder="country, region, city" /></div>
        </div>
        <div class="actions" style="margin-top:8px;">
          <button id="submitHierBtn" class="primary-blue small" type="button">Save</button>
          <button id="cancelHierBtn" class="secondary small" type="button">Cancel</button>
        </div>
      </div>
    </div>

    <!-- Versions panel (hidden by default) -->
    <div class="card" id="versionsCard" style="display:none;">
      <h3>Version History</h3>
      <table>
        <thead>
          <tr><th>Version</th><th>Change</th><th>By</th><th>Date</th></tr>
        </thead>
        <tbody id="versionsBody"></tbody>
      </table>
    </div>
  </div>
</div>`;
  return t;
}

function fmtDate(d) {
  if (!d) return "--";
  try { return new Date(d).toLocaleString(); } catch { return String(d); }
}

function badgeFor(status) {
  const map = { draft: "badge-draft", published: "badge-published", archived: "badge-archived" };
  return map[(status || "").toLowerCase()] || "badge-draft";
}

function kindClass(kind) {
  const map = { fact: "kind-fact", dimension: "kind-dimension", entity: "kind-entity", mart: "kind-mart" };
  return map[(kind || "").toLowerCase()] || "kind-entity";
}

function roleClass(role) {
  return `role-${(role || "attribute").toLowerCase().replace(/\s+/g, "_")}`;
}

class SemanticLayerComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._models = [];
    this._currentModel = null;
  }

  connectedCallback() {
    this.shadowRoot.appendChild(buildTemplate().content.cloneNode(true));
    this._bind();
    this._attach();
  }

  _bind() {
    const q = (s) => this.shadowRoot.querySelector(s);
    this.$status = q("#status");
    this.$listView = q("#listView");
    this.$detailView = q("#detailView");
    this.$modelsBody = q("#modelsBody");
    this.$modelsEmpty = q("#modelsEmpty");
    this.$generateCard = q("#generateCard");
    this.$generateBtn = q("#generateBtn");
    this.$refreshBtn = q("#refreshBtn");
    this.$connFilter = q("#connFilter");
    this.$schemaFilter = q("#schemaFilter");
    this.$statusFilter = q("#statusFilter");
    this.$genConnId = q("#genConnId");
    this.$genSchema = q("#genSchema");
    this.$genGraphIds = q("#genGraphIds");
    this.$submitGenBtn = q("#submitGenBtn");
    this.$cancelGenBtn = q("#cancelGenBtn");
    this.$backToList = q("#backToList");
    this.$detailName = q("#detailName");
    this.$detailMeta = q("#detailMeta");
    this.$publishBtn = q("#publishBtn");
    this.$archiveBtn = q("#archiveBtn");
    this.$versionsBtn = q("#versionsBtn");
    this.$entitiesContainer = q("#entitiesContainer");
    this.$relsContainer = q("#relsContainer");
    this.$versionsCard = q("#versionsCard");
    this.$versionsBody = q("#versionsBody");
    // Phase 2 elements
    this.$calcContainer = q("#calcContainer");
    this.$addCalcBtn = q("#addCalcBtn");
    this.$addCalcForm = q("#addCalcForm");
    this.$calcName = q("#calcName");
    this.$calcEntity = q("#calcEntity");
    this.$calcAgg = q("#calcAgg");
    this.$calcExpr = q("#calcExpr");
    this.$calcDesc = q("#calcDesc");
    this.$submitCalcBtn = q("#submitCalcBtn");
    this.$cancelCalcBtn = q("#cancelCalcBtn");
    this.$restrContainer = q("#restrContainer");
    this.$addRestrBtn = q("#addRestrBtn");
    this.$addRestrForm = q("#addRestrForm");
    this.$restrName = q("#restrName");
    this.$restrEntity = q("#restrEntity");
    this.$restrBase = q("#restrBase");
    this.$restrFilterCol = q("#restrFilterCol");
    this.$restrFilterVals = q("#restrFilterVals");
    this.$restrVia = q("#restrVia");
    this.$submitRestrBtn = q("#submitRestrBtn");
    this.$cancelRestrBtn = q("#cancelRestrBtn");
    this.$hierContainer = q("#hierContainer");
    this.$addHierBtn = q("#addHierBtn");
    this.$addHierForm = q("#addHierForm");
    this.$hierName = q("#hierName");
    this.$hierEntity = q("#hierEntity");
    this.$hierLevels = q("#hierLevels");
    this.$submitHierBtn = q("#submitHierBtn");
    this.$cancelHierBtn = q("#cancelHierBtn");
  }

  _attach() {
    this.$generateBtn.addEventListener("click", () => {
      this.$generateCard.style.display = this.$generateCard.style.display === "none" ? "block" : "none";
    });
    this.$cancelGenBtn.addEventListener("click", () => { this.$generateCard.style.display = "none"; });
    this.$submitGenBtn.addEventListener("click", () => this._generate());
    this.$refreshBtn.addEventListener("click", () => this.loadModels());
    this.$connFilter.addEventListener("change", () => this.loadModels());
    this.$schemaFilter.addEventListener("change", () => this.loadModels());
    this.$statusFilter.addEventListener("change", () => this.loadModels());
    this.$backToList.addEventListener("click", () => this._showList());
    this.$publishBtn.addEventListener("click", () => this._publish());
    this.$archiveBtn.addEventListener("click", () => this._archive());
    this.$versionsBtn.addEventListener("click", () => this._toggleVersions());
    // Phase 2 events
    this.$addCalcBtn.addEventListener("click", () => {
      this.$addCalcForm.style.display = this.$addCalcForm.style.display === "none" ? "block" : "none";
    });
    this.$cancelCalcBtn.addEventListener("click", () => { this.$addCalcForm.style.display = "none"; });
    this.$submitCalcBtn.addEventListener("click", () => this._addCalculatedMeasure());
    this.$addRestrBtn.addEventListener("click", () => {
      this.$addRestrForm.style.display = this.$addRestrForm.style.display === "none" ? "block" : "none";
    });
    this.$cancelRestrBtn.addEventListener("click", () => { this.$addRestrForm.style.display = "none"; });
    this.$submitRestrBtn.addEventListener("click", () => this._addRestrictedMeasure());
    this.$addHierBtn.addEventListener("click", () => {
      this.$addHierForm.style.display = this.$addHierForm.style.display === "none" ? "block" : "none";
    });
    this.$cancelHierBtn.addEventListener("click", () => { this.$addHierForm.style.display = "none"; });
    this.$submitHierBtn.addEventListener("click", () => this._addHierarchy());
  }

  _setStatus(msg, type = "") {
    this.$status.textContent = msg || "";
    this.$status.className = type ? `status-bar ${type}` : "status-bar";
  }

  _showList() {
    this.$listView.style.display = "block";
    this.$detailView.style.display = "none";
    this._currentModel = null;
  }

  _showDetail() {
    this.$listView.style.display = "none";
    this.$detailView.style.display = "block";
  }

  // ── List ──
  async loadModels() {
    const connId = this.$connFilter.value || "";
    if (!connId) {
      this.$modelsBody.innerHTML = "";
      this.$modelsEmpty.style.display = "block";
      this.$modelsEmpty.querySelector("p").textContent = "Enter a Connection ID to browse semantic models.";
      return;
    }
    try {
      const schema = this.$schemaFilter.value || "";
      const status = this.$statusFilter.value || "";
      let url = `/api/semantic/models?conn_id=${encodeURIComponent(connId)}`;
      if (schema) url += `&schema=${encodeURIComponent(schema)}`;
      if (status) url += `&status=${encodeURIComponent(status)}`;
      const resp = await request(url);
      const models = resp?.data?.models || resp?.models || [];
      this._models = models;
      this._renderModels(models);
      this._setStatus(`${models.length} model(s) loaded.`);
    } catch (e) {
      this._setStatus("Failed to load models: " + (e.message || e), "error");
    }
  }

  _renderModels(models) {
    this.$modelsBody.innerHTML = "";
    this.$modelsEmpty.style.display = models.length ? "none" : "block";
    if (!models.length) {
      this.$modelsEmpty.querySelector("p").textContent = "No semantic models found. Generate one from your proposals.";
      return;
    }
    models.forEach((m) => {
      const tr = document.createElement("tr");
      tr.className = "clickable";
      const conf = m.confidence_score != null ? Math.round(m.confidence_score * 100) : "--";
      tr.innerHTML = `
        <td>${m.model_id}</td>
        <td><strong>${m.name || "--"}</strong></td>
        <td>${m.schema_name || "public"}</td>
        <td>v${m.version || 1}</td>
        <td><span class="badge ${badgeFor(m.status)}">${m.status || "draft"}</span></td>
        <td>${conf}%</td>
        <td>${fmtDate(m.updated_at_utc)}</td>`;
      tr.addEventListener("click", () => this._openModel(m.model_id));
      this.$modelsBody.appendChild(tr);
    });
  }

  // ── Detail ──
  async _openModel(modelId) {
    try {
      const resp = await request(`/api/semantic/models/${modelId}`);
      const row = resp?.data || resp;
      this._currentModel = row;
      this._renderDetail(row);
      this._showDetail();
      this.$versionsCard.style.display = "none";
      this._setStatus("");
    } catch (e) {
      this._setStatus("Failed to load model: " + (e.message || e), "error");
    }
  }

  _renderDetail(row) {
    const model = row.model || {};
    this.$detailName.textContent = row.name || model.name || "Semantic Model";
    const conf = model.confidence != null ? Math.round(model.confidence * 100) : "--";
    this.$detailMeta.innerHTML = `
      <span class="badge ${badgeFor(row.status)}">${row.status || "draft"}</span>
      &nbsp; v${row.version || 1} &nbsp;|&nbsp; Confidence: ${conf}%
      &nbsp;|&nbsp; Connection: ${row.conn_id} &nbsp;|&nbsp; Schema: ${row.schema_name || "public"}
      &nbsp;|&nbsp; Source: ${model.source || "auto"}
      &nbsp;|&nbsp; Updated: ${fmtDate(row.updated_at_utc)}`;

    this.$publishBtn.disabled = row.status === "published";
    this.$archiveBtn.disabled = row.status === "archived";

    this._renderEntities(model.entities || {});
    this._renderRelationships(model.relationships || []);
    this._renderCalcMeasures(model.calculated_measures || []);
    this._renderRestrMeasures(model.restricted_measures || []);
    this._renderHierarchies(model.hierarchies || []);
  }

  _renderEntities(entities) {
    this.$entitiesContainer.innerHTML = "";
    const keys = Object.keys(entities);
    if (!keys.length) {
      this.$entitiesContainer.innerHTML = '<div class="empty"><p>No entities in this model.</p></div>';
      return;
    }
    keys.forEach((key) => {
      const ent = entities[key];
      const div = document.createElement("div");
      div.className = "entity-card";

      const columns = ent.columns || [];
      const colRows = columns.map((c) => `
        <tr>
          <td>${c.name || "--"}</td>
          <td>${c.type || "STRING"}</td>
          <td><span class="col-role ${roleClass(c.role)}">${c.role || "attribute"}</span></td>
          <td>${c.nullable !== false ? "YES" : "NO"}</td>
          <td style="font-size:12px;color:#6b7280;">${c.description || ""}</td>
        </tr>`).join("");

      const grain = ent.grain;
      const grainLabel = grain
        ? (typeof grain === "string" ? grain : JSON.stringify(grain))
        : "--";

      div.innerHTML = `
        <div class="flex-between">
          <h4>${key}</h4>
          <span class="entity-kind ${kindClass(ent.kind)}">${ent.kind || "entity"}</span>
        </div>
        <div style="font-size:12px;color:#6b7280;margin-bottom:8px;">
          Table: <strong>${ent.table || key}</strong> &nbsp;|&nbsp; Grain: ${grainLabel}
          &nbsp;|&nbsp; Columns: ${columns.length}
          ${ent.description ? `<br/>${ent.description}` : ""}
        </div>
        <table>
          <thead><tr><th>Column</th><th>Type</th><th>Role</th><th>Nullable</th><th>Description</th></tr></thead>
          <tbody>${colRows}</tbody>
        </table>`;
      this.$entitiesContainer.appendChild(div);
    });
  }

  _renderRelationships(rels) {
    this.$relsContainer.innerHTML = "";
    if (!rels.length) {
      this.$relsContainer.innerHTML = `
        <div class="empty">
          <p>No relationships discovered. This is expected for Snowflake/Databricks/BigQuery
          which don't expose FK metadata. Relationships can be added manually.</p>
        </div>`;
      return;
    }
    const rows = rels.map((r) => `
      <tr>
        <td><strong>${r.from}</strong>.${r.from_column}</td>
        <td class="rel-arrow">&rarr;</td>
        <td><strong>${r.to}</strong>.${r.to_column}</td>
        <td>${r.type || "many_to_one"}</td>
        <td>${r.join || "LEFT"}</td>
      </tr>`).join("");
    this.$relsContainer.innerHTML = `
      <table>
        <thead><tr><th>From</th><th></th><th>To</th><th>Type</th><th>Join</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  }

  // ── Calculated Measures ──
  _renderCalcMeasures(measures) {
    this.$calcContainer.innerHTML = "";
    if (!measures.length) {
      this.$calcContainer.innerHTML = '<div style="font-size:12px;color:#9ca3af;">No calculated measures defined yet.</div>';
      return;
    }
    const rows = measures.map((m) => `
      <tr>
        <td><strong>${m.name}</strong></td>
        <td>${m.entity || "--"}</td>
        <td style="font-family:monospace;font-size:12px;">${m.expression || "--"}</td>
        <td>${m.aggregation || "row"}</td>
        <td style="font-size:12px;color:#6b7280;">${m.description || ""}</td>
      </tr>`).join("");
    this.$calcContainer.innerHTML = `
      <table>
        <thead><tr><th>Name</th><th>Entity</th><th>Expression</th><th>Aggregation</th><th>Description</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  }

  async _addCalculatedMeasure() {
    if (!this._currentModel) return;
    try {
      this._setStatus("Adding calculated measure...");
      await request(`/api/semantic/models/${this._currentModel.model_id}/measures/calculated`, {
        method: "POST",
        body: {
          name: this.$calcName.value.trim(),
          entity: this.$calcEntity.value.trim(),
          expression: this.$calcExpr.value.trim(),
          aggregation: this.$calcAgg.value,
          description: this.$calcDesc.value.trim() || undefined,
        },
      });
      this.$addCalcForm.style.display = "none";
      this.$calcName.value = "";
      this.$calcEntity.value = "";
      this.$calcExpr.value = "";
      this.$calcDesc.value = "";
      this._setStatus("Calculated measure added.", "success");
      await this._openModel(this._currentModel.model_id);
    } catch (e) {
      this._setStatus("Failed: " + (e.message || e), "error");
    }
  }

  // ── Restricted Measures ──
  _renderRestrMeasures(measures) {
    this.$restrContainer.innerHTML = "";
    if (!measures.length) {
      this.$restrContainer.innerHTML = '<div style="font-size:12px;color:#9ca3af;">No restricted measures defined yet.</div>';
      return;
    }
    const rows = measures.map((m) => `
      <tr>
        <td><strong>${m.name}</strong></td>
        <td>${m.entity || "--"}</td>
        <td style="font-family:monospace;font-size:12px;">${m.base_measure || "--"}</td>
        <td>${m.filter_column} IN (${(m.filter_values || []).join(", ")})</td>
        <td style="font-size:12px;color:#6b7280;">${m.via_relationship || ""}</td>
      </tr>`).join("");
    this.$restrContainer.innerHTML = `
      <table>
        <thead><tr><th>Name</th><th>Entity</th><th>Base</th><th>Filter</th><th>Via</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  }

  async _addRestrictedMeasure() {
    if (!this._currentModel) return;
    try {
      this._setStatus("Adding restricted measure...");
      const vals = this.$restrFilterVals.value.split(",").map((s) => s.trim()).filter(Boolean);
      await request(`/api/semantic/models/${this._currentModel.model_id}/measures/restricted`, {
        method: "POST",
        body: {
          name: this.$restrName.value.trim(),
          entity: this.$restrEntity.value.trim(),
          base_measure: this.$restrBase.value.trim(),
          filter_column: this.$restrFilterCol.value.trim(),
          filter_values: vals,
          via_relationship: this.$restrVia.value.trim() || undefined,
        },
      });
      this.$addRestrForm.style.display = "none";
      this.$restrName.value = "";
      this.$restrEntity.value = "";
      this.$restrBase.value = "";
      this.$restrFilterCol.value = "";
      this.$restrFilterVals.value = "";
      this.$restrVia.value = "";
      this._setStatus("Restricted measure added.", "success");
      await this._openModel(this._currentModel.model_id);
    } catch (e) {
      this._setStatus("Failed: " + (e.message || e), "error");
    }
  }

  // ── Hierarchies ──
  _renderHierarchies(hierarchies) {
    this.$hierContainer.innerHTML = "";
    if (!hierarchies.length) {
      this.$hierContainer.innerHTML = '<div style="font-size:12px;color:#9ca3af;">No hierarchies defined yet.</div>';
      return;
    }
    const rows = hierarchies.map((h) => {
      const levels = (h.levels || []).map((l) => l.column || l).join(" &rarr; ");
      return `<tr><td><strong>${h.name}</strong></td><td>${h.entity || "--"}</td><td>${levels}</td></tr>`;
    }).join("");
    this.$hierContainer.innerHTML = `
      <table>
        <thead><tr><th>Name</th><th>Entity</th><th>Levels</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  }

  async _addHierarchy() {
    if (!this._currentModel) return;
    try {
      this._setStatus("Adding hierarchy...");
      const levels = this.$hierLevels.value.split(",").map((s) => ({ column: s.trim() })).filter((l) => l.column);
      await request(`/api/semantic/models/${this._currentModel.model_id}/hierarchies`, {
        method: "POST",
        body: {
          name: this.$hierName.value.trim(),
          entity: this.$hierEntity.value.trim(),
          levels,
        },
      });
      this.$addHierForm.style.display = "none";
      this.$hierName.value = "";
      this.$hierEntity.value = "";
      this.$hierLevels.value = "";
      this._setStatus("Hierarchy added.", "success");
      await this._openModel(this._currentModel.model_id);
    } catch (e) {
      this._setStatus("Failed: " + (e.message || e), "error");
    }
  }

  // ── Generate ──
  async _generate() {
    try {
      const connId = parseInt(this.$genConnId.value, 10);
      if (!connId) throw new Error("Connection ID is required");
      const graphIds = this.$genGraphIds.value
        .split(",")
        .map((s) => parseInt(s.trim(), 10))
        .filter((n) => !isNaN(n));
      if (!graphIds.length) throw new Error("At least one Graph ID is required");

      this._setStatus("Generating semantic model...");
      this.$submitGenBtn.disabled = true;

      const resp = await request("/api/semantic/generate", {
        method: "POST",
        body: {
          conn_id: connId,
          schema: this.$genSchema.value.trim() || "public",
          graph_ids: graphIds,
        },
      });
      this.$generateCard.style.display = "none";
      this._setStatus("Semantic model generated.", "success");
      // Set filter to match and reload
      this.$connFilter.value = connId;
      await this.loadModels();
      // Open the new model
      const newId = resp?.data?.model_id || resp?.model_id;
      if (newId) await this._openModel(newId);
    } catch (e) {
      this._setStatus("Generation failed: " + (e.message || e), "error");
    } finally {
      this.$submitGenBtn.disabled = false;
    }
  }

  // ── Publish / Archive ──
  async _publish() {
    if (!this._currentModel) return;
    try {
      await request(`/api/semantic/models/${this._currentModel.model_id}/publish`, {
        method: "POST",
        body: {},
      });
      this._setStatus("Model published.", "success");
      await this._openModel(this._currentModel.model_id);
    } catch (e) {
      this._setStatus("Publish failed: " + (e.message || e), "error");
    }
  }

  async _archive() {
    if (!this._currentModel) return;
    try {
      await request(`/api/semantic/models/${this._currentModel.model_id}`, {
        method: "DELETE",
      });
      this._setStatus("Model archived.", "success");
      this._showList();
      await this.loadModels();
    } catch (e) {
      this._setStatus("Archive failed: " + (e.message || e), "error");
    }
  }

  // ── Versions ──
  async _toggleVersions() {
    if (!this._currentModel) return;
    const card = this.$versionsCard;
    if (card.style.display !== "none") {
      card.style.display = "none";
      return;
    }
    try {
      const resp = await request(`/api/semantic/models/${this._currentModel.model_id}/versions`);
      const versions = resp?.data?.versions || resp?.versions || [];
      this.$versionsBody.innerHTML = "";
      if (!versions.length) {
        this.$versionsBody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:#9ca3af;">No version history yet.</td></tr>';
      } else {
        versions.forEach((v) => {
          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td>v${v.version}</td>
            <td>${v.change_summary || "--"}</td>
            <td>${v.created_by || "system"}</td>
            <td>${fmtDate(v.created_at_utc)}</td>`;
          this.$versionsBody.appendChild(tr);
        });
      }
      card.style.display = "block";
    } catch (e) {
      this._setStatus("Failed to load versions: " + (e.message || e), "error");
    }
  }
}

customElements.define("semantic-layer-component", SemanticLayerComponent);
