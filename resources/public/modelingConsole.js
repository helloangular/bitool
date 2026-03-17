import EventHandler from "./library/eventHandler.js";
import { request, customConfirm } from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `
<style>
  :host {
    visibility: hidden;
    background: #f5f7fa;
    color: #1a1a2e;
    overflow: auto;
    font-family: Georgia, "Times New Roman", serif;
    z-index: 100;
  }
  * { box-sizing: border-box; }
  .shell {
    min-height: 100vh;
    padding: 20px 24px;
    background: linear-gradient(135deg, rgba(30,60,114,0.06), transparent 40%), linear-gradient(180deg, #f5f7fa 0%, #eef1f5 100%);
  }
  .header {
    display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 16px;
  }
  .header h2 { margin: 0; font-size: 24px; font-weight: 700; color: #1a1a2e; }
  .header p { margin: 2px 0 0 0; color: #6b7280; font-size: 13px; }
  .actions { display: flex; gap: 8px; flex-wrap: wrap; }

  button {
    border: 1px solid #203326; background: #203326; color: #fffdf8;
    padding: 8px 14px; cursor: pointer; font-size: 13px; font-family: inherit;
  }
  button.secondary { background: transparent; color: #203326; }
  button.small { padding: 5px 10px; font-size: 12px; }
  button.primary-blue { background: #2563eb; border-color: #2563eb; color: #fff; }
  button.danger { background: #dc2626; border-color: #dc2626; color: #fff; }
  button.success { background: #16a34a; border-color: #16a34a; color: #fff; }
  button.warning { background: #d97706; border-color: #d97706; color: #fff; }
  button:disabled { opacity: 0.5; cursor: not-allowed; }

  /* Tabs */
  .tabs { display: flex; gap: 0; border-bottom: 2px solid #e5e7eb; margin-bottom: 16px; }
  .tab {
    padding: 10px 20px; cursor: pointer; font-size: 14px; font-weight: 500;
    color: #6b7280; border-bottom: 2px solid transparent; margin-bottom: -2px;
    background: none; border-left: none; border-right: none; border-top: none;
  }
  .tab.active { color: #1a1a2e; border-bottom-color: #2563eb; font-weight: 600; }
  .tab:hover { color: #1a1a2e; }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* Cards */
  .card {
    background: rgba(255,255,255,0.92); border: 1px solid rgba(0,0,0,0.08);
    box-shadow: 0 4px 12px rgba(0,0,0,0.04); padding: 18px; margin-bottom: 14px;
  }
  .card h3 { margin: 0 0 12px 0; font-size: 16px; font-weight: 600; color: #1a1a2e; }
  .card h4 { margin: 0 0 8px 0; font-size: 14px; font-weight: 600; color: #374151; }

  /* Form grid */
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
  label { display: block; font-size: 12px; color: #6b7280; margin-bottom: 3px; font-weight: 500; }
  input, select, textarea {
    width: 100%; border: 1px solid #d1d5db; background: #fff; padding: 7px 10px;
    color: #1a1a2e; font-size: 13px; font-family: inherit;
  }
  textarea {
    min-height: 80px; resize: vertical;
    font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace; font-size: 12px;
  }
  input:focus, select:focus, textarea:focus { outline: none; border-color: #2563eb; box-shadow: 0 0 0 2px rgba(37,99,235,0.1); }

  /* Table */
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; background: #f9fafb; border-bottom: 2px solid #e5e7eb; font-weight: 600; color: #374151; font-size: 12px; }
  td { padding: 8px 10px; border-bottom: 1px solid #f3f4f6; color: #374151; }
  tr:hover td { background: #f9fafb; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover td { background: #eff6ff; }
  tr.selected td { background: #dbeafe; }

  /* Badges */
  .badge {
    display: inline-block; padding: 2px 8px; font-size: 11px; font-weight: 600;
    border-radius: 10px; text-transform: uppercase; letter-spacing: 0.3px;
  }
  .badge-draft { background: #fef3c7; color: #92400e; }
  .badge-proposed { background: #dbeafe; color: #1e40af; }
  .badge-compiled { background: #e0e7ff; color: #3730a3; }
  .badge-validated { background: #d1fae5; color: #065f46; }
  .badge-approved { background: #bbf7d0; color: #14532d; }
  .badge-published { background: #a7f3d0; color: #064e3b; }
  .badge-rejected { background: #fecaca; color: #991b1b; }
  .badge-running { background: #bfdbfe; color: #1e3a8a; }
  .badge-completed { background: #bbf7d0; color: #14532d; }
  .badge-failed { background: #fecaca; color: #991b1b; }

  /* Status bar */
  .status-bar { display: flex; align-items: center; gap: 8px; min-height: 24px; color: #6b7280; font-size: 13px; margin-top: 8px; }
  .status-bar .spinner { width: 14px; height: 14px; border: 2px solid #e5e7eb; border-top-color: #2563eb; border-radius: 50%; animation: spin 0.8s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* SQL preview */
  .sql-preview {
    background: #1e1e2e; color: #cdd6f4; padding: 16px; font-family: "SFMono-Regular", Consolas, monospace;
    font-size: 12px; line-height: 1.5; overflow-x: auto; white-space: pre-wrap; word-break: break-word;
    max-height: 500px; overflow-y: auto; border: 1px solid #313244;
  }
  .sql-preview .keyword { color: #89b4fa; font-weight: 600; }
  .sql-preview .string { color: #a6e3a1; }
  .sql-preview .comment { color: #6c7086; font-style: italic; }

  /* Detail panel */
  .detail-panel { display: none; }
  .detail-panel.active { display: block; }
  .back-link { cursor: pointer; color: #2563eb; font-size: 13px; margin-bottom: 12px; display: inline-block; }
  .back-link:hover { text-decoration: underline; }

  /* Editable schema table */
  .schema-table input, .schema-table select {
    border: none; background: transparent; padding: 4px 6px; width: 100%; font-size: 13px;
  }
  .schema-table input:focus, .schema-table select:focus { background: #fff; border: 1px solid #2563eb; }
  .schema-table td { padding: 2px 4px; }
  .schema-row-actions { display: flex; gap: 4px; }
  .schema-row-actions button { padding: 2px 6px; font-size: 11px; }

  /* Stepper / pipeline */
  .pipeline-steps { display: flex; align-items: center; gap: 0; margin-bottom: 16px; flex-wrap: wrap; }
  .pipeline-step {
    display: flex; align-items: center; gap: 6px; padding: 6px 14px;
    font-size: 12px; font-weight: 500; color: #6b7280; background: #f3f4f6;
    border: 1px solid #e5e7eb; cursor: pointer; position: relative;
  }
  .pipeline-step:first-child { border-radius: 4px 0 0 4px; }
  .pipeline-step:last-child { border-radius: 0 4px 4px 0; }
  .pipeline-step.done { background: #d1fae5; color: #065f46; border-color: #a7f3d0; }
  .pipeline-step.current { background: #dbeafe; color: #1e40af; border-color: #93c5fd; font-weight: 700; }
  .pipeline-step.error { background: #fecaca; color: #991b1b; border-color: #fca5a5; }
  .pipeline-step .step-num { width: 18px; height: 18px; border-radius: 50%; background: #d1d5db; display: flex; align-items: center; justify-content: center; font-size: 10px; color: #fff; font-weight: 700; }
  .pipeline-step.done .step-num { background: #16a34a; }
  .pipeline-step.current .step-num { background: #2563eb; }

  /* Diff viewer */
  .diff-line { font-family: monospace; font-size: 12px; padding: 1px 8px; white-space: pre-wrap; }
  .diff-add { background: #dcfce7; color: #166534; }
  .diff-remove { background: #fee2e2; color: #991b1b; }

  /* Split layout */
  .split { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  @media (max-width: 900px) { .split { grid-template-columns: 1fr; } }

  /* Review notes */
  .review-box { border: 1px solid #d1d5db; padding: 12px; margin-top: 10px; background: #fefce8; }
  .review-box label { font-weight: 600; margin-bottom: 6px; }

  /* Empty state */
  .empty-state { text-align: center; padding: 48px 20px; color: #9ca3af; }
  .empty-state h3 { color: #6b7280; margin-bottom: 8px; }

  /* Collapsible */
  .collapsible-header { cursor: pointer; display: flex; align-items: center; gap: 6px; user-select: none; }
  .collapsible-header::before { content: "\\25B6"; font-size: 10px; transition: transform 0.2s; }
  .collapsible-header.open::before { transform: rotate(90deg); }
  .collapsible-body { display: none; padding-top: 8px; }
  .collapsible-body.open { display: block; }

  /* Toolbar row */
  .toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }
  .toolbar select, .toolbar input { width: auto; min-width: 140px; }

  .flex-between { display: flex; justify-content: space-between; align-items: center; }
</style>

<div class="shell">
  <!-- Header -->
  <div class="header">
    <div>
      <h2>Modeling Console</h2>
      <p>Medallion architecture: Bronze → Silver → Gold schema generation, review, approval, and execution</p>
    </div>
    <div class="actions">
      <div>
        <label for="layerSelect">Layer</label>
        <select id="layerSelect">
          <option value="silver" selected>Silver</option>
          <option value="gold">Gold</option>
        </select>
      </div>
      <button id="closeBtn" class="secondary" type="button">Close</button>
    </div>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <button class="tab active" data-tab="proposals">Proposals</button>
    <button class="tab" data-tab="detail">Proposal Detail</button>
    <button class="tab" data-tab="review">Review &amp; Approve</button>
    <button class="tab" data-tab="releases">Releases &amp; Execution</button>
    <button class="tab" data-tab="sql">SQL Preview</button>
  </div>

  <!-- ================ TAB: PROPOSALS ================ -->
  <div class="tab-panel active" data-panel="proposals">
    <div class="card">
      <div class="flex-between">
        <h3 id="proposalsTitle">Silver Schema Proposals</h3>
        <div class="actions">
          <button id="newProposalBtn" class="primary-blue small" type="button">+ New Proposal</button>
          <button id="refreshProposalsBtn" class="secondary small" type="button">Refresh</button>
        </div>
      </div>
      <div class="toolbar" style="margin-top:12px;">
        <div>
          <label>Status Filter</label>
          <select id="filterStatus">
            <option value="">All</option>
            <option value="draft">Draft</option>
            <option value="proposed">Proposed</option>
            <option value="compiled">Compiled</option>
            <option value="validated">Validated</option>
            <option value="approved">Approved</option>
            <option value="published">Published</option>
            <option value="rejected">Rejected</option>
          </select>
        </div>
        <div>
          <label>Limit</label>
          <select id="filterLimit">
            <option value="25">25</option>
            <option value="50">50</option>
            <option value="100">100</option>
          </select>
        </div>
      </div>
      <table id="proposalsTable">
        <thead>
          <tr>
            <th>ID</th>
            <th>Source</th>
            <th>Endpoint</th>
            <th>Layer</th>
            <th>Status</th>
            <th>Created By</th>
            <th>Updated</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="proposalsBody"></tbody>
      </table>
      <div id="proposalsEmpty" class="empty-state" style="display:none;">
        <h3>No proposals found</h3>
        <p id="proposalsEmptyText">Create a new proposal from a Bronze source node to get started.</p>
      </div>
    </div>

    <!-- New proposal form -->
    <div class="card" id="newProposalCard" style="display:none;">
      <h3 id="newProposalTitle">Create New Silver Proposal</h3>
      <p id="newProposalHelp" style="font-size:13px;color:#6b7280;margin:0 0 12px 0;">Select a source node and endpoint to generate a Silver schema proposal. The system will profile the Bronze data and propose a canonical Silver schema for human review.</p>
      <div class="grid" id="silverProposalFields">
        <div>
          <label for="propNodeId">Source Node ID</label>
          <input id="propNodeId" type="number" placeholder="Node ID from graph" />
        </div>
        <div>
          <label for="propEndpoint">Endpoint Name</label>
          <input id="propEndpoint" type="text" placeholder="e.g. orders, users" />
        </div>
      </div>
      <div class="grid" id="goldProposalFields" style="display:none;">
        <div>
          <label for="propSilverProposalId">Silver Proposal ID</label>
          <input id="propSilverProposalId" type="number" placeholder="Existing Silver proposal ID" />
        </div>
      </div>
      <div style="margin-top:12px; display:flex; gap:8px;">
        <button id="submitProposalBtn" class="primary-blue small" type="button">Generate Proposal</button>
        <button id="cancelProposalBtn" class="secondary small" type="button">Cancel</button>
      </div>
    </div>
  </div>

  <!-- ================ TAB: PROPOSAL DETAIL ================ -->
  <div class="tab-panel" data-panel="detail">
    <div id="detailEmpty" class="empty-state">
      <h3>No proposal selected</h3>
      <p>Select a proposal from the Proposals tab to view its detail.</p>
    </div>
    <div id="detailContent" style="display:none;">
      <span class="back-link" id="backToList">&larr; Back to proposals</span>

      <!-- Pipeline steps -->
      <div class="pipeline-steps" id="pipelineSteps">
        <div class="pipeline-step" data-step="propose"><span class="step-num">1</span> Propose</div>
        <div class="pipeline-step" data-step="edit"><span class="step-num">2</span> Edit</div>
        <div class="pipeline-step" data-step="compile"><span class="step-num">3</span> Compile</div>
        <div class="pipeline-step" data-step="validate"><span class="step-num">4</span> Validate</div>
        <div class="pipeline-step" data-step="review"><span class="step-num">5</span> Review</div>
        <div class="pipeline-step" data-step="publish"><span class="step-num">6</span> Publish</div>
        <div class="pipeline-step" data-step="execute"><span class="step-num">7</span> Execute</div>
      </div>

      <!-- Proposal metadata -->
      <div class="card">
        <div class="flex-between">
          <div>
            <h3 id="detailTitle">Proposal #--</h3>
            <p style="font-size:13px;color:#6b7280;margin:4px 0 0 0;" id="detailMeta"></p>
          </div>
          <div class="actions" id="detailActions"></div>
        </div>
      </div>

      <!-- Schema editor -->
      <div class="card">
        <div class="flex-between">
          <h3 id="schemaTitle">Silver Schema</h3>
          <div class="actions">
            <button id="addColumnBtn" class="small secondary" type="button">+ Add Column</button>
            <button id="saveSchemaBtn" class="small primary-blue" type="button" disabled>Save Changes</button>
          </div>
        </div>
        <div style="overflow-x:auto;">
          <table class="schema-table" id="schemaTable">
            <thead>
              <tr>
                <th>Column Name</th>
                <th>Data Type</th>
                <th>Nullable</th>
                <th>Primary Key</th>
                <th>Description</th>
                <th>Source Expression</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="schemaBody"></tbody>
          </table>
        </div>
      </div>

      <!-- Mapping / transform info -->
      <div class="card" id="mappingCard" style="display:none;">
        <h3>Transformation Mapping</h3>
        <p id="mappingSubtitle" style="font-size:13px;color:#6b7280;margin:0 0 8px 0;">Bronze → Silver field mapping with type coercions and transformations.</p>
        <table id="mappingTable">
          <thead>
            <tr><th>Source Field</th><th>Target Column</th><th>Transform</th><th>Cast</th></tr>
          </thead>
          <tbody id="mappingBody"></tbody>
        </table>
      </div>

      <!-- Compile output -->
      <div class="card" id="compileCard" style="display:none;">
        <div class="flex-between">
          <h3>Compiled Artifact</h3>
          <button id="viewSqlBtn" class="small secondary" type="button">View Full SQL</button>
        </div>
        <div id="compileInfo" style="font-size:13px;color:#374151;"></div>
      </div>

      <!-- Validation results -->
      <div class="card" id="validationCard" style="display:none;">
        <h3>Validation Results</h3>
        <div id="validationResults"></div>
      </div>

      <div class="status-bar" id="detailStatus"></div>
    </div>
  </div>

  <!-- ================ TAB: REVIEW & APPROVE ================ -->
  <div class="tab-panel" data-panel="review">
    <div id="reviewEmpty" class="empty-state">
      <h3>No proposal loaded for review</h3>
      <p>Open a proposal from the Proposals tab, then navigate to the Review tab to approve or reject.</p>
    </div>
    <div id="reviewContent" style="display:none;">
      <div class="card">
        <h3>Review: <span id="reviewTitle"></span></h3>
        <div id="reviewPipeline" class="pipeline-steps" style="margin-bottom:14px;"></div>

        <div class="split">
          <!-- Left: schema summary -->
          <div>
            <h4 id="reviewSchemaTitle">Proposed Silver Schema</h4>
            <table id="reviewSchemaTable" style="font-size:12px;">
              <thead><tr><th>Column</th><th>Type</th><th>PK</th><th>Nullable</th></tr></thead>
              <tbody id="reviewSchemaBody"></tbody>
            </table>
          </div>
          <!-- Right: SQL preview -->
          <div>
            <h4>Compiled SQL</h4>
            <div class="sql-preview" id="reviewSqlPreview" style="max-height:300px;">No SQL compiled yet.</div>
          </div>
        </div>

        <!-- Validation summary -->
        <div id="reviewValidation" style="margin-top:14px;"></div>

        <!-- Review action -->
        <div class="review-box" style="margin-top:14px;">
          <label>Review Decision</label>
          <div style="display:flex; gap:8px; margin-top:8px; align-items:flex-start;">
            <select id="reviewDecision" style="width:160px;">
              <option value="">-- Select --</option>
              <option value="approved">Approve</option>
              <option value="changes_requested">Request Changes</option>
              <option value="rejected">Reject</option>
            </select>
            <textarea id="reviewNotes" placeholder="Review notes (required for reject/changes requested)..." style="min-height:60px; flex:1;"></textarea>
            <button id="submitReviewBtn" class="primary-blue" type="button" disabled>Submit Review</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- ================ TAB: RELEASES & EXECUTION ================ -->
  <div class="tab-panel" data-panel="releases">
    <div class="card">
      <div class="flex-between">
        <h3>Published Releases</h3>
        <button id="refreshReleasesBtn" class="secondary small" type="button">Refresh</button>
      </div>
      <table id="releasesTable">
        <thead>
          <tr><th>Release ID</th><th>Proposal</th><th>Endpoint</th><th>Layer</th><th>Published</th><th>Status</th><th>Actions</th></tr>
        </thead>
        <tbody id="releasesBody"></tbody>
      </table>
      <div id="releasesEmpty" class="empty-state" style="display:none;">
        <h3>No published releases</h3>
        <p>Publish an approved proposal to create a release.</p>
      </div>
    </div>

    <!-- Execution monitor -->
    <div class="card" id="executionCard" style="display:none;">
      <h3>Execution Monitor</h3>
      <div class="grid" style="margin-bottom:12px;">
        <div><label>Run ID</label><span id="execRunId" style="font-size:14px;font-weight:600;">--</span></div>
        <div><label>Status</label><span id="execStatus">--</span></div>
        <div><label>Started</label><span id="execStarted" style="font-size:13px;">--</span></div>
        <div><label>Duration</label><span id="execDuration" style="font-size:13px;">--</span></div>
      </div>
      <div id="execLog" style="font-family:monospace; font-size:12px; background:#1e1e2e; color:#cdd6f4; padding:12px; max-height:200px; overflow-y:auto; white-space:pre-wrap;"></div>
      <div style="margin-top:8px; display:flex; gap:8px;">
        <button id="pollRunBtn" class="small secondary" type="button">Poll Status</button>
        <button id="stopPollBtn" class="small danger" type="button" style="display:none;">Stop Polling</button>
      </div>
    </div>
  </div>

  <!-- ================ TAB: SQL PREVIEW ================ -->
  <div class="tab-panel" data-panel="sql">
    <div class="card">
      <div class="flex-between">
        <h3>Generated SQL</h3>
        <div class="actions">
          <button id="copySqlBtn" class="small secondary" type="button">Copy to Clipboard</button>
        </div>
      </div>
      <p style="font-size:13px;color:#6b7280;margin:0 0 12px 0;">
        Snowflake-dialect SQL compiled from the approved proposal. Includes DDL, DML (MERGE/INSERT), and materialization statements.
      </p>
      <div id="sqlTabs" class="tabs" style="margin-bottom:12px;">
        <button class="tab active" data-sqltab="ddl">DDL</button>
        <button class="tab" data-sqltab="dml">DML / Transform</button>
        <button class="tab" data-sqltab="full">Full Artifact</button>
      </div>
      <div class="sql-preview" id="sqlContent" style="min-height:200px;">-- No SQL available. Compile a proposal first.</div>
    </div>
  </div>
</div>
`;

// ────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────

function badgeClass(status) {
  const s = (status || "").toLowerCase().replace(/\s+/g, "_");
  const map = {
    draft: "badge-draft", proposed: "badge-proposed", compiled: "badge-compiled",
    validated: "badge-validated", approved: "badge-approved", published: "badge-published",
    rejected: "badge-rejected", running: "badge-running", completed: "badge-completed",
    failed: "badge-failed", changes_requested: "badge-warning",
  };
  return map[s] || "badge-draft";
}

function fmtDate(d) {
  if (!d) return "--";
  try { return new Date(d).toLocaleString(); } catch { return String(d); }
}

function highlightSQL(sql) {
  if (!sql) return "";
  const kw = /\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|ON|AND|OR|NOT|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|ALTER|DROP|TABLE|VIEW|INDEX|AS|MERGE|USING|WHEN|MATCHED|THEN|REPLACE|IF|EXISTS|BEGIN|END|DECLARE|VARIANT|PARSE_JSON|FLATTEN|LATERAL|TRY_TO_TIMESTAMP|TRY_TO_NUMBER|TIMESTAMP_NTZ|TIMESTAMP_TZ|VARCHAR|NUMBER|INTEGER|BOOLEAN|BIGINT|COPY|STAGE|WAREHOUSE|DATABASE|SCHEMA|WITH|CASE|ELSE|NULLS|NULL|IN|IS|LIKE|BETWEEN|GROUP|BY|ORDER|HAVING|LIMIT|OFFSET|UNION|ALL|DISTINCT|PRIMARY|KEY|NOT NULL|DEFAULT|COMMENT|CLUSTER|PARTITION|FORMAT|OVERWRITE|APPEND|TRANSIENT|TEMPORARY)\b/gi;
  const str = /('(?:[^'\\]|\\.)*')/g;
  const cmt = /(--.*$)/gm;
  return sql
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(cmt, '<span class="comment">$1</span>')
    .replace(str, '<span class="string">$1</span>')
    .replace(kw, '<span class="keyword">$1</span>');
}

const SF_TYPES = [
  "VARCHAR", "NUMBER", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "BOOLEAN",
  "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ", "DATE", "TIME",
  "VARIANT", "OBJECT", "ARRAY", "BINARY", "NUMBER(38,0)", "NUMBER(18,2)",
  "NUMBER(10,4)", "VARCHAR(256)", "VARCHAR(1000)", "VARCHAR(4000)", "VARCHAR(16777216)"
];

// ────────────────────────────────────────────────────────────────
// Component
// ────────────────────────────────────────────────────────────────

class ModelingConsole extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._proposals = [];
    this._currentProposal = null;
    this._compiledArtifact = null;
    this._schemaDirty = false;
    this._pollTimer = null;
    this._currentRunId = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this._bindElements();
    this.closeImmediately();
  }

  disconnectedCallback() {
    EventHandler.removeGroup("Modeling");
    this._stopPolling();
  }

  attributeChangedCallback(name, _old, val) {
    if (name !== "visibility") return;
    if (val === "open") this.open();
    else this.close();
  }

  closeImmediately() { this.style.visibility = "hidden"; }

  open() {
    this.style.visibility = "visible";
    this._attachEvents();
    this._refreshLayerCopy();
    this._loadProposals();
  }

  close() {
    this._stopPolling();
    this.closeImmediately();
  }

  // ── Element binding ──
  _bindElements() {
    const q = (s) => this.shadowRoot.querySelector(s);
    this.$layerSelect = q("#layerSelect");
    this.$closeBtn = q("#closeBtn");
    this.$proposalsTitle = q("#proposalsTitle");
    this.$proposalsBody = q("#proposalsBody");
    this.$proposalsEmpty = q("#proposalsEmpty");
    this.$proposalsEmptyText = q("#proposalsEmptyText");
    this.$newProposalCard = q("#newProposalCard");
    this.$newProposalBtn = q("#newProposalBtn");
    this.$refreshProposalsBtn = q("#refreshProposalsBtn");
    this.$submitProposalBtn = q("#submitProposalBtn");
    this.$cancelProposalBtn = q("#cancelProposalBtn");
    this.$newProposalTitle = q("#newProposalTitle");
    this.$newProposalHelp = q("#newProposalHelp");
    this.$silverProposalFields = q("#silverProposalFields");
    this.$goldProposalFields = q("#goldProposalFields");
    this.$propNodeId = q("#propNodeId");
    this.$propEndpoint = q("#propEndpoint");
    this.$propSilverProposalId = q("#propSilverProposalId");
    this.$filterStatus = q("#filterStatus");
    this.$filterLimit = q("#filterLimit");

    this.$detailEmpty = q("#detailEmpty");
    this.$detailContent = q("#detailContent");
    this.$detailTitle = q("#detailTitle");
    this.$detailMeta = q("#detailMeta");
    this.$detailActions = q("#detailActions");
    this.$detailStatus = q("#detailStatus");
    this.$backToList = q("#backToList");
    this.$schemaTitle = q("#schemaTitle");
    this.$schemaBody = q("#schemaBody");
    this.$addColumnBtn = q("#addColumnBtn");
    this.$saveSchemaBtn = q("#saveSchemaBtn");
    this.$mappingCard = q("#mappingCard");
    this.$mappingSubtitle = q("#mappingSubtitle");
    this.$mappingBody = q("#mappingBody");
    this.$compileCard = q("#compileCard");
    this.$compileInfo = q("#compileInfo");
    this.$viewSqlBtn = q("#viewSqlBtn");
    this.$validationCard = q("#validationCard");
    this.$validationResults = q("#validationResults");
    this.$pipelineSteps = q("#pipelineSteps");

    this.$reviewEmpty = q("#reviewEmpty");
    this.$reviewContent = q("#reviewContent");
    this.$reviewTitle = q("#reviewTitle");
    this.$reviewPipeline = q("#reviewPipeline");
    this.$reviewSchemaTitle = q("#reviewSchemaTitle");
    this.$reviewSchemaBody = q("#reviewSchemaBody");
    this.$reviewSqlPreview = q("#reviewSqlPreview");
    this.$reviewValidation = q("#reviewValidation");
    this.$reviewDecision = q("#reviewDecision");
    this.$reviewNotes = q("#reviewNotes");
    this.$submitReviewBtn = q("#submitReviewBtn");

    this.$releasesBody = q("#releasesBody");
    this.$releasesEmpty = q("#releasesEmpty");
    this.$refreshReleasesBtn = q("#refreshReleasesBtn");
    this.$executionCard = q("#executionCard");
    this.$execRunId = q("#execRunId");
    this.$execStatus = q("#execStatus");
    this.$execStarted = q("#execStarted");
    this.$execDuration = q("#execDuration");
    this.$execLog = q("#execLog");
    this.$pollRunBtn = q("#pollRunBtn");
    this.$stopPollBtn = q("#stopPollBtn");

    this.$sqlContent = q("#sqlContent");
    this.$copySqlBtn = q("#copySqlBtn");
  }

  // ── Events ──
  _attachEvents() {
    EventHandler.removeGroup("Modeling");
    const on = (el, ev, fn) => EventHandler.on(el, ev, fn, false, "Modeling");

    on(this.$closeBtn, "click", () => this.setAttribute("visibility", "close"));
    on(this.$layerSelect, "change", () => {
      this._currentProposal = null;
      this._compiledArtifact = null;
      this._refreshLayerCopy();
      this._switchTab("proposals");
      this._loadProposals();
    });

    // Tabs
    this.shadowRoot.querySelectorAll(".tabs .tab[data-tab]").forEach((t) => {
      on(t, "click", () => this._switchTab(t.dataset.tab));
    });
    this.shadowRoot.querySelectorAll(".tabs .tab[data-sqltab]").forEach((t) => {
      on(t, "click", () => this._switchSqlTab(t.dataset.sqltab));
    });

    // Proposals
    on(this.$newProposalBtn, "click", () => this._toggleNewProposalForm(true));
    on(this.$cancelProposalBtn, "click", () => this._toggleNewProposalForm(false));
    on(this.$submitProposalBtn, "click", () => this._createProposal());
    on(this.$refreshProposalsBtn, "click", () => this._loadProposals());
    on(this.$filterStatus, "change", () => this._loadProposals());
    on(this.$filterLimit, "change", () => this._loadProposals());

    // Detail
    on(this.$backToList, "click", () => this._switchTab("proposals"));
    on(this.$addColumnBtn, "click", () => this._addSchemaColumn());
    on(this.$saveSchemaBtn, "click", () => this._saveSchema());
    on(this.$viewSqlBtn, "click", () => this._switchTab("sql"));

    // Review
    on(this.$reviewDecision, "change", () => {
      this.$submitReviewBtn.disabled = !this.$reviewDecision.value;
    });
    on(this.$submitReviewBtn, "click", () => this._submitReview());

    // Releases
    on(this.$refreshReleasesBtn, "click", () => this._loadReleases());
    on(this.$pollRunBtn, "click", () => this._pollRun());
    on(this.$stopPollBtn, "click", () => this._stopPolling());

    // SQL
    on(this.$copySqlBtn, "click", () => this._copySql());
  }

  _currentLayer() {
    return this.$layerSelect?.value === "gold" ? "gold" : "silver";
  }

  _layerConfig(layer = this._currentLayer()) {
    if (layer === "gold") {
      return {
        layer: "gold",
        layerLabel: "Gold",
        sourceLabel: "Silver",
        listRoute: "/goldProposals",
        getRoute: "/goldProposals",
        proposeRoute: "/proposeGoldSchema",
        updateRoute: "/updateGoldProposal",
        compileRoute: "/compileGoldProposal",
        synthesizeRoute: "/synthesizeGoldGraph",
        validateRoute: "/validateGoldProposal",
        validateWarehouseRoute: "/validateGoldProposalWarehouse",
        reviewRoute: "/reviewGoldProposal",
        publishRoute: "/publishGoldProposal",
        executeRoute: "/executeGoldRelease",
        pollRoute: "/pollGoldModelRun",
        newProposalHelp: "Select an existing Silver proposal to generate a Gold mart proposal. The system will derive grouped dimensions and aggregate measures for human review.",
        emptyCopy: "Create a new proposal from an existing Silver proposal to get started.",
      };
    }
    return {
      layer: "silver",
      layerLabel: "Silver",
      sourceLabel: "Bronze",
      listRoute: "/silverProposals",
      getRoute: "/silverProposals",
      proposeRoute: "/proposeSilverSchema",
      updateRoute: "/updateSilverProposal",
      compileRoute: "/compileSilverProposal",
      synthesizeRoute: "/synthesizeSilverGraph",
      validateRoute: "/validateSilverProposal",
      validateWarehouseRoute: "/validateSilverProposalWarehouse",
      reviewRoute: "/reviewSilverProposal",
      publishRoute: "/publishSilverProposal",
      executeRoute: "/executeSilverRelease",
      pollRoute: "/pollSilverModelRun",
      newProposalHelp: "Select a source node and endpoint to generate a Silver schema proposal. The system will profile the Bronze data and propose a canonical Silver schema for human review.",
      emptyCopy: "Create a new proposal from a Bronze source node to get started.",
    };
  }

  _refreshLayerCopy() {
    const cfg = this._layerConfig();
    this.$proposalsTitle.textContent = `${cfg.layerLabel} Schema Proposals`;
    this.$newProposalTitle.textContent = `Create New ${cfg.layerLabel} Proposal`;
    this.$newProposalHelp.textContent = cfg.newProposalHelp;
    this.$proposalsEmptyText.textContent = cfg.emptyCopy;
    this.$schemaTitle.textContent = `${cfg.layerLabel} Schema`;
    this.$mappingSubtitle.textContent = `${cfg.sourceLabel} → ${cfg.layerLabel} field mapping with type coercions and transformations.`;
    this.$reviewSchemaTitle.textContent = `Proposed ${cfg.layerLabel} Schema`;
    this.$silverProposalFields.style.display = cfg.layer === "silver" ? "grid" : "none";
    this.$goldProposalFields.style.display = cfg.layer === "gold" ? "grid" : "none";
  }

  _proposalLayer(p = this._currentProposal) {
    return (p?.layer || p?.target_layer || p?.proposal?.layer || this._currentLayer() || "silver").toLowerCase();
  }

  _proposalConfig(p = this._currentProposal) {
    return this._layerConfig(this._proposalLayer(p));
  }

  _proposalSourceName(p) {
    return p?.source_endpoint_name
      || p?.endpoint_name
      || p?.proposal?.source_model
      || p?.proposal?.endpoint_name
      || "--";
  }

  _proposalCreatedAt(p) {
    return p?.created_at_utc || p?.created_at || p?.updated_at || null;
  }

  // ── Tab switching ──
  _switchTab(tab) {
    this.shadowRoot.querySelectorAll(".tabs .tab[data-tab]").forEach((t) => {
      t.classList.toggle("active", t.dataset.tab === tab);
    });
    this.shadowRoot.querySelectorAll(".tab-panel[data-panel]").forEach((p) => {
      p.classList.toggle("active", p.dataset.panel === tab);
    });
    if (tab === "releases") this._loadReleases();
    if (tab === "review") this._renderReview();
    if (tab === "sql") this._renderSqlTab();
  }

  _switchSqlTab(subtab) {
    this.shadowRoot.querySelectorAll(".tabs .tab[data-sqltab]").forEach((t) => {
      t.classList.toggle("active", t.dataset.sqltab === subtab);
    });
    this._renderSqlTab(subtab);
  }

  // ── Proposals List ──
  async _loadProposals() {
    try {
      const cfg = this._layerConfig();
      const params = new URLSearchParams();
      const status = this.$filterStatus.value;
      const limit = this.$filterLimit.value;
      if (status) params.set("status", status);
      if (limit) params.set("limit", limit);
      const gid = this._getGid();
      if (gid) params.set("gid", gid);

      const data = await request(`${cfg.listRoute}?${params.toString()}`);
      this._proposals = Array.isArray(data) ? data : (data?.proposals || []);
      this._renderProposals();
    } catch (e) {
      console.error("Load proposals error:", e);
      this._proposals = [];
      this._renderProposals();
    }
  }

  _renderProposals() {
    const tbody = this.$proposalsBody;
    tbody.innerHTML = "";

    if (!this._proposals.length) {
      this.$proposalsEmpty.style.display = "block";
      return;
    }
    this.$proposalsEmpty.style.display = "none";

    for (const p of this._proposals) {
      const layer = this._proposalLayer(p);
      const tr = document.createElement("tr");
      tr.classList.add("clickable");
      tr.innerHTML = `
        <td>${p.proposal_id || p.id || "--"}</td>
        <td>${p.source_node_id || p.api_node_id || "--"}</td>
        <td>${this._esc(this._proposalSourceName(p))}</td>
        <td>${this._esc(layer)}</td>
        <td><span class="badge ${badgeClass(p.status)}">${p.status || "draft"}</span></td>
        <td>${p.created_by || "--"}</td>
        <td>${fmtDate(this._proposalCreatedAt(p))}</td>
        <td class="proposal-actions"></td>
      `;
      const actionsCell = tr.querySelector(".proposal-actions");

      const openBtn = document.createElement("button");
      openBtn.className = "small secondary";
      openBtn.textContent = "Open";
      openBtn.addEventListener("click", (e) => { e.stopPropagation(); this._openProposal(p.proposal_id || p.id); });
      actionsCell.appendChild(openBtn);

      tr.addEventListener("click", () => this._openProposal(p.proposal_id || p.id));
      tbody.appendChild(tr);
    }
  }

  _toggleNewProposalForm(show) {
    this.$newProposalCard.style.display = show ? "block" : "none";
    if (show) {
      if (this._currentLayer() === "gold") this.$propSilverProposalId.focus();
      else this.$propNodeId.focus();
    }
  }

  async _createProposal() {
    const cfg = this._layerConfig();
    const nodeId = this.$propNodeId.value.trim();
    const endpoint = this.$propEndpoint.value.trim();
    const silverProposalId = this.$propSilverProposalId.value.trim();

    if (cfg.layer === "gold") {
      if (!silverProposalId) { alert("Silver Proposal ID is required."); return; }
    } else {
      if (!nodeId) { alert("Source Node ID is required."); return; }
      if (!endpoint) { alert("Endpoint name is required."); return; }
    }

    this.$submitProposalBtn.disabled = true;
    try {
      const gid = this._getGid();
      const body = cfg.layer === "gold"
        ? { silver_proposal_id: Number(silverProposalId) }
        : { id: Number(nodeId), endpoint_name: endpoint, gid };
      const result = await request(cfg.proposeRoute, { method: "POST", body });
      this._toggleNewProposalForm(false);
      this.$propNodeId.value = "";
      this.$propEndpoint.value = "";
      this.$propSilverProposalId.value = "";
      await this._loadProposals();
      const pid = result?.proposal_id || result?.id;
      if (pid) this._openProposal(pid);
    } catch (e) {
      alert("Failed to create proposal: " + (e.message || e));
    } finally {
      this.$submitProposalBtn.disabled = false;
    }
  }

  // ── Proposal Detail ──
  async _openProposal(proposalId) {
    const cfg = this._layerConfig();
    this._switchTab("detail");
    this.$detailEmpty.style.display = "none";
    this.$detailContent.style.display = "block";
    this._setDetailStatus("Loading proposal...", true);

    try {
      const p = await request(`${cfg.getRoute}/${proposalId}`);
      if (p?.layer && this.$layerSelect.value !== p.layer) {
        this.$layerSelect.value = p.layer;
        this._refreshLayerCopy();
      }
      this._currentProposal = p;
      this._schemaDirty = false;
      this._renderDetail();
      this._setDetailStatus("");

      // Try loading compile artifact
      if (["compiled", "validated", "approved", "published"].includes(p.status)) {
        this._loadCompileArtifact(proposalId);
      }
    } catch (e) {
      this._setDetailStatus("Failed to load proposal: " + (e.message || e));
    }
  }

  _renderDetail() {
    const p = this._currentProposal;
    if (!p) return;
    const cfg = this._proposalConfig(p);

    this.$detailTitle.textContent = `Proposal #${p.proposal_id || p.id} — ${this._proposalSourceName(p)}`;
    this.$detailMeta.textContent = `Layer: ${cfg.layerLabel} | Status: ${p.status || "draft"} | Source node: ${p.source_node_id || p.api_node_id || "--"} | Created: ${fmtDate(this._proposalCreatedAt(p))} | By: ${p.created_by || "--"}`;

    this._renderPipeline(this.$pipelineSteps, p.status);
    this._renderSchema(p);
    this._renderMapping(p);
    this._renderDetailActions(p);
    this._renderValidation(p);
  }

  _renderPipeline(container, status) {
    const steps = ["proposed", "compiled", "validated", "approved", "published"];
    const statusIdx = steps.indexOf((status || "").toLowerCase());
    container.querySelectorAll(".pipeline-step").forEach((el) => {
      const stepName = el.dataset.step;
      const stepMap = { propose: 0, edit: 0, compile: 1, validate: 2, review: 3, publish: 4, execute: 5 };
      const idx = stepMap[stepName] ?? -1;
      el.classList.remove("done", "current", "error");
      if (status === "rejected" && stepName === "review") el.classList.add("error");
      else if (idx < statusIdx) el.classList.add("done");
      else if (idx === statusIdx) el.classList.add("current");
    });
  }

  _renderSchema(p) {
    const proposal = p.proposal || p;
    const columns = proposal.columns || proposal.silver_columns || proposal.schema?.columns || [];
    const mergeKeys = new Set(proposal.materialization?.keys || []);
    this.$schemaBody.innerHTML = "";

    if (!columns.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = '<td colspan="7" style="text-align:center;color:#9ca3af;padding:20px;">No schema columns. Generate or add columns above.</td>';
      this.$schemaBody.appendChild(tr);
      return;
    }

    columns.forEach((col, idx) => {
      const columnName = col.target_column || col.column_name || col.name || "";
      const dataType = (col.type || col.data_type || "").toUpperCase();
      const nullable = col.nullable !== false && col.is_nullable !== "NO";
      const primaryKey = col.primary_key || col.role === "business_key" || mergeKeys.has(columnName);
      const expression = col.expression || col.source_expression || col.source_field || "";
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><input type="text" value="${this._esc(columnName)}" data-idx="${idx}" data-field="column_name" /></td>
        <td>
          <select data-idx="${idx}" data-field="data_type">
            ${SF_TYPES.map((t) => `<option value="${t}" ${dataType === t ? "selected" : ""}>${t}</option>`).join("")}
            ${SF_TYPES.includes(dataType) ? "" : `<option value="${dataType}" selected>${dataType}</option>`}
          </select>
        </td>
        <td><input type="checkbox" data-idx="${idx}" data-field="nullable" ${nullable ? "checked" : ""} /></td>
        <td><input type="checkbox" data-idx="${idx}" data-field="primary_key" ${primaryKey ? "checked" : ""} /></td>
        <td><input type="text" value="${this._esc(col.description || "")}" data-idx="${idx}" data-field="description" /></td>
        <td><input type="text" value="${this._esc(expression)}" data-idx="${idx}" data-field="source_expression" /></td>
        <td class="schema-row-actions">
          <button class="small danger" data-remove="${idx}" type="button">&times;</button>
        </td>
      `;
      // Listen for changes
      tr.querySelectorAll("input, select").forEach((el) => {
        el.addEventListener("input", () => this._markSchemaDirty());
        el.addEventListener("change", () => this._markSchemaDirty());
      });
      tr.querySelector(`[data-remove="${idx}"]`).addEventListener("click", () => {
        tr.remove();
        this._markSchemaDirty();
      });
      this.$schemaBody.appendChild(tr);
    });
  }

  _addSchemaColumn() {
    const idx = this.$schemaBody.children.length;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="text" value="" data-idx="${idx}" data-field="column_name" placeholder="new_column" /></td>
      <td>
        <select data-idx="${idx}" data-field="data_type">
          ${SF_TYPES.map((t) => `<option value="${t}">${t}</option>`).join("")}
        </select>
      </td>
      <td><input type="checkbox" data-idx="${idx}" data-field="nullable" checked /></td>
      <td><input type="checkbox" data-idx="${idx}" data-field="primary_key" /></td>
      <td><input type="text" value="" data-idx="${idx}" data-field="description" /></td>
      <td><input type="text" value="" data-idx="${idx}" data-field="source_expression" /></td>
      <td class="schema-row-actions">
        <button class="small danger" data-remove="${idx}" type="button">&times;</button>
      </td>
    `;
    tr.querySelectorAll("input, select").forEach((el) => {
      el.addEventListener("input", () => this._markSchemaDirty());
      el.addEventListener("change", () => this._markSchemaDirty());
    });
    tr.querySelector(`[data-remove="${idx}"]`).addEventListener("click", () => {
      tr.remove();
      this._markSchemaDirty();
    });
    this.$schemaBody.appendChild(tr);
    this._markSchemaDirty();
    tr.querySelector("input").focus();
  }

  _markSchemaDirty() {
    this._schemaDirty = true;
    this.$saveSchemaBtn.disabled = false;
  }

  _collectSchemaFromTable() {
    const rows = this.$schemaBody.querySelectorAll("tr");
    const proposal = this._currentProposal?.proposal || this._currentProposal || {};
    const existingColumns = proposal.columns || [];
    const columns = [];
    rows.forEach((tr, idx) => {
      const nameEl = tr.querySelector('[data-field="column_name"]');
      if (!nameEl) return;
      const name = nameEl.value.trim();
      if (!name) return;
      const prior = existingColumns[idx] || {};
      const expression = tr.querySelector('[data-field="source_expression"]')?.value || "";
      columns.push({
        ...prior,
        target_column: name,
        type: tr.querySelector('[data-field="data_type"]')?.value || "VARCHAR",
        nullable: tr.querySelector('[data-field="nullable"]')?.checked ?? true,
        role: tr.querySelector('[data-field="primary_key"]')?.checked ? "business_key" : (prior.role || "attribute"),
        description: tr.querySelector('[data-field="description"]')?.value || "",
        expression,
      });
    });
    return columns;
  }

  async _saveSchema() {
    const p = this._currentProposal;
    if (!p) return;
    const cfg = this._proposalConfig(p);
    const columns = this._collectSchemaFromTable();
    this.$saveSchemaBtn.disabled = true;
    this._setDetailStatus("Saving schema changes...", true);

    try {
      const proposal = { ...(p.proposal || {}), columns };
      const result = await request(cfg.updateRoute, {
        method: "POST",
        body: { proposal_id: p.proposal_id || p.id, proposal },
      });
      this._currentProposal = result || { ...p, proposal, status: p.status };
      this._schemaDirty = false;
      this._setDetailStatus("Schema saved successfully.");
    } catch (e) {
      this._setDetailStatus("Save failed: " + (e.message || e));
      this.$saveSchemaBtn.disabled = false;
    }
  }

  _renderMapping(p) {
    const proposal = p.proposal || p;
    const mappings = proposal.mappings || proposal.field_mappings || [];
    if (!mappings.length) {
      this.$mappingCard.style.display = "none";
      return;
    }
    this.$mappingCard.style.display = "block";
    this.$mappingBody.innerHTML = "";
    mappings.forEach((m) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${this._esc(m.source_field || m.bronze_field || (m.source_columns || []).join(", ") || "")}</td>
        <td>${this._esc(m.target_column || m.silver_column || "")}</td>
        <td>${this._esc(m.transform || m.expression || "--")}</td>
        <td>${this._esc(m.cast || m.type_cast || "--")}</td>
      `;
      this.$mappingBody.appendChild(tr);
    });
  }

  _renderDetailActions(p) {
    this.$detailActions.innerHTML = "";
    const status = (p.status || "").toLowerCase();
    const pid = p.proposal_id || p.id;
    const cfg = this._proposalConfig(p);

    const actions = [];

    if (["draft", "proposed", "changes_requested"].includes(status)) {
      actions.push({ label: "Compile", cls: "primary-blue", fn: () => this._compileProposal(pid) });
      actions.push({ label: "Synthesize Graph", cls: "secondary", fn: () => this._synthesizeGraph(pid) });
    }
    if (["compiled"].includes(status)) {
      actions.push({ label: "Validate", cls: "primary-blue", fn: () => this._validateProposal(pid) });
      actions.push({ label: "Warehouse Validate", cls: "secondary", fn: () => this._validateWarehouse(pid) });
    }
    if (["validated", "compiled"].includes(status)) {
      actions.push({ label: "Go to Review", cls: "warning", fn: () => this._switchTab("review") });
    }
    if (["approved"].includes(status)) {
      actions.push({ label: "Publish Release", cls: "success", fn: () => this._publishProposal(pid) });
    }
    if (["published"].includes(status)) {
      if (p.active_release?.release_id) {
        actions.push({ label: "Execute Release", cls: "success", fn: () => this._executeRelease(p.active_release.release_id, cfg) });
      }
      actions.push({ label: "View Releases", cls: "secondary", fn: () => this._switchTab("releases") });
    }

    for (const a of actions) {
      const btn = document.createElement("button");
      btn.className = `small ${a.cls}`;
      btn.textContent = a.label;
      btn.type = "button";
      btn.addEventListener("click", a.fn);
      this.$detailActions.appendChild(btn);
    }
  }

  _renderValidation(p) {
    const val = p.validation || p.validation_results || p.latest_validation?.validation;
    if (!val) {
      this.$validationCard.style.display = "none";
      return;
    }
    this.$validationCard.style.display = "block";
    const results = val.results || val.checks || (Array.isArray(val) ? val : []);
    if (!results.length) {
      this.$validationResults.innerHTML = '<p style="color:#6b7280;font-size:13px;">No validation results available.</p>';
      return;
    }
    let html = '<table style="font-size:12px;"><thead><tr><th>Check</th><th>Result</th><th>Message</th></tr></thead><tbody>';
    for (const r of results) {
      const pass = r.passed || r.result === "pass" || r.status === "pass";
      html += `<tr>
        <td>${this._esc(r.check || r.name || "")}</td>
        <td><span class="badge ${pass ? "badge-validated" : "badge-failed"}">${pass ? "PASS" : "FAIL"}</span></td>
        <td>${this._esc(r.message || r.detail || "")}</td>
      </tr>`;
    }
    html += "</tbody></table>";
    this.$validationResults.innerHTML = html;
  }

  async _compileProposal(pid) {
    const cfg = this._proposalConfig();
    this._setDetailStatus("Compiling proposal...", true);
    try {
      const result = await request(cfg.compileRoute, { method: "POST", body: { proposal_id: pid } });
      this._compiledArtifact = result;
      this._setDetailStatus("Compilation complete.");
      await this._openProposal(pid);
    } catch (e) {
      this._setDetailStatus("Compile failed: " + (e.message || e));
    }
  }

  async _synthesizeGraph(pid) {
    const cfg = this._proposalConfig();
    this._setDetailStatus("Synthesizing graph...", true);
    try {
      await request(cfg.synthesizeRoute, { method: "POST", body: { proposal_id: pid } });
      this._setDetailStatus("Graph synthesized.");
      await this._openProposal(pid);
    } catch (e) {
      this._setDetailStatus("Synthesize failed: " + (e.message || e));
    }
  }

  async _validateProposal(pid) {
    const cfg = this._proposalConfig();
    this._setDetailStatus("Running validation...", true);
    try {
      const result = await request(cfg.validateRoute, { method: "POST", body: { proposal_id: pid } });
      this._setDetailStatus("Validation complete.");
      await this._openProposal(pid);
    } catch (e) {
      this._setDetailStatus("Validation failed: " + (e.message || e));
    }
  }

  async _validateWarehouse(pid) {
    const cfg = this._proposalConfig();
    this._setDetailStatus("Running warehouse validation...", true);
    try {
      await request(cfg.validateWarehouseRoute, { method: "POST", body: { proposal_id: pid } });
      this._setDetailStatus("Warehouse validation complete.");
      await this._openProposal(pid);
    } catch (e) {
      this._setDetailStatus("Warehouse validation failed: " + (e.message || e));
    }
  }

  async _publishProposal(pid) {
    const cfg = this._proposalConfig();
    const confirmed = await customConfirm("Publish this proposal as a release? This will make it available for execution.");
    if (!confirmed) return;
    this._setDetailStatus("Publishing...", true);
    try {
      const result = await request(cfg.publishRoute, { method: "POST", body: { proposal_id: pid } });
      this._setDetailStatus("Published successfully. Release ID: " + (result?.release_id || "--"));
      await this._openProposal(pid);
    } catch (e) {
      this._setDetailStatus("Publish failed: " + (e.message || e));
    }
  }

  async _loadCompileArtifact(pid) {
    // The compile endpoint returns the artifact; re-compile to get it if not cached
    try {
      const cfg = this._proposalConfig();
      const result = await request(cfg.compileRoute, { method: "POST", body: { proposal_id: pid } });
      this._compiledArtifact = result;
      this._renderCompileCard(result);
    } catch (_) {
      // Silently ignore - artifact may not be available
    }
  }

  _renderCompileCard(artifact) {
    if (!artifact) { this.$compileCard.style.display = "none"; return; }
    this.$compileCard.style.display = "block";
    const sql = artifact.sql || artifact.compiled_sql || artifact.artifact?.sql || "";
    const ddl = artifact.ddl || artifact.artifact?.ddl || "";
    const target = artifact.target_table || artifact.artifact?.target_table || "";
    this.$compileInfo.innerHTML = `
      <p><strong>Target table:</strong> ${this._esc(target)}</p>
      ${ddl ? '<p><strong>DDL present:</strong> Yes</p>' : ""}
      ${sql ? `<p><strong>SQL length:</strong> ${sql.length} chars</p>` : ""}
    `;
  }

  // ── Review Tab ──
  _renderReview() {
    const p = this._currentProposal;
    if (!p) { this.$reviewEmpty.style.display = "block"; this.$reviewContent.style.display = "none"; return; }
    this.$reviewEmpty.style.display = "none";
    this.$reviewContent.style.display = "block";
    const cfg = this._proposalConfig(p);

    this.$reviewTitle.textContent = `Proposal #${p.proposal_id || p.id} — ${this._proposalSourceName(p)}`;

    // Pipeline
    this.$reviewPipeline.innerHTML = "";
    const steps = ["Propose", "Compile", "Validate", "Review", "Publish"];
    const statusOrder = ["proposed", "compiled", "validated", "approved", "published"];
    const currentIdx = statusOrder.indexOf((p.status || "").toLowerCase());
    steps.forEach((label, i) => {
      const div = document.createElement("div");
      div.className = "pipeline-step" + (i < currentIdx ? " done" : i === currentIdx ? " current" : "");
      div.innerHTML = `<span class="step-num">${i + 1}</span> ${label}`;
      this.$reviewPipeline.appendChild(div);
    });

    // Schema summary
    const proposal = p.proposal || p;
    const columns = proposal.columns || proposal.silver_columns || [];
    this.$reviewSchemaBody.innerHTML = "";
    columns.forEach((col) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${this._esc(col.target_column || col.column_name || col.name || "")}</td>
        <td>${this._esc(col.type || col.data_type || "")}</td>
        <td>${col.primary_key || col.role === "business_key" ? "YES" : ""}</td>
        <td>${col.nullable !== false ? "YES" : "NO"}</td>
      `;
      this.$reviewSchemaBody.appendChild(tr);
    });

    // SQL
    const artifact = this._compiledArtifact;
    const sql = artifact?.sql || artifact?.compiled_sql || artifact?.artifact?.sql || "";
    this.$reviewSqlPreview.innerHTML = sql ? highlightSQL(sql) : "No SQL compiled yet. Compile the proposal first.";

    // Validation
    const val = p.validation || p.validation_results || p.latest_validation?.validation;
    if (val) {
      const checks = val.results || val.checks || (Array.isArray(val) ? val : []);
      const passed = checks.filter((c) => c.passed || c.result === "pass").length;
      const total = checks.length;
      this.$reviewValidation.innerHTML = `
        <div class="card" style="margin:0;">
          <h4>Validation: ${passed}/${total} checks passed</h4>
          ${checks.filter((c) => !(c.passed || c.result === "pass")).map((c) =>
            `<p style="color:#dc2626;font-size:12px;">FAIL: ${this._esc(c.check || c.name || "")} — ${this._esc(c.message || "")}</p>`
          ).join("")}
        </div>
      `;
    } else {
      this.$reviewValidation.innerHTML = '<p style="font-size:13px;color:#6b7280;">No validation results. Validate the proposal before review.</p>';
    }

    // Reset decision
    this.$reviewDecision.value = "";
    this.$reviewNotes.value = "";
    this.$submitReviewBtn.disabled = true;
  }

  async _submitReview() {
    const p = this._currentProposal;
    if (!p) return;
    const cfg = this._proposalConfig(p);
    const decision = this.$reviewDecision.value;
    const notes = this.$reviewNotes.value.trim();

    if (!decision) { alert("Select a review decision."); return; }
    if (decision !== "approved" && !notes) { alert("Notes are required for rejection or change requests."); return; }

    this.$submitReviewBtn.disabled = true;
    try {
      await request(cfg.reviewRoute, {
        method: "POST",
        body: {
          proposal_id: p.proposal_id || p.id,
          review_state: decision,
          review_notes: notes,
        },
      });
      alert(`Review submitted: ${decision}`);
      await this._openProposal(p.proposal_id || p.id);
      this._renderReview();
    } catch (e) {
      alert("Review failed: " + (e.message || e));
      this.$submitReviewBtn.disabled = false;
    }
  }

  // ── Releases Tab ──
  async _loadReleases() {
    try {
      const cfg = this._layerConfig();
      const params = new URLSearchParams();
      const gid = this._getGid();
      if (gid) params.set("gid", gid);
      params.set("status", "published");
      const data = await request(`${cfg.listRoute}?${params.toString()}`);
      const proposals = Array.isArray(data) ? data : (data?.proposals || []);
      this._renderReleases(proposals.filter((p) => p.status === "published" || p.release_id));
    } catch (e) {
      console.error("Load releases error:", e);
      this._renderReleases([]);
    }
  }

  _renderReleases(releases) {
    this.$releasesBody.innerHTML = "";
    if (!releases.length) {
      this.$releasesEmpty.style.display = "block";
      return;
    }
    this.$releasesEmpty.style.display = "none";

    for (const r of releases) {
      const layer = this._proposalLayer(r);
      const tr = document.createElement("tr");
      tr.classList.add("clickable");
      tr.innerHTML = `
        <td>${r.release_id || "--"}</td>
        <td>#${r.proposal_id || r.id || "--"}</td>
        <td>${this._esc(this._proposalSourceName(r))}</td>
        <td>${this._esc(layer)}</td>
        <td>${fmtDate(r.published_at || this._proposalCreatedAt(r))}</td>
        <td><span class="badge ${badgeClass(r.run_status || "published")}">${r.run_status || "published"}</span></td>
        <td class="release-actions"></td>
      `;
      const actionsCell = tr.querySelector(".release-actions");

      if (r.release_id) {
        const execBtn = document.createElement("button");
        execBtn.className = "small success";
        execBtn.textContent = "Execute";
        execBtn.type = "button";
        execBtn.addEventListener("click", (e) => { e.stopPropagation(); this._executeRelease(r.release_id, this._proposalConfig(r)); });
        actionsCell.appendChild(execBtn);
      }

      const viewBtn = document.createElement("button");
      viewBtn.className = "small secondary";
      viewBtn.textContent = "View";
      viewBtn.type = "button";
      viewBtn.addEventListener("click", (e) => { e.stopPropagation(); this._openProposal(r.proposal_id || r.id); });
      actionsCell.appendChild(viewBtn);

      this.$releasesBody.appendChild(tr);
    }
  }

  async _executeRelease(releaseId, cfg = this._proposalConfig()) {
    const confirmed = await customConfirm(`Execute release #${releaseId}? This will run the compiled SQL against the target warehouse.`);
    if (!confirmed) return;

    this.$executionCard.style.display = "block";
    this.$execRunId.textContent = "Starting...";
    this.$execStatus.innerHTML = '<span class="badge badge-running">STARTING</span>';
    this.$execLog.textContent = "Submitting execution request...\n";

    try {
      const result = await request(cfg.executeRoute, {
        method: "POST",
        body: { release_id: releaseId },
      });
      this._currentRunId = result?.model_run_id || result?.run_id;
      this.$execRunId.textContent = this._currentRunId || "--";
      this.$execStatus.innerHTML = '<span class="badge badge-running">RUNNING</span>';
      this.$execLog.textContent += `Execution started. Run ID: ${this._currentRunId}\n`;

      // Start auto-polling
      this._startPolling();
    } catch (e) {
      this.$execStatus.innerHTML = '<span class="badge badge-failed">FAILED</span>';
      this.$execLog.textContent += `Error: ${e.message || e}\n`;
    }
  }

  async _pollRun() {
    if (!this._currentRunId) { alert("No active run to poll."); return; }
    try {
      const result = await request(this._proposalConfig().pollRoute, {
        method: "POST",
        body: { model_run_id: this._currentRunId },
      });
      const status = result?.status || result?.run_status || "unknown";
      this.$execStatus.innerHTML = `<span class="badge ${badgeClass(status)}">${status.toUpperCase()}</span>`;
      this.$execStarted.textContent = fmtDate(result?.started_at);
      this.$execDuration.textContent = result?.duration || result?.elapsed || "--";

      const log = result?.log || result?.output || result?.message || "";
      if (log) this.$execLog.textContent += log + "\n";

      if (["completed", "failed", "error", "cancelled"].includes(status.toLowerCase())) {
        this._stopPolling();
        this.$execLog.textContent += `\n--- Run ${status} ---\n`;
      }
    } catch (e) {
      this.$execLog.textContent += `Poll error: ${e.message || e}\n`;
    }
  }

  _startPolling() {
    this._stopPolling();
    this.$stopPollBtn.style.display = "inline-block";
    this._pollTimer = setInterval(() => this._pollRun(), 5000);
    this._pollRun(); // immediate first poll
  }

  _stopPolling() {
    if (this._pollTimer) { clearInterval(this._pollTimer); this._pollTimer = null; }
    this.$stopPollBtn.style.display = "none";
  }

  // ── SQL Tab ──
  _renderSqlTab(subtab) {
    const active = subtab || this.shadowRoot.querySelector(".tab[data-sqltab].active")?.dataset.sqltab || "ddl";
    const artifact = this._compiledArtifact;
    if (!artifact) {
      this.$sqlContent.innerHTML = "-- No compiled artifact available. Open a proposal and compile it first.";
      return;
    }

    let sql = "";
    switch (active) {
      case "ddl":
        sql = artifact.ddl || artifact.artifact?.ddl || "-- No DDL available";
        break;
      case "dml":
        sql = artifact.sql || artifact.compiled_sql || artifact.artifact?.sql || "-- No DML available";
        break;
      case "full":
        sql = [
          artifact.ddl || artifact.artifact?.ddl || "",
          "",
          artifact.sql || artifact.compiled_sql || artifact.artifact?.sql || "",
        ].join("\n").trim() || "-- No artifact available";
        break;
    }
    this.$sqlContent.innerHTML = highlightSQL(sql);
  }

  _copySql() {
    const text = this.$sqlContent.textContent || "";
    navigator.clipboard.writeText(text).then(
      () => alert("SQL copied to clipboard."),
      () => alert("Failed to copy.")
    );
  }

  // ── Utilities ──
  _getGid() {
    // Try to get graph ID from session / panelItems
    const items = window.data?.panelItems || [];
    if (items.length && items[0]?.id) return items[0].id;
    return null;
  }

  _setDetailStatus(msg, loading = false) {
    this.$detailStatus.innerHTML = (loading ? '<span class="spinner"></span>' : "") + (msg ? `<span>${this._esc(msg)}</span>` : "");
  }

  _esc(s) {
    if (!s) return "";
    const d = document.createElement("div");
    d.textContent = String(s);
    return d.innerHTML;
  }
}

customElements.define("modeling-console", ModelingConsole);
