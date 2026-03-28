import EventHandler from "./library/eventHandler.js";
import { request, customConfirm, getPanelItems } from "./library/utils.js";
import { aiAssistCSS, renderAiLoading, renderAiCard, renderAiEditsCard, flattenEdits, bindAiEditActions, callAiEndpoint } from "./aiAssistCard.js";

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
  .badge-warning { background: #fef3c7; color: #92400e; }
  .mapping-expression-note { display: block; margin-top: 4px; color: #6b7280; font-size: 11px; font-family: "SFMono-Regular", Consolas, monospace; }
  .mapping-action-group { display: flex; gap: 6px; }
  .expr-modal {
    position: fixed; inset: 0; background: rgba(17, 24, 39, 0.45); z-index: 160;
    display: none; align-items: center; justify-content: center; padding: 24px;
  }
  .expr-modal.open { display: flex; }
  .expr-dialog {
    width: min(1100px, 100%); max-height: min(88vh, 900px); overflow: hidden;
    background: #fff; border: 1px solid rgba(0,0,0,0.14); box-shadow: 0 20px 60px rgba(0,0,0,0.2);
    display: flex; flex-direction: column;
  }
  .expr-dialog-head {
    display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;
    padding: 16px 18px 12px 18px; border-bottom: 1px solid #e5e7eb; background: #f9fafb;
  }
  .expr-dialog-head p { margin: 4px 0 0 0; color: #6b7280; font-size: 12px; }
  .expr-dialog-body { padding: 0; overflow: auto; }
  .expr-dialog-body expression-component { display: block; }

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
  .field-help { margin-top: 4px; font-size: 12px; color: #6b7280; }

  /* AI Assist (shared) */
  ${aiAssistCSS}
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
          <select id="propNodeId">
            <option value="">Select a Bronze source node</option>
          </select>
          <div id="propNodeHelp" class="field-help">Options come from the open graph and include node ids.</div>
        </div>
        <div>
          <label for="propEndpoint">Endpoint Name</label>
          <select id="propEndpoint" disabled>
            <option value="">Select an endpoint</option>
          </select>
          <div id="propEndpointHelp" class="field-help">Select a source node first.</div>
        </div>
      </div>
      <div class="grid" id="goldProposalFields" style="display:none;">
        <div>
          <label for="propSilverProposalId">Silver Proposal</label>
          <select id="propSilverProposalId">
            <option value="">Select a Silver proposal</option>
          </select>
          <div id="propSilverHelp" class="field-help">Published or approved Silver proposals for this graph.</div>
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

      <div class="card" id="processingPolicyCard">
        <div class="flex-between">
          <div>
            <h3>Processing Policy</h3>
            <p style="font-size:13px;color:#6b7280;margin:4px 0 0 0;">Configure business keys, ordering, and reprocessing for late or out-of-order data.</p>
          </div>
        </div>
        <div class="grid" style="margin-top:12px;">
          <div>
            <label for="policyBusinessKeys">Business Keys</label>
            <input id="policyBusinessKeys" type="text" placeholder="trip_id, vehicle_id" />
            <div class="field-help">Comma-separated target columns used for latest-state dedupe and merge semantics.</div>
          </div>
          <div>
            <label for="policyOrderingStrategy">Ordering Strategy</label>
            <select id="policyOrderingStrategy">
              <option value="">None</option>
              <option value="latest_event_time_wins">Latest event time wins</option>
              <option value="latest_sequence_wins">Latest sequence wins</option>
              <option value="event_time_then_sequence">Event time, then sequence</option>
              <option value="append_only">Append only</option>
            </select>
          </div>
          <div>
            <label for="policyEventTime">Event Time Column</label>
            <select id="policyEventTime">
              <option value="">None</option>
            </select>
          </div>
          <div>
            <label for="policySequence">Sequence Column</label>
            <select id="policySequence">
              <option value="">None</option>
            </select>
          </div>
          <div>
            <label for="policyLateMode">Late Data Mode</label>
            <select id="policyLateMode">
              <option value="">Default</option>
              <option value="merge">Merge</option>
              <option value="append">Append</option>
            </select>
          </div>
          <div>
            <label for="policyTooLate">Too-Late Behavior</label>
            <select id="policyTooLate">
              <option value="">Default</option>
              <option value="accept">Accept</option>
              <option value="quarantine">Quarantine</option>
              <option value="drop">Drop</option>
            </select>
          </div>
        </div>
        <div class="grid" style="margin-top:12px;">
          <div>
            <label for="policyLateToleranceValue">Late Data Tolerance</label>
            <div style="display:flex; gap:8px;">
              <input id="policyLateToleranceValue" type="number" min="0" placeholder="10" />
              <select id="policyLateToleranceUnit">
                <option value="">Unit</option>
                <option value="minutes">minutes</option>
                <option value="hours">hours</option>
                <option value="days">days</option>
              </select>
            </div>
          </div>
          <div>
            <label for="policyReprocessValue">Reprocess Window</label>
            <div style="display:flex; gap:8px;">
              <input id="policyReprocessValue" type="number" min="0" placeholder="24" />
              <select id="policyReprocessUnit">
                <option value="">Unit</option>
                <option value="minutes">minutes</option>
                <option value="hours">hours</option>
                <option value="days">days</option>
              </select>
            </div>
          </div>
        </div>
      </div>

      <!-- Mapping / transform info -->
      <div class="card" id="mappingCard" style="display:none;">
        <h3>Transformation Mapping</h3>
        <p id="mappingSubtitle" style="font-size:13px;color:#6b7280;margin:0 0 8px 0;">Bronze → Silver field mapping with type coercions and transformations.</p>
        <table id="mappingTable">
          <thead>
            <tr><th>Source Field</th><th>Target Column</th><th>Transform</th><th>Cast</th><th></th></tr>
          </thead>
          <tbody id="mappingBody"></tbody>
        </table>
        <div style="margin-top:10px;border-top:1px solid #e5e7eb;padding-top:10px;">
          <button class="ai-trigger" id="aiSuggestTransformsBtn" type="button">
            <span class="ai-icon">&#9733;</span> Suggest transforms with AI
          </button>
          <div id="aiTransformsResult"></div>
        </div>
      </div>

      <!-- BRD Intake -->
      <div class="card" id="brdCard" style="display:none;">
        <h3>Business Requirements (BRD)</h3>
        <p style="font-size:13px;color:#6b7280;margin:0 0 8px 0;">Paste a BRD or describe desired model fields, grain, and business logic. AI will generate a proposal from your requirements.</p>
        <textarea id="brdText" rows="6" placeholder="Describe your requirements: business entity, required fields, grain, SLAs..."></textarea>
        <div style="margin-top:8px;display:flex;gap:8px;">
          <button class="ai-trigger" id="aiGenerateFromBrdBtn" type="button">
            <span class="ai-icon">&#9733;</span> Generate proposal from BRD
          </button>
        </div>
        <div id="aiBrdResult"></div>
      </div>

      <!-- Gold Mart Suggestions -->
      <div class="card" id="goldMartCard" style="display:none;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <h3>Mart Design</h3>
          <button class="ai-trigger" id="aiSuggestGoldMartBtn" type="button">
            <span class="ai-icon">&#9733;</span> Suggest mart improvements
          </button>
        </div>
        <div id="aiGoldMartResult"></div>
      </div>

      <div class="card" id="metricGlossaryCard" style="display:none;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <h3>Metric Glossary</h3>
          <button class="ai-trigger" id="aiGenerateMetricGlossaryBtn" type="button">
            <span class="ai-icon">&#9733;</span> Generate metric definitions
          </button>
        </div>
        <div id="aiMetricGlossaryResult"></div>
      </div>

      <div class="card" id="anomalyCard" style="display:none;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <h3>Run / KPI Analysis</h3>
          <button class="ai-trigger" id="aiExplainAnomalyBtn" type="button">
            <span class="ai-icon">&#9733;</span> Why did this change?
          </button>
        </div>
        <div id="aiAnomalyResult"></div>
      </div>

      <!-- Compile output -->
      <div class="card" id="compileCard" style="display:none;">
        <div class="flex-between">
          <h3>Compiled Artifact</h3>
          <button id="viewSqlBtn" class="small secondary" type="button">View Full SQL</button>
        </div>
        <div id="compileInfo" style="font-size:13px;color:#374151;"></div>
        <div style="margin-top:10px;border-top:1px solid #e5e7eb;padding-top:10px;">
          <button class="ai-trigger" id="aiExplainProposalBtn" type="button">
            <span class="ai-icon">&#9733;</span> Explain proposal with AI
          </button>
          <div id="aiProposalResult"></div>
        </div>
      </div>

      <!-- Validation results -->
      <div class="card" id="validationCard" style="display:none;">
        <h3>Validation Results</h3>
        <div id="validationResults"></div>
        <div style="margin-top:10px;border-top:1px solid #e5e7eb;padding-top:10px;">
          <button class="ai-trigger" id="aiExplainValidationBtn" type="button">
            <span class="ai-icon">&#9733;</span> Explain validation with AI
          </button>
          <div id="aiValidationResult"></div>
        </div>
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
        <div><label>Rows Affected</label><span id="execRowCount" style="font-size:13px;">--</span></div>
        <div><label>Backend</label><span id="execBackend" style="font-size:13px;">--</span></div>
      </div>
      <div id="execLog" style="font-family:monospace; font-size:12px; background:#1e1e2e; color:#cdd6f4; padding:12px; max-height:200px; overflow-y:auto; white-space:pre-wrap;"></div>
      <div style="margin-top:8px; display:flex; gap:8px;">
        <button id="pollRunBtn" class="small secondary" type="button">Poll Status</button>
        <button id="stopPollBtn" class="small danger" type="button" style="display:none;">Stop Polling</button>
        <button id="previewDataBtn" class="small secondary" type="button" style="display:none;">Preview Data</button>
      </div>
      <div id="execPreview" style="display:none; margin-top:12px; overflow-x:auto;"></div>
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

  <div id="expressionModal" class="expr-modal">
    <div class="expr-dialog">
      <div class="expr-dialog-head">
        <div>
          <h3 id="expressionModalTitle">Expression Editor</h3>
          <p id="expressionModalHint">Use functions, columns, operators, and freeform SQL expressions for this mapping.</p>
        </div>
        <div class="actions">
          <button id="expressionCancelBtn" class="small secondary" type="button">Cancel</button>
          <button id="expressionSaveBtn" class="small primary-blue" type="button">Apply Expression</button>
        </div>
      </div>
      <div class="expr-dialog-body" id="expressionMount"></div>
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
    rejected: "badge-rejected", running: "badge-running", submitted: "badge-running",
    pending: "badge-running", completed: "badge-completed", succeeded: "badge-completed",
    failed: "badge-failed", timed_out: "badge-failed", cancelled: "badge-failed",
    changes_requested: "badge-warning",
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

const SOURCE_NODE_TYPES = new Set(["Ap", "Kf", "Fs"]);

function sourceConfigKey(sourceNode) {
  switch (sourceNode?.btype) {
    case "Ap": return "endpoint_configs";
    case "Kf": return "topic_configs";
    case "Fs": return "file_configs";
    default: return null;
  }
}

function sourceTypeLabel(sourceNode) {
  switch (sourceNode?.btype) {
    case "Ap": return "API";
    case "Kf": return "Kafka";
    case "Fs": return "File";
    default: return "Source";
  }
}

function displaySourceName(sourceNode) {
  return sourceNode?.api_name
    || sourceNode?.source_system
    || sourceNode?.business_name
    || sourceNode?.alias
    || sourceNode?.name
    || sourceTypeLabel(sourceNode);
}

function sourceOptionLabel(sourceNode) {
  return `#${sourceNode.id} ${sourceTypeLabel(sourceNode)} ${displaySourceName(sourceNode)}`.trim();
}

function enabledSourceConfigs(sourceNode) {
  const configKey = sourceConfigKey(sourceNode);
  if (!configKey) return [];
  return (Array.isArray(sourceNode?.[configKey]) ? sourceNode[configKey] : [])
    .filter((cfg) => cfg && cfg.enabled !== false)
    .map((cfg) => ({
      ...cfg,
      endpoint_name: cfg.endpoint_name || cfg.topic_name || cfg.path || cfg.file_pattern || "",
    }))
    .filter((cfg) => cfg.endpoint_name);
}

function normalizeTransformType(type) {
  return String(type || "").trim().toUpperCase();
}

function parseTransformItem(text) {
  const raw = String(text || "").trim();
  const match = raw.match(/^([A-Z_]+)\s*\((.*)\)$/i);
  if (!match) return { type: normalizeTransformType(raw), params: [] };
  const type = normalizeTransformType(match[1]);
  const paramText = String(match[2] || "").trim();
  if (!paramText || /^no parameters$/i.test(paramText)) return { type, params: [] };
  return {
    type,
    params: paramText.split(",").map((part) => part.trim()).filter(Boolean),
  };
}

function quoteSqlIdentifier(value) {
  return `"${String(value || "").replace(/"/g, '""')}"`;
}

function isPostgresqlProposal(proposal) {
  return String(proposal?.target_warehouse || "").trim().toLowerCase() === "postgresql";
}

function sourceReferenceMatches(expression) {
  return Array.from(String(expression || "").matchAll(/(?:bronze|silver)\.(?:"((?:[^"]|"")*)"|([A-Za-z_][A-Za-z0-9_]*))/g));
}

function buildExpressionFromTransforms(baseExpression, transforms) {
  let expression = String(baseExpression || "").trim();
  if (!expression) throw new Error("A base source expression is required before transforms can be applied.");

  for (const transformText of Array.isArray(transforms) ? transforms : []) {
    const { type, params } = parseTransformItem(transformText);
    switch (type) {
      case "TRIM":
        expression = `TRIM(${expression})`;
        break;
      case "TO_DATE":
        expression = `CAST(${expression} AS DATE)`;
        break;
      case "TO_VARCHAR":
      case "TO_STRING":
      case "TOSTRING":
        expression = `CAST(${expression} AS VARCHAR)`;
        break;
      case "UPPER":
      case "UPPERCASE":
        expression = `UPPER(${expression})`;
        break;
      case "LOWER":
      case "LOWERCASE":
        expression = `LOWER(${expression})`;
        break;
      case "SUBSTRING": {
        if (params.length < 1 || params.length > 2 || params.some((param) => !/^\d+$/.test(param))) {
          throw new Error("SUBSTRING requires one or two numeric parameters.");
        }
        expression = `SUBSTRING(${expression}, ${params.join(", ")})`;
        break;
      }
      default:
        throw new Error(`Unsupported transform type: ${type}`);
    }
  }

  return expression;
}

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
    this._pollCount = 0;
    this._currentRunId = null;
    this._currentRequestId = null;
    this._activePollRoute = null;
    this._sourceNodeCache = new Map();
    this._activeTransformTargetColumn = null;
    this._activeExpressionTargetColumn = null;
    this._mappingExpressionEditor = null;
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
    this.$propNodeHelp = q("#propNodeHelp");
    this.$propEndpoint = q("#propEndpoint");
    this.$propEndpointHelp = q("#propEndpointHelp");
    this.$propSilverProposalId = q("#propSilverProposalId");
    this.$propSilverHelp = q("#propSilverHelp");
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
    this.$processingPolicyCard = q("#processingPolicyCard");
    this.$policyBusinessKeys = q("#policyBusinessKeys");
    this.$policyOrderingStrategy = q("#policyOrderingStrategy");
    this.$policyEventTime = q("#policyEventTime");
    this.$policySequence = q("#policySequence");
    this.$policyLateMode = q("#policyLateMode");
    this.$policyTooLate = q("#policyTooLate");
    this.$policyLateToleranceValue = q("#policyLateToleranceValue");
    this.$policyLateToleranceUnit = q("#policyLateToleranceUnit");
    this.$policyReprocessValue = q("#policyReprocessValue");
    this.$policyReprocessUnit = q("#policyReprocessUnit");
    this.$mappingCard = q("#mappingCard");
    this.$mappingSubtitle = q("#mappingSubtitle");
    this.$mappingBody = q("#mappingBody");
    this.$compileCard = q("#compileCard");
    this.$compileInfo = q("#compileInfo");
    this.$viewSqlBtn = q("#viewSqlBtn");
    this.$aiExplainProposalBtn = q("#aiExplainProposalBtn");
    this.$aiProposalResult = q("#aiProposalResult");
    this.$validationCard = q("#validationCard");
    this.$validationResults = q("#validationResults");
    this.$aiExplainValidationBtn = q("#aiExplainValidationBtn");
    this.$aiValidationResult = q("#aiValidationResult");
    this.$aiSuggestTransformsBtn = q("#aiSuggestTransformsBtn");
    this.$aiTransformsResult = q("#aiTransformsResult");
    this.$brdCard = q("#brdCard");
    this.$brdText = q("#brdText");
    this.$aiGenerateFromBrdBtn = q("#aiGenerateFromBrdBtn");
    this.$aiBrdResult = q("#aiBrdResult");
    this.$goldMartCard = q("#goldMartCard");
    this.$aiSuggestGoldMartBtn = q("#aiSuggestGoldMartBtn");
    this.$aiGoldMartResult = q("#aiGoldMartResult");
    this.$metricGlossaryCard = q("#metricGlossaryCard");
    this.$aiGenerateMetricGlossaryBtn = q("#aiGenerateMetricGlossaryBtn");
    this.$aiMetricGlossaryResult = q("#aiMetricGlossaryResult");
    this.$anomalyCard = q("#anomalyCard");
    this.$aiExplainAnomalyBtn = q("#aiExplainAnomalyBtn");
    this.$aiAnomalyResult = q("#aiAnomalyResult");
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
    this.$execRowCount = q("#execRowCount");
    this.$execBackend = q("#execBackend");
    this.$pollRunBtn = q("#pollRunBtn");
    this.$stopPollBtn = q("#stopPollBtn");
    this.$previewDataBtn = q("#previewDataBtn");
    this.$execPreview = q("#execPreview");

    this.$sqlContent = q("#sqlContent");
    this.$copySqlBtn = q("#copySqlBtn");
    this.$transformEditor = document.querySelector("transform-editor");
    this.$expressionModal = q("#expressionModal");
    this.$expressionModalTitle = q("#expressionModalTitle");
    this.$expressionModalHint = q("#expressionModalHint");
    this.$expressionCancelBtn = q("#expressionCancelBtn");
    this.$expressionSaveBtn = q("#expressionSaveBtn");
    this.$expressionMount = q("#expressionMount");

    if (this.$expressionMount && !this._mappingExpressionEditor) {
      this._mappingExpressionEditor = document.createElement("expression-component");
      this.$expressionMount.appendChild(this._mappingExpressionEditor);
      const insertValuesButton = this._mappingExpressionEditor.shadowRoot?.querySelector("#insertValuesBtn");
      if (insertValuesButton) insertValuesButton.style.display = "none";
    }
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
      if (this._currentLayer() === "silver") {
        void this._refreshSilverProposalInputs();
      }
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
    on(this.$propNodeId, "change", () => this._handleSourceNodeChange());

    // Detail
    on(this.$backToList, "click", () => this._switchTab("proposals"));
    on(this.$addColumnBtn, "click", () => this._addSchemaColumn());
    on(this.$saveSchemaBtn, "click", () => this._saveSchema());
    on(this.$viewSqlBtn, "click", () => this._switchTab("sql"));
    [
      this.$policyBusinessKeys,
      this.$policyOrderingStrategy,
      this.$policyEventTime,
      this.$policySequence,
      this.$policyLateMode,
      this.$policyTooLate,
      this.$policyLateToleranceValue,
      this.$policyLateToleranceUnit,
      this.$policyReprocessValue,
      this.$policyReprocessUnit,
    ].filter(Boolean).forEach((el) => {
      on(el, "input", () => this._markSchemaDirty());
      on(el, "change", () => this._markSchemaDirty());
    });

    // Review
    on(this.$reviewDecision, "change", () => {
      this.$submitReviewBtn.disabled = !this.$reviewDecision.value;
    });
    on(this.$submitReviewBtn, "click", () => this._submitReview());

    // Releases
    on(this.$refreshReleasesBtn, "click", () => this._loadReleases());
    on(this.$pollRunBtn, "click", () => this._pollRun());
    on(this.$stopPollBtn, "click", () => this._stopPolling());
    on(this.$previewDataBtn, "click", () => this._previewTargetData());

    // SQL
    on(this.$copySqlBtn, "click", () => this._copySql());
    on(this.$expressionCancelBtn, "click", () => this._closeExpressionEditor());
    on(this.$expressionSaveBtn, "click", () => this._applyExpressionEditor());
    on(this.$expressionModal, "click", (e) => {
      if (e.target === this.$expressionModal) this._closeExpressionEditor();
    });

    if (this.$transformEditor) {
      on(this.$transformEditor, "transform-data", (e) => this._handleTransformData(e));
    }

    // AI Assist: P1-C — Explain Proposal
    if (this.$aiExplainProposalBtn) {
      on(this.$aiExplainProposalBtn, "click", () => this._aiExplainProposal());
    }
    // AI Assist: P1-D — Explain Validation
    if (this.$aiExplainValidationBtn) {
      on(this.$aiExplainValidationBtn, "click", () => this._aiExplainValidation());
    }
    // AI Assist: P2-A — Suggest Transforms
    if (this.$aiSuggestTransformsBtn) {
      on(this.$aiSuggestTransformsBtn, "click", () => this._aiSuggestTransforms());
    }
    // AI Assist: P2-B/C — Generate from BRD
    if (this.$aiGenerateFromBrdBtn) {
      on(this.$aiGenerateFromBrdBtn, "click", () => this._aiGenerateFromBrd());
    }
    // AI Assist: P2-D — Suggest Gold Mart Design
    if (this.$aiSuggestGoldMartBtn) {
      on(this.$aiSuggestGoldMartBtn, "click", () => this._aiSuggestGoldMart());
    }
    // AI Assist: P3-C — Generate Metric Glossary
    if (this.$aiGenerateMetricGlossaryBtn) {
      on(this.$aiGenerateMetricGlossaryBtn, "click", () => this._aiGenerateMetricGlossary());
    }
    // AI Assist: P3-D — Explain Run/KPI Anomaly
    if (this.$aiExplainAnomalyBtn) {
      on(this.$aiExplainAnomalyBtn, "click", () => this._aiExplainAnomaly());
    }
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
        previewRoute: "/previewTargetData",
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
      previewRoute: "/previewTargetData",
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

  _collectSourceNodes() {
    const roots = getPanelItems();
    const seenObjects = new WeakSet();
    const seenIds = new Set();
    const queue = Array.isArray(roots) ? [...roots] : [roots];
    const nodes = [];

    while (queue.length) {
      const current = queue.shift();
      if (!current || typeof current !== "object") continue;
      if (seenObjects.has(current)) continue;
      seenObjects.add(current);

      if (SOURCE_NODE_TYPES.has(current.btype) && current.id != null && !seenIds.has(String(current.id))) {
        seenIds.add(String(current.id));
        nodes.push(current);
      }

      if (Array.isArray(current)) {
        queue.push(...current);
        continue;
      }

      for (const value of Object.values(current)) {
        if (value && typeof value === "object") queue.push(value);
      }
    }

    return nodes.sort((a, b) => Number(a.id) - Number(b.id));
  }

  _renderSourceNodeOptions(selectedId = "") {
    const sourceNodes = this._collectSourceNodes();
    const options = ['<option value="">Select a Bronze source node</option>'];
    for (const node of sourceNodes) {
      const selected = String(node.id) === String(selectedId) ? " selected" : "";
      options.push(`<option value="${this._esc(node.id)}"${selected}>${this._esc(sourceOptionLabel(node))}</option>`);
    }
    this.$propNodeId.innerHTML = options.join("");
    this.$propNodeId.disabled = sourceNodes.length === 0;
    this.$propNodeHelp.textContent = sourceNodes.length
      ? "Options come from the open graph and include node ids."
      : "No Bronze source nodes found in the open graph.";
    return sourceNodes;
  }

  async _loadSourceNode(nodeId) {
    if (!nodeId) return null;
    const key = String(nodeId);
    if (this._sourceNodeCache.has(key)) return this._sourceNodeCache.get(key);
    const node = await request(`/getItem?id=${encodeURIComponent(nodeId)}`);
    this._sourceNodeCache.set(key, node);
    return node;
  }

  _renderEndpointOptions(endpointConfigs, selectedEndpoint = "") {
    const options = ['<option value="">Select an endpoint</option>'];
    for (const cfg of endpointConfigs) {
      const selected = cfg.endpoint_name === selectedEndpoint ? " selected" : "";
      options.push(`<option value="${this._esc(cfg.endpoint_name)}"${selected}>${this._esc(cfg.endpoint_name)}</option>`);
    }
    this.$propEndpoint.innerHTML = options.join("");
    this.$propEndpoint.disabled = endpointConfigs.length === 0;
    // Explicitly set .value — innerHTML selected attribute can be unreliable in shadow DOM
    if (selectedEndpoint && endpointConfigs.some((cfg) => cfg.endpoint_name === selectedEndpoint)) {
      this.$propEndpoint.value = selectedEndpoint;
    } else if (endpointConfigs.length === 1) {
      this.$propEndpoint.value = endpointConfigs[0].endpoint_name;
    }
    this.$propEndpointHelp.textContent = endpointConfigs.length
      ? "Select the exact enabled source config to profile."
      : "This source node has no enabled endpoint configs.";
  }

  async _handleSourceNodeChange() {
    const nodeId = this.$propNodeId.value.trim();
    const previousEndpoint = this.$propEndpoint.value.trim();
    if (!nodeId) {
      this._renderEndpointOptions([]);
      return;
    }

    try {
      const sourceNode = await this._loadSourceNode(nodeId);
      const endpointConfigs = enabledSourceConfigs(sourceNode);
      const selectedEndpoint = endpointConfigs.some((cfg) => cfg.endpoint_name === previousEndpoint)
        ? previousEndpoint
        : (endpointConfigs[0]?.endpoint_name || "");
      this._renderEndpointOptions(endpointConfigs, selectedEndpoint);
      this.$propNodeHelp.textContent = `${sourceTypeLabel(sourceNode)} node #${sourceNode.id} from the open graph.`;
    } catch (e) {
      this._renderEndpointOptions([]);
      this.$propNodeHelp.textContent = `Failed to load node #${nodeId}: ${e.message || e}`;
    }
  }

  async _refreshSilverProposalInputs() {
    const currentNodeId = this.$propNodeId.value.trim();
    const sourceNodes = this._renderSourceNodeOptions(currentNodeId);
    const nextNodeId = sourceNodes.some((node) => String(node.id) === String(currentNodeId))
      ? currentNodeId
      : String(sourceNodes[0]?.id || "");
    this.$propNodeId.value = nextNodeId;
    await this._handleSourceNodeChange();
  }

  async _loadSilverProposalOptions() {
    try {
      const gid = this._getGid();
      const params = new URLSearchParams();
      if (gid) params.set("gid", gid);
      const data = await request(`/silverProposals?${params.toString()}`);
      const proposals = (Array.isArray(data) ? data : (data?.proposals || []))
        .filter((p) => p.proposal_id || p.id);
      const options = ['<option value="">Select a Silver proposal</option>'];
      for (const p of proposals) {
        const pid = p.proposal_id || p.id;
        const source = p.source_endpoint_name || p.endpoint_name || p.proposal?.endpoint_name || "--";
        const status = p.status || "draft";
        options.push(`<option value="${pid}">#${pid} — ${this._esc(source)} (${status})</option>`);
      }
      this.$propSilverProposalId.innerHTML = options.join("");
      this.$propSilverHelp.textContent = proposals.length
        ? "Published or approved Silver proposals for this graph."
        : "No Silver proposals found for this graph.";
    } catch (e) {
      this.$propSilverHelp.textContent = "Failed to load Silver proposals: " + (e.message || e);
    }
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

  _proposalState() {
    return this._currentProposal?.proposal || this._currentProposal || {};
  }

  _setProposalState(nextProposal) {
    if (!this._currentProposal) {
      this._currentProposal = nextProposal;
      return;
    }
    if (this._currentProposal.proposal) {
      this._currentProposal = { ...this._currentProposal, proposal: nextProposal };
      return;
    }
    this._currentProposal = nextProposal;
  }

  _proposalSourceAlias(proposal = this._proposalState()) {
    return proposal?.source_alias || proposal?.source_layer || (this._proposalLayer() === "gold" ? "silver" : "bronze");
  }

  _simpleSourceExpressionForColumn(sourceColumn, proposal = this._proposalState()) {
    const sourceAlias = this._proposalSourceAlias(proposal);
    if (!sourceColumn || !sourceAlias) return "";
    if (isPostgresqlProposal(proposal)) {
      return `${sourceAlias}.${quoteSqlIdentifier(sourceColumn)}`;
    }
    return `${sourceAlias}.${sourceColumn}`;
  }

  _mappingTargetColumn(mapping) {
    return mapping?.target_column || mapping?.silver_column || mapping?.column_name || mapping?.name || "";
  }

  _mappingTransforms(mapping) {
    return Array.isArray(mapping?.transform)
      ? mapping.transform
      : (Array.isArray(mapping?.transforms) ? mapping.transforms : []);
  }

  _mappingBaseExpression(mapping, proposal = this._proposalState()) {
    if (!mapping) return "";
    const explicitBase = mapping.base_expression || mapping.source_expression_base || mapping._base_expression;
    if (explicitBase) return explicitBase;
    const sourceColumn = Array.isArray(mapping.source_columns) ? mapping.source_columns[0] : "";
    const directRef = this._simpleSourceExpressionForColumn(sourceColumn, proposal);
    if (directRef) return directRef;
    const expression = String(mapping.expression || "").trim();
    return /^(?:bronze|silver)\.(?:[A-Za-z_][A-Za-z0-9_]*|"(?:[^"]|"")+")$/.test(expression) ? expression : "";
  }

  _expressionSourceCandidates(proposal = this._proposalState()) {
    const state = proposal?.proposal || proposal || {};
    const items = [...(state.columns || []), ...(state.mappings || state.field_mappings || [])];
    const seen = new Set();
    return items.flatMap((item) => {
      const expression = String(item.base_expression || item.source_expression_base || item._base_expression || item.expression || "").trim();
      const targetColumn = this._mappingTargetColumn(item);
      const sourceColumns = Array.isArray(item.source_columns) ? item.source_columns.filter(Boolean) : [];
      if (!expression || seen.has(`${targetColumn}::${expression}`)) return [];
      seen.add(`${targetColumn}::${expression}`);
      return [{
        targetColumn,
        expression,
        sourceColumns: sourceColumns.length ? sourceColumns : (targetColumn ? [targetColumn] : []),
      }];
    });
  }

  _expressionEditorItems(proposal = this._proposalState()) {
    return this._expressionSourceCandidates(proposal).map((item) => ({
      business_name: item.expression,
      technical_name: item.targetColumn || item.expression,
      column_type: "column",
      af: "Expression",
    }));
  }

  _extractSourceColumnsFromExpression(expression, proposal = this._proposalState()) {
    const refs = new Set();
    sourceReferenceMatches(expression).forEach((match) => {
      const quoted = match[1] ? match[1].replace(/""/g, '"') : "";
      const plain = match[2] || "";
      const value = quoted || plain;
      if (value) refs.add(value);
    });

    const expr = String(expression || "");
    this._expressionSourceCandidates(proposal).forEach((candidate) => {
      if (candidate.expression && expr.includes(candidate.expression)) {
        candidate.sourceColumns.forEach((col) => refs.add(col));
      }
    });
    return [...refs];
  }

  _mappingTransformSummary(mapping) {
    const transforms = this._mappingTransforms(mapping);
    return transforms.length ? transforms.join(" -> ") : (mapping?.expression || "--");
  }

  _columnNamesFromDraft(proposal = this._proposalState()) {
    const schemaRows = Array.from(this.$schemaBody?.querySelectorAll("tr") || []);
    const rowNames = schemaRows
      .map((tr) => tr.querySelector('[data-field="column_name"]')?.value?.trim())
      .filter(Boolean);
    if (rowNames.length) return rowNames;
    return (proposal?.columns || [])
      .map((col) => this._mappingTargetColumn(col))
      .filter(Boolean);
  }

  _timestampColumnCandidates(proposal = this._proposalState()) {
    const draftNames = new Set(this._columnNamesFromDraft(proposal));
    return (proposal?.columns || [])
      .filter((col) => {
        const type = String(col.type || col.data_type || "").toUpperCase();
        const role = String(col.role || "").toLowerCase();
        const columnName = this._mappingTargetColumn(col);
        return draftNames.has(columnName) && (
          role === "timestamp"
          || type === "TIMESTAMP"
          || type === "TIMESTAMPTZ"
          || /(?:event_?time|updated_?at|created_?at|timestamp)/i.test(columnName)
        );
      })
      .map((col) => this._mappingTargetColumn(col));
  }

  _processingPolicyDefault(proposal = this._proposalState()) {
    const materializationKeys = Array.isArray(proposal?.materialization?.keys) ? proposal.materialization.keys.filter(Boolean) : [];
    const timestampColumn = this._timestampColumnCandidates(proposal)[0] || "";
    const processingPolicy = proposal?.processing_policy || {};
    return {
      business_keys: Array.isArray(processingPolicy.business_keys) ? processingPolicy.business_keys.filter(Boolean) : materializationKeys,
      ordering_strategy: processingPolicy.ordering_strategy || ((materializationKeys.length && timestampColumn) ? "latest_event_time_wins" : ""),
      event_time_column: processingPolicy.event_time_column || timestampColumn,
      sequence_column: processingPolicy.sequence_column || "",
      late_data_mode: processingPolicy.late_data_mode || (proposal?.materialization?.mode === "merge" ? "merge" : ""),
      too_late_behavior: processingPolicy.too_late_behavior || "",
      late_data_tolerance: processingPolicy.late_data_tolerance || {},
      reprocess_window: processingPolicy.reprocess_window || {},
    };
  }

  _renderSelectOptions(selectEl, options, selectedValue = "", noneLabel = "None") {
    if (!selectEl) return;
    const normalized = String(selectedValue || "");
    const items = [`<option value="">${this._esc(noneLabel)}</option>`];
    const seen = new Set();
    options.filter(Boolean).forEach((value) => {
      const key = String(value);
      if (seen.has(key)) return;
      seen.add(key);
      items.push(`<option value="${this._esc(key)}"${key === normalized ? " selected" : ""}>${this._esc(key)}</option>`);
    });
    selectEl.innerHTML = items.join("");
  }

  _renderProcessingPolicy(p) {
    const proposal = p?.proposal || p || {};
    const policy = this._processingPolicyDefault(proposal);
    const columnNames = this._columnNamesFromDraft(proposal);
    const timestampCandidates = this._timestampColumnCandidates(proposal);
    const eventOptions = timestampCandidates.length ? timestampCandidates : columnNames;

    this._renderSelectOptions(this.$policyEventTime, eventOptions, policy.event_time_column, "None");
    this._renderSelectOptions(this.$policySequence, columnNames, policy.sequence_column, "None");
    this.$policyBusinessKeys.value = (policy.business_keys || []).join(", ");
    this.$policyOrderingStrategy.value = policy.ordering_strategy || "";
    this.$policyLateMode.value = policy.late_data_mode || "";
    this.$policyTooLate.value = policy.too_late_behavior || "";
    this.$policyLateToleranceValue.value = policy.late_data_tolerance?.value ?? "";
    this.$policyLateToleranceUnit.value = policy.late_data_tolerance?.unit || "";
    this.$policyReprocessValue.value = policy.reprocess_window?.value ?? "";
    this.$policyReprocessUnit.value = policy.reprocess_window?.unit || "";
  }

  _collectWindowConfig(valueEl, unitEl) {
    const value = valueEl?.value?.trim() || "";
    const unit = unitEl?.value || "";
    if (!value && !unit) return null;
    return {
      value: value ? Number(value) : null,
      unit: unit || null,
    };
  }

  _collectProcessingPolicyFromForm() {
    const businessKeys = String(this.$policyBusinessKeys?.value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    const policy = {
      business_keys: businessKeys,
      ordering_strategy: this.$policyOrderingStrategy?.value || null,
      event_time_column: this.$policyEventTime?.value || null,
      sequence_column: this.$policySequence?.value || null,
      late_data_mode: this.$policyLateMode?.value || null,
      too_late_behavior: this.$policyTooLate?.value || null,
      late_data_tolerance: this._collectWindowConfig(this.$policyLateToleranceValue, this.$policyLateToleranceUnit),
      reprocess_window: this._collectWindowConfig(this.$policyReprocessValue, this.$policyReprocessUnit),
    };

    if (!Object.values(policy).some((value) => {
      if (Array.isArray(value)) return value.length;
      if (value && typeof value === "object") return Object.values(value).some(Boolean);
      return Boolean(value);
    })) {
      return null;
    }
    return policy;
  }

  _mappingCastSummary(mapping) {
    const transforms = this._mappingTransforms(mapping).map((item) => normalizeTransformType(parseTransformItem(item).type));
    if (transforms.includes("TO_DATE")) return "DATE";
    if (transforms.includes("TO_VARCHAR") || transforms.includes("TO_STRING") || transforms.includes("TOSTRING")) return "VARCHAR";
    if (/CAST\(.* AS DATE\)/i.test(String(mapping?.expression || ""))) return "DATE";
    if (/CAST\(.* AS (?:VARCHAR|STRING)\)/i.test(String(mapping?.expression || ""))) return "VARCHAR";
    return "--";
  }

  _updateSchemaExpressionInput(targetColumn, expression) {
    const rows = Array.from(this.$schemaBody.querySelectorAll("tr"));
    const row = rows.find((tr) => tr.querySelector('[data-field="column_name"]')?.value.trim() === targetColumn);
    const expressionInput = row?.querySelector('[data-field="source_expression"]');
    if (expressionInput) expressionInput.value = expression;
  }

  _editMappingTransforms(targetColumn) {
    const proposal = this._proposalState();
    const mappings = proposal.mappings || proposal.field_mappings || [];
    const mapping = mappings.find((item) => this._mappingTargetColumn(item) === targetColumn);
    if (!mapping) {
      alert(`No mapping found for ${targetColumn}.`);
      return;
    }
    const baseExpression = this._mappingBaseExpression(mapping, proposal);
    if (!baseExpression) {
      alert(`Transforms currently require a simple source column reference for ${targetColumn}.`);
      return;
    }
    if (!this.$transformEditor) {
      alert("Transform editor is not available on this page.");
      return;
    }

    this._activeTransformTargetColumn = targetColumn;
    this.$transformEditor.setAttribute("item", JSON.stringify({
      business_name: targetColumn,
      technical_name: targetColumn,
      transform: this._mappingTransforms(mapping),
    }));
    this.$transformEditor.setAttribute("visibility", "open");
  }

  _handleTransformData(event) {
    const targetColumn = event?.detail?.businessName || this._activeTransformTargetColumn;
    if (!targetColumn || !this._currentProposal) return;
    const proposal = this._proposalState();
    const mappings = [...(proposal.mappings || proposal.field_mappings || [])];
    const columns = [...(proposal.columns || [])];
    const mappingIndex = mappings.findIndex((item) => this._mappingTargetColumn(item) === targetColumn);
    if (mappingIndex < 0) return;

    const mapping = mappings[mappingIndex];
    const baseExpression = this._mappingBaseExpression(mapping, proposal);
    if (!baseExpression) {
      alert(`Transforms currently require a simple source column reference for ${targetColumn}.`);
      return;
    }

    try {
      const transforms = Array.isArray(event?.detail?.transformations) ? event.detail.transformations : [];
      const expression = buildExpressionFromTransforms(baseExpression, transforms);
      mappings[mappingIndex] = {
        ...mapping,
        base_expression: baseExpression,
        transform: [...transforms],
        expression,
      };

      const columnIndex = columns.findIndex((col) => this._mappingTargetColumn(col) === targetColumn);
      if (columnIndex >= 0) {
        columns[columnIndex] = {
          ...columns[columnIndex],
          base_expression: baseExpression,
          transform: [...transforms],
          expression,
        };
      }

      const nextProposal = {
        ...proposal,
        columns,
        mappings,
      };
      if (proposal.field_mappings && !proposal.mappings) nextProposal.field_mappings = mappings;
      this._setProposalState(nextProposal);
      this._updateSchemaExpressionInput(targetColumn, expression);
      this._renderMapping(this._currentProposal);
      this._markSchemaDirty();
      this._setDetailStatus(`Updated transforms for ${targetColumn}.`);
    } catch (e) {
      this._setDetailStatus(`Transform update failed for ${targetColumn}: ${e.message || e}`);
    } finally {
      this._activeTransformTargetColumn = null;
    }
  }

  _openExpressionEditor(targetColumn) {
    const proposal = this._proposalState();
    const mappings = proposal.mappings || proposal.field_mappings || [];
    const mapping = mappings.find((item) => this._mappingTargetColumn(item) === targetColumn);
    if (!mapping || !this._mappingExpressionEditor) return;

    this._activeExpressionTargetColumn = targetColumn;
    this.$expressionModalTitle.textContent = `Expression Editor: ${targetColumn}`;
    this.$expressionModalHint.textContent = `Use source expressions from the ${this._proposalSourceAlias(proposal)} layer, plus functions and operators, to define ${targetColumn}.`;
    this._mappingExpressionEditor.selectedRectangle = {
      alias: this._proposalSourceAlias(proposal),
      items: this._expressionEditorItems(proposal),
    };
    this._mappingExpressionEditor.updateColumnCount?.();
    this._mappingExpressionEditor.expressionArea?.setTextContent(String(mapping.expression || "").trim());
    this.$expressionModal.classList.add("open");
  }

  _closeExpressionEditor() {
    this.$expressionModal.classList.remove("open");
    this._activeExpressionTargetColumn = null;
  }

  _applyExpressionEditor() {
    const targetColumn = this._activeExpressionTargetColumn;
    if (!targetColumn || !this._currentProposal || !this._mappingExpressionEditor) return;
    const expression = String(this._mappingExpressionEditor.expressionArea?.getTextContent?.() || "").trim();
    if (!expression) {
      alert("Expression cannot be blank.");
      return;
    }

    const proposal = this._proposalState();
    const mappings = [...(proposal.mappings || proposal.field_mappings || [])];
    const columns = [...(proposal.columns || [])];
    const mappingIndex = mappings.findIndex((item) => this._mappingTargetColumn(item) === targetColumn);
    if (mappingIndex < 0) return;

    const mapping = mappings[mappingIndex];
    const baseExpression = this._mappingBaseExpression(mapping, proposal);
    const transforms = this._mappingTransforms(mapping);
    const extractedSourceColumns = this._extractSourceColumnsFromExpression(expression, proposal);
    const nextMapping = {
      ...mapping,
      expression,
      source_columns: extractedSourceColumns.length ? extractedSourceColumns : (Array.isArray(mapping.source_columns) ? [...mapping.source_columns] : []),
    };

    if (expression === baseExpression) {
      nextMapping.base_expression = baseExpression;
      delete nextMapping.transform;
    } else if (transforms.length && baseExpression) {
      try {
        const generatedExpression = buildExpressionFromTransforms(baseExpression, transforms);
        if (generatedExpression === expression) {
          nextMapping.transform = [...transforms];
          nextMapping.base_expression = baseExpression;
        } else {
          delete nextMapping.transform;
          delete nextMapping.base_expression;
        }
      } catch (_) {
        delete nextMapping.transform;
        delete nextMapping.base_expression;
      }
    } else {
      delete nextMapping.transform;
      delete nextMapping.base_expression;
    }

    mappings[mappingIndex] = nextMapping;

    const columnIndex = columns.findIndex((col) => this._mappingTargetColumn(col) === targetColumn);
    if (columnIndex >= 0) {
      const nextColumn = {
        ...columns[columnIndex],
        expression,
        source_columns: nextMapping.source_columns,
      };
      if (nextMapping.base_expression) nextColumn.base_expression = nextMapping.base_expression;
      else delete nextColumn.base_expression;
      if (Array.isArray(nextMapping.transform)) nextColumn.transform = [...nextMapping.transform];
      else delete nextColumn.transform;
      columns[columnIndex] = nextColumn;
    }

    const nextProposal = { ...proposal, columns, mappings };
    if (proposal.field_mappings && !proposal.mappings) nextProposal.field_mappings = mappings;
    this._setProposalState(nextProposal);
    this._schemaDirty = true;
    this.$saveSchemaBtn.disabled = false;
    this._renderMapping(this._currentProposal);
    this._updateSchemaExpressionInput(targetColumn, expression);
    this._setDetailStatus(`Updated expression for ${targetColumn}.`);
    this._closeExpressionEditor();
  }

  _syncMappingsFromColumns(proposal, columns) {
    const proposalMappings = proposal.mappings || proposal.field_mappings || [];
    if (!proposalMappings.length) return proposalMappings;
    return proposalMappings.map((mapping) => {
      const targetColumn = this._mappingTargetColumn(mapping);
      const column = columns.find((col) => this._mappingTargetColumn(col) === targetColumn);
      if (!column) return mapping;

      const nextMapping = {
        ...mapping,
        expression: column.expression || column.source_expression || mapping.expression || "",
        source_columns: Array.isArray(column.source_columns) && column.source_columns.length
          ? [...column.source_columns]
          : (Array.isArray(mapping.source_columns) ? [...mapping.source_columns] : []),
      };
      const transforms = Array.isArray(column.transform)
        ? column.transform
        : this._mappingTransforms(mapping);
      const baseExpression = column.base_expression || mapping.base_expression || this._mappingBaseExpression(mapping, proposal);

      if (transforms.length && baseExpression) {
        try {
          const generatedExpression = buildExpressionFromTransforms(baseExpression, transforms);
          if (generatedExpression === nextMapping.expression) {
            nextMapping.transform = [...transforms];
            nextMapping.base_expression = baseExpression;
          } else {
            delete nextMapping.transform;
            delete nextMapping.base_expression;
          }
        } catch (_) {
          delete nextMapping.transform;
          delete nextMapping.base_expression;
        }
      }
      return nextMapping;
    });
  }

  // ── Tab switching ──
  _switchTab(tab) {
    this.shadowRoot.querySelectorAll(".tabs .tab[data-tab]").forEach((t) => {
      t.classList.toggle("active", t.dataset.tab === tab);
    });
    this.shadowRoot.querySelectorAll(".tab-panel[data-panel]").forEach((p) => {
      p.classList.toggle("active", p.dataset.panel === tab);
    });
    if (tab === "proposals") this._loadProposals();
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
      const safeStatus = this._esc(p.status || "draft");
      tr.innerHTML = `
        <td>${this._esc(p.proposal_id || p.id || "--")}</td>
        <td>${this._esc(p.source_node_id || p.api_node_id || "--")}</td>
        <td>${this._esc(this._proposalSourceName(p))}</td>
        <td>${this._esc(layer)}</td>
        <td><span class="badge ${badgeClass(p.status)}">${safeStatus}</span></td>
        <td>${this._esc(p.created_by || "--")}</td>
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

  async _toggleNewProposalForm(show) {
    this.$newProposalCard.style.display = show ? "block" : "none";
    if (show) {
      if (this._currentLayer() === "gold") {
        this.$submitProposalBtn.disabled = true;
        await this._loadSilverProposalOptions();
        this.$submitProposalBtn.disabled = false;
        this.$propSilverProposalId.focus();
      }
      else {
        this.$submitProposalBtn.disabled = true;
        await this._refreshSilverProposalInputs();
        this.$submitProposalBtn.disabled = false;
        this.$propNodeId.focus();
      }
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
      this._renderEndpointOptions([]);
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
      // Clear stale AI results from previous proposal
      if (this.$aiProposalResult) this.$aiProposalResult.innerHTML = "";
      if (this.$aiValidationResult) this.$aiValidationResult.innerHTML = "";
      if (this.$aiTransformsResult) this.$aiTransformsResult.innerHTML = "";
      if (this.$aiBrdResult) this.$aiBrdResult.innerHTML = "";
      if (this.$aiGoldMartResult) this.$aiGoldMartResult.innerHTML = "";
      if (this.$aiMetricGlossaryResult) this.$aiMetricGlossaryResult.innerHTML = "";
      if (this.$aiAnomalyResult) this.$aiAnomalyResult.innerHTML = "";
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
    const latestValidationStatus = p.latest_validation?.status
      ? ` | Latest validation: ${p.latest_validation.status}`
      : "";

    this.$detailTitle.textContent = `Proposal #${p.proposal_id || p.id} — ${this._proposalSourceName(p)}`;
    this.$detailMeta.textContent = `Layer: ${cfg.layerLabel} | Status: ${p.status || "draft"}${latestValidationStatus} | Source node: ${p.source_node_id || p.api_node_id || "--"} | Created: ${fmtDate(this._proposalCreatedAt(p))} | By: ${p.created_by || "--"}`;

    this._renderPipeline(this.$pipelineSteps, p);
    this._renderSchema(p);
    this._renderProcessingPolicy(p);
    this._renderMapping(p);
    this._renderDetailActions(p);
    this._renderValidation(p);
    // P2: Show BRD card always, show gold mart card for gold proposals
    if (this.$brdCard) this.$brdCard.style.display = "block";
    if (this.$goldMartCard) {
      this.$goldMartCard.style.display = (p.layer === "gold") ? "block" : "none";
    }
    // P3: Metric glossary for gold proposals, anomaly card always
    if (this.$metricGlossaryCard) {
      this.$metricGlossaryCard.style.display = (p.layer === "gold") ? "block" : "none";
    }
    if (this.$anomalyCard) this.$anomalyCard.style.display = "block";
  }

  _pipelineState(p) {
    const proposalStatus = String(p?.status || "draft").toLowerCase();
    const latestValidationStatus = String(p?.latest_validation?.status || p?.validation?.status || "").toLowerCase();

    if (proposalStatus === "rejected") {
      return { doneSteps: new Set(["propose", "edit", "compile", "validate"]), currentStep: null, errorStep: "review" };
    }
    if (proposalStatus === "changes_requested") {
      return { doneSteps: new Set(["propose", "edit", "compile", "validate"]), currentStep: "edit", errorStep: "review" };
    }
    if (proposalStatus === "draft" && latestValidationStatus === "invalid") {
      return { doneSteps: new Set(["propose", "edit", "compile"]), currentStep: null, errorStep: "validate" };
    }

    switch (proposalStatus) {
      case "published":
        return { doneSteps: new Set(["propose", "edit", "compile", "validate", "review", "publish"]), currentStep: "execute", errorStep: null };
      case "approved":
        return { doneSteps: new Set(["propose", "edit", "compile", "validate", "review"]), currentStep: "publish", errorStep: null };
      case "validated":
        return { doneSteps: new Set(["propose", "edit", "compile", "validate"]), currentStep: "review", errorStep: null };
      case "compiled":
        return { doneSteps: new Set(["propose", "edit", "compile"]), currentStep: "validate", errorStep: null };
      case "proposed":
        return { doneSteps: new Set(["propose", "edit"]), currentStep: "compile", errorStep: null };
      default:
        return { doneSteps: new Set(), currentStep: "propose", errorStep: null };
    }
  }

  _renderPipeline(container, proposal) {
    const { doneSteps, currentStep, errorStep } = this._pipelineState(proposal);
    container.querySelectorAll(".pipeline-step").forEach((el) => {
      const stepName = el.dataset.step;
      el.classList.remove("done", "current", "error");
      if (errorStep === stepName) {
        el.classList.add("error");
      } else if (doneSteps.has(stepName)) {
        el.classList.add("done");
      } else if (currentStep === stepName) {
        el.classList.add("current");
      }
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
    const proposalState = p.proposal || p || {};
    const columns = this._collectSchemaFromTable();
    const mappings = this._syncMappingsFromColumns(proposalState, columns);
    this.$saveSchemaBtn.disabled = true;
    this._setDetailStatus("Saving schema changes...", true);

    try {
      const proposal = { ...proposalState, columns, mappings, processing_policy: this._collectProcessingPolicyFromForm(proposalState) };
      if (proposalState.field_mappings && !proposalState.mappings) proposal.field_mappings = mappings;
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
      const targetColumn = this._mappingTargetColumn(m);
      const transformSummary = this._mappingTransformSummary(m);
      const castSummary = this._mappingCastSummary(m);
      const canEditTransforms = Boolean(this._mappingBaseExpression(m, proposal));
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${this._esc(m.source_field || m.bronze_field || (m.source_columns || []).join(", ") || "")}</td>
        <td>${this._esc(targetColumn)}</td>
        <td>
          ${this._esc(transformSummary)}
          <span class="mapping-expression-note">${this._esc(m.expression || "--")}</span>
        </td>
        <td>${this._esc(m.cast || m.type_cast || castSummary)}</td>
        <td>
          <div class="mapping-action-group">
            <button class="small secondary" type="button" data-edit-expression="${this._esc(targetColumn)}">Expression</button>
            <button class="small secondary" type="button" data-edit-transform="${this._esc(targetColumn)}" ${canEditTransforms ? "" : "disabled"}>Transform</button>
          </div>
        </td>
      `;
      tr.querySelector('[data-edit-expression]')?.addEventListener("click", () => this._openExpressionEditor(targetColumn));
      tr.querySelector('[data-edit-transform]')?.addEventListener("click", () => this._editMappingTransforms(targetColumn));
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

    // Backend stores errors in schema_errors/warnings; normalize to a single list
    const schemaErrors = (val.schema_errors || []).map(e => ({ ...e, result: "fail" }));
    const warnings = (val.warnings || []).map(w => ({ ...w, result: "warn" }));
    const sampleError = val.sample_execution?.error
      ? [{ kind: val.sample_execution.error.kind || "sample_execution", message: val.sample_execution.error.message, result: "fail" }]
      : [];
    const legacyResults = val.results || val.checks || (Array.isArray(val) ? val : []);
    const allResults = [...schemaErrors, ...sampleError, ...warnings, ...legacyResults];

    const validationStatus = val.status || p.latest_validation?.status || "";

    if (!allResults.length && !validationStatus) {
      this.$validationResults.innerHTML = '<p style="color:#6b7280;font-size:13px;">No validation results available.</p>';
      return;
    }

    let html = "";

    // Show overall status
    if (validationStatus) {
      const isValid = validationStatus === "valid";
      html += `<p style="font-size:14px;font-weight:600;margin-bottom:8px;color:${isValid ? "#16a34a" : "#dc2626"};">
        Validation status: ${this._esc(validationStatus.toUpperCase())}</p>`;
    }

    if (!allResults.length) {
      html += '<p style="color:#6b7280;font-size:13px;">No detailed check results.</p>';
      this.$validationResults.innerHTML = html;
      return;
    }

    html += '<table style="font-size:12px;"><thead><tr><th>Kind</th><th>Result</th><th>Message</th></tr></thead><tbody>';
    for (const r of allResults) {
      const pass = r.passed || r.result === "pass" || r.status === "pass";
      const isWarn = r.result === "warn";
      const badgeClass = pass ? "badge-validated" : isWarn ? "badge-warning" : "badge-failed";
      const label = pass ? "PASS" : isWarn ? "WARN" : "FAIL";
      html += `<tr>
        <td>${this._esc(r.kind || r.check || r.name || "")}</td>
        <td><span class="badge ${badgeClass}">${label}</span></td>
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
      const errors = e.responseData?.data?.errors || [];
      if (errors.length) {
        const lines = errors.map(err => this._esc(`${err.kind || "error"}: ${err.message || JSON.stringify(err)}`));
        this.$detailStatus.innerHTML = `<span>${this._esc("Compile failed: " + (e.message || e))}<br>${lines.join("<br>")}</span>`;
      } else {
        this._setDetailStatus("Compile failed: " + (e.message || e));
      }
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

  // ── AI Assist handlers ──

  async _aiExplainProposal() {
    const p = this._currentProposal;
    if (!p) return;
    const pid = p.proposal_id || p.id;
    this.$aiExplainProposalBtn.disabled = true;
    this.$aiProposalResult.innerHTML = renderAiLoading("Explaining proposal...");
    try {
      const result = await callAiEndpoint("/aiExplainModelProposal", {
        proposal_id: pid,
        proposal_json: p.proposal || p,
        compile_result: this._compiledArtifact || null,
      });
      this.$aiProposalResult.innerHTML = renderAiCard(result, { title: "Proposal Explanation" });
    } catch (err) {
      this.$aiProposalResult.innerHTML = `<div class="ai-warnings">&#9888; ${this._esc(err.message || "AI request failed")}</div>`;
    } finally {
      this.$aiExplainProposalBtn.disabled = false;
    }
  }

  async _aiExplainValidation() {
    const p = this._currentProposal;
    if (!p) return;
    const pid = p.proposal_id || p.id;
    const val = p.validation || p.validation_results || p.latest_validation?.validation;
    if (!val) return;
    this.$aiExplainValidationBtn.disabled = true;
    this.$aiValidationResult.innerHTML = renderAiLoading("Explaining validation results...");
    try {
      const result = await callAiEndpoint("/aiExplainProposalValidation", {
        proposal_id: pid,
        validation_result: val,
      });
      this.$aiValidationResult.innerHTML = renderAiCard(result, { title: "Validation Explanation" });
    } catch (err) {
      this.$aiValidationResult.innerHTML = `<div class="ai-warnings">&#9888; ${this._esc(err.message || "AI request failed")}</div>`;
    } finally {
      this.$aiExplainValidationBtn.disabled = false;
    }
  }

  // ── AI Assist P2 handlers ──

  /** Snapshot the current proposal for one-level undo. */
  _snapshotForUndo() {
    const p = this._currentProposal;
    if (!p) return;
    this._aiUndoSnapshot = JSON.parse(JSON.stringify(p.proposal || p));
  }

  /** Restore proposal from undo snapshot and re-render. */
  _undoAiEdits() {
    if (!this._aiUndoSnapshot || !this._currentProposal) return;
    if (this._currentProposal.proposal) {
      this._currentProposal.proposal = this._aiUndoSnapshot;
    } else {
      Object.keys(this._aiUndoSnapshot).forEach(k => { this._currentProposal[k] = this._aiUndoSnapshot[k]; });
    }
    this._aiUndoSnapshot = null;
    this._schemaDirty = true;
    this._renderDetail();
    this._setDetailStatus("AI edits undone.");
  }

  /**
   * Apply an array of flattened edit objects to the current proposal.
   * Each edit has { category, value } where value is the AI-returned entry.
   */
  _applyAiEdits(edits) {
    const p = this._currentProposal;
    if (!p || !edits.length) return;
    this._snapshotForUndo();
    const proposal = p.proposal || p;
    const columns = proposal.columns || proposal.target_columns || [];

    for (const edit of edits) {
      switch (edit.category) {
        case "type_casts": {
          const col = columns.find(c => (c.target_column || c.column_name) === edit.value.target_column);
          if (col) { col.expression = edit.value.expression; col.type_cast = edit.value.expression; }
          break;
        }
        case "column_renames": {
          const col = columns.find(c => (c.target_column || c.column_name) === edit.value.target_column);
          if (col && edit.value.new_name) col.target_column = edit.value.new_name;
          break;
        }
        case "derived_columns":
        case "add_columns": {
          if (!columns.find(c => (c.target_column || c.column_name) === edit.value.target_column)) {
            columns.push({
              target_column: edit.value.target_column,
              data_type: edit.value.data_type || "STRING",
              expression: edit.value.expression || null,
              source_columns: edit.value.source_column ? [edit.value.source_column] : [],
            });
          }
          break;
        }
        case "target_columns": {
          if (!columns.find(c => (c.target_column || c.column_name) === edit.value.target_column)) {
            columns.push(edit.value);
          }
          break;
        }
        case "modify_columns": {
          const col = columns.find(c => (c.target_column || c.column_name) === edit.value.target_column);
          if (col && edit.value.expression) col.expression = edit.value.expression;
          if (col && edit.value.data_type) col.data_type = edit.value.data_type;
          break;
        }
        case "materialization": {
          proposal.materialization = edit.value;
          break;
        }
        case "merge_keys": {
          if (proposal.materialization && typeof proposal.materialization === "object") {
            proposal.materialization.keys = Array.isArray(edit.value) ? edit.value : [edit.value];
          } else {
            proposal.merge_keys = Array.isArray(edit.value) ? edit.value : [edit.value];
          }
          break;
        }
        case "filters": {
          if (!proposal.filters) proposal.filters = [];
          proposal.filters.push(edit.value);
          break;
        }
        case "partition_keys": {
          if (!proposal.partition_keys) proposal.partition_keys = [];
          proposal.partition_keys.push(edit.value.target_column || edit.value.expression || edit.value);
          break;
        }
      }
    }

    // Ensure column array reference is set back on proposal
    if (!proposal.columns && !proposal.target_columns) proposal.columns = columns;

    this._schemaDirty = true;
    this._renderDetail();
    this._setDetailStatus(`Applied ${edits.length} AI edit(s). Compile or save to persist changes.`);
  }

  async _aiSuggestTransforms() {
    const p = this._currentProposal;
    if (!p) return;
    const pid = p.proposal_id || p.id;
    this.$aiSuggestTransformsBtn.disabled = true;
    this.$aiTransformsResult.innerHTML = renderAiLoading("Analyzing transforms...");
    try {
      const result = await callAiEndpoint("/aiSuggestSilverTransforms", {
        proposal_id: pid,
        proposal: p.proposal || p,
      });
      const edits = flattenEdits(result.edits);
      this.$aiTransformsResult.innerHTML = edits.length
        ? renderAiEditsCard(result, { title: "Transform Suggestions" })
        : renderAiCard(result, { title: "Transform Suggestions" });
      if (edits.length) {
        bindAiEditActions(this.$aiTransformsResult, {
          edits,
          onApply: (selected) => this._applyAiEdits(selected),
          onUndo: () => this._undoAiEdits(),
        });
      }
    } catch (err) {
      this.$aiTransformsResult.innerHTML = `<div class="ai-warnings">&#9888; ${this._esc(err.message || "AI request failed")}</div>`;
    } finally {
      this.$aiSuggestTransformsBtn.disabled = false;
    }
  }

  async _aiGenerateFromBrd() {
    const p = this._currentProposal;
    if (!p) return;
    const brdText = this.$brdText?.value?.trim();
    if (!brdText) { this.$aiBrdResult.innerHTML = `<div class="ai-warnings">&#9888; Please enter BRD text first.</div>`; return; }
    const proposal = p.proposal || p;
    const sourceColumns = proposal.columns || proposal.target_columns || [];
    const layer = p.layer || this._currentLayer();
    const route = layer === "gold" ? "/aiGenerateGoldFromBRD" : "/aiGenerateSilverFromBRD";
    this.$aiGenerateFromBrdBtn.disabled = true;
    this.$aiBrdResult.innerHTML = renderAiLoading(`Generating ${layer} proposal from BRD...`);
    try {
      const payload = layer === "gold"
        ? { brd_text: brdText, source_columns: sourceColumns, silver_proposal_id: p.proposal_id || p.id }
        : { brd_text: brdText, source_columns: sourceColumns, endpoint_config: { endpoint_name: p.source_endpoint_name } };
      const result = await callAiEndpoint(route, payload);
      const edits = flattenEdits(result.edits);
      const title = `${layer === "gold" ? "Gold" : "Silver"} Proposal from BRD`;
      this.$aiBrdResult.innerHTML = edits.length
        ? renderAiEditsCard(result, { title })
        : renderAiCard(result, { title });
      if (edits.length) {
        bindAiEditActions(this.$aiBrdResult, {
          edits,
          onApply: (selected) => this._applyAiEdits(selected),
          onUndo: () => this._undoAiEdits(),
        });
      }
    } catch (err) {
      this.$aiBrdResult.innerHTML = `<div class="ai-warnings">&#9888; ${this._esc(err.message || "AI request failed")}</div>`;
    } finally {
      this.$aiGenerateFromBrdBtn.disabled = false;
    }
  }

  async _aiSuggestGoldMart() {
    const p = this._currentProposal;
    if (!p) return;
    const pid = p.proposal_id || p.id;
    this.$aiSuggestGoldMartBtn.disabled = true;
    this.$aiGoldMartResult.innerHTML = renderAiLoading("Analyzing mart design...");
    try {
      const proposal = p.proposal || p;
      const result = await callAiEndpoint("/aiSuggestGoldMartDesign", {
        proposal_id: pid,
        proposal: proposal,
        source_table: proposal.source_table || null,
      });
      const edits = flattenEdits(result.edits);
      this.$aiGoldMartResult.innerHTML = edits.length
        ? renderAiEditsCard(result, { title: "Mart Design Suggestions" })
        : renderAiCard(result, { title: "Mart Design Suggestions" });
      if (edits.length) {
        bindAiEditActions(this.$aiGoldMartResult, {
          edits,
          onApply: (selected) => this._applyAiEdits(selected),
          onUndo: () => this._undoAiEdits(),
        });
      }
    } catch (err) {
      this.$aiGoldMartResult.innerHTML = `<div class="ai-warnings">&#9888; ${this._esc(err.message || "AI request failed")}</div>`;
    } finally {
      this.$aiSuggestGoldMartBtn.disabled = false;
    }
  }

  async _aiGenerateMetricGlossary() {
    const p = this._currentProposal;
    if (!p) return;
    const pid = p.proposal_id || p.id;
    this.$aiGenerateMetricGlossaryBtn.disabled = true;
    this.$aiMetricGlossaryResult.innerHTML = renderAiLoading("Generating metric definitions...");
    try {
      const proposal = p.proposal || p;
      const result = await callAiEndpoint("/aiGenerateMetricGlossary", {
        proposal_id: pid,
        proposal: proposal,
        brd_text: this.$brdText?.value || null,
      });
      this.$aiMetricGlossaryResult.innerHTML = renderAiCard(result, { title: "Metric Glossary" });
    } catch (err) {
      this.$aiMetricGlossaryResult.innerHTML = `<div class="ai-warnings">&#9888; ${this._esc(err.message || "AI request failed")}</div>`;
    } finally {
      this.$aiGenerateMetricGlossaryBtn.disabled = false;
    }
  }

  async _aiExplainAnomaly() {
    const p = this._currentProposal;
    if (!p) return;
    const pid = p.proposal_id || p.id;
    this.$aiExplainAnomalyBtn.disabled = true;
    this.$aiAnomalyResult.innerHTML = renderAiLoading("Analyzing anomalies...");
    try {
      const result = await callAiEndpoint("/aiExplainRunOrKpiAnomaly", {
        proposal_id: pid,
        run_history: p.run_history || [],
        validation_history: p.validation_history || [],
        drift_events: p.drift_events || [],
        kpi_delta: p.kpi_delta || null,
      });
      this.$aiAnomalyResult.innerHTML = renderAiCard(result, { title: "Anomaly Explanation" });
    } catch (err) {
      this.$aiAnomalyResult.innerHTML = `<div class="ai-warnings">&#9888; ${this._esc(err.message || "AI request failed")}</div>`;
    } finally {
      this.$aiExplainAnomalyBtn.disabled = false;
    }
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
    const stepNames = ["propose", "compile", "validate", "review", "publish"];
    const { doneSteps, currentStep, errorStep } = this._pipelineState(p);
    steps.forEach((label, i) => {
      const div = document.createElement("div");
      const stepName = stepNames[i];
      let cls = "";
      if (errorStep === stepName) cls = " error";
      else if (doneSteps.has(stepName)) cls = " done";
      else if (currentStep === stepName) cls = " current";
      div.className = "pipeline-step" + cls;
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
      const schemaErrors = (val.schema_errors || []).map(e => ({ ...e, result: "fail" }));
      const warnings = (val.warnings || []).map(w => ({ ...w, result: "warn" }));
      const sampleError = val.sample_execution?.error
        ? [{ kind: val.sample_execution.error.kind || "sample_execution", message: val.sample_execution.error.message, result: "fail" }]
        : [];
      const legacyChecks = val.results || val.checks || (Array.isArray(val) ? val : []);
      const allChecks = [...schemaErrors, ...sampleError, ...warnings, ...legacyChecks];
      const failed = allChecks.filter(c => !(c.passed || c.result === "pass"));
      const validationStatus = val.status || p.latest_validation?.status || "";
      this.$reviewValidation.innerHTML = `
        <div class="card" style="margin:0;">
          <h4>Validation: ${validationStatus === "valid" ? "VALID" : validationStatus === "invalid" ? "INVALID" : `${allChecks.length - failed.length}/${allChecks.length} checks passed`}</h4>
          ${failed.map((c) =>
            `<p style="color:#dc2626;font-size:12px;">${this._esc(c.result === "warn" ? "WARN" : "FAIL")}: ${this._esc(c.kind || c.check || c.name || "")} — ${this._esc(c.message || "")}</p>`
          ).join("")}
        </div>
      `;
    } else {
      this.$reviewValidation.innerHTML = '<p style="font-size:13px;color:#6b7280;">No validation results. Validate the proposal before review.</p>';
    }

    // Check if proposal is in a reviewable state
    const reviewableStatuses = new Set(["validated", "reviewed", "approved", "published"]);
    const proposalStatus = (p.status || "draft").toLowerCase();
    const isReviewable = reviewableStatuses.has(proposalStatus);

    const review = p.review || p.proposal?.review || {};
    const statusDecision =
      p.status === "published" ? "approved"
        : (p.status === "approved" || p.status === "rejected" || p.status === "changes_requested") ? p.status
          : "";
    const storedDecision = (review.state || statusDecision || "").toLowerCase();
    const storedNotes = review.notes || p.proposal?.review?.notes || "";
    const allowedDecision = ["approved", "changes_requested", "rejected"].includes(storedDecision) ? storedDecision : "";

    this.$reviewDecision.value = allowedDecision;
    this.$reviewNotes.value = storedNotes;
    this.$reviewDecision.disabled = !isReviewable;
    this.$submitReviewBtn.disabled = !isReviewable || !allowedDecision;

    // Show warning if not yet reviewable
    const warningId = "review-status-warning";
    const existing = this.$reviewContent.querySelector(`#${warningId}`);
    if (existing) existing.remove();
    if (!isReviewable) {
      const warning = document.createElement("div");
      warning.id = warningId;
      warning.style.cssText = "background:#fef3c7;border:1px solid #f59e0b;padding:10px 14px;margin-bottom:12px;font-size:13px;color:#92400e;";
      const stepHint = proposalStatus === "compiled" ? "Validate the proposal first."
        : proposalStatus === "proposed" || proposalStatus === "draft" ? "Compile and validate the proposal first."
        : `Current status: "${proposalStatus}". Proposal must be validated before review.`;
      warning.textContent = `Review is not available yet. ${stepHint}`;
      this.$reviewContent.prepend(warning);
    }
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
    this._setDetailStatus(`Submitting review: ${decision}...`, true);
    try {
      const result = await request(cfg.reviewRoute, {
        method: "POST",
        body: {
          proposal_id: p.proposal_id || p.id,
          review_state: decision,
          review_notes: notes,
        },
      });
      const nextStatus = String(result?.status || decision || "").toLowerCase();
      await this._openProposal(p.proposal_id || p.id);
      await this._loadProposals();
      this._renderReview();
      this._setDetailStatus(
        nextStatus === "approved"
          ? "Review submitted: approved. Next step: Publish Release."
          : nextStatus === "changes_requested"
            ? "Review submitted: changes requested. Proposal returned for edits."
            : nextStatus === "rejected"
              ? "Review submitted: rejected."
              : `Review submitted: ${result?.status || decision}.`
      );
      if (nextStatus === "approved") {
        this._switchTab("detail");
        const publishBtn = Array.from(this.$detailActions.querySelectorAll("button"))
          .find((btn) => btn.textContent.trim() === "Publish Release");
        publishBtn?.focus();
      }
    } catch (e) {
      const msg = (e.message || String(e));
      const is409 = msg.includes("409") || msg.includes("not ready for review");
      const displayMsg = is409
        ? "Proposal is not ready for review. Make sure it has been validated first (Propose → Compile → Validate → Review)."
        : "Review failed: " + msg;
      this._setDetailStatus(displayMsg);
      alert(displayMsg);
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
      const releaseId = r.release_id || r.active_release?.release_id;
      const tr = document.createElement("tr");
      tr.classList.add("clickable");
      const safeRunStatus = this._esc(r.run_status || "published");
      tr.innerHTML = `
        <td>${this._esc(releaseId || "--")}</td>
        <td>#${this._esc(r.proposal_id || r.id || "--")}</td>
        <td>${this._esc(this._proposalSourceName(r))}</td>
        <td>${this._esc(layer)}</td>
        <td>${fmtDate(r.published_at || this._proposalCreatedAt(r))}</td>
        <td><span class="badge ${badgeClass(r.run_status || "published")}">${safeRunStatus}</span></td>
        <td class="release-actions"></td>
      `;
      const actionsCell = tr.querySelector(".release-actions");

      if (releaseId) {
        const execBtn = document.createElement("button");
        execBtn.className = "small success";
        execBtn.textContent = "Execute";
        execBtn.type = "button";
        execBtn.addEventListener("click", (e) => { e.stopPropagation(); this._executeRelease(releaseId, this._proposalConfig(r)); });
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

    this._switchTab("releases");
    this.$executionCard.style.display = "block";
    this.$execRunId.textContent = "Starting...";
    this.$execStatus.innerHTML = '<span class="badge badge-running">STARTING</span>';
    this.$execStarted.textContent = "--";
    this.$execDuration.textContent = "--";
    this.$execRowCount.textContent = "--";
    this.$execBackend.textContent = "--";
    this.$execLog.textContent = "Submitting execution request...\n";
    this.$previewDataBtn.style.display = "none";
    this.$execPreview.style.display = "none";
    this.$execPreview.innerHTML = "";
    this._currentRequestId = null;
    this._currentRunId = null;
    this._activePollRoute = cfg.pollRoute;

    try {
      const result = await request(cfg.executeRoute, {
        method: "POST",
        body: { release_id: releaseId },
      });
      this._currentRequestId = result?.request_id || null;
      this._currentRunId = result?.model_run_id || null;
      this.$execRunId.textContent = this._currentRequestId || this._currentRunId || result?.run_id || "--";
      const initialStatus = String(result?.request_status || result?.status || "queued").toLowerCase();
      this.$execStatus.innerHTML = `<span class="badge ${badgeClass(initialStatus)}">${initialStatus.toUpperCase()}</span>`;
      if (this._currentRequestId) this.$execLog.textContent += `Request ID: ${this._currentRequestId}\n`;
      if (result?.run_id) this.$execLog.textContent += `Execution Run ID: ${result.run_id}\n`;
      if (this._currentRunId) this.$execLog.textContent += `Model Run ID: ${this._currentRunId}\n`;
      if (result?.deduped) this.$execLog.textContent += "Request deduped against an already active execution.\n";
      if (result?.backend) this.$execLog.textContent += `Backend: ${result.backend}\n`;

      // Start auto-polling
      this._startPolling();
    } catch (e) {
      this.$execStatus.innerHTML = '<span class="badge badge-failed">FAILED</span>';
      this.$execLog.textContent += `Error: ${e.message || e}\n`;
    }
  }

  async _pollRun() {
    if (!this._currentRequestId && !this._currentRunId) { alert("No active run to poll."); return; }
    try {
      const body = this._currentRequestId
        ? { request_id: this._currentRequestId }
        : { model_run_id: this._currentRunId };
      const pollRoute = this._activePollRoute || this._proposalConfig().pollRoute;
      const result = await request(pollRoute, {
        method: "POST",
        body,
      });
      if (result?.request_id) this._currentRequestId = result.request_id;
      if (result?.model_run_id) {
        this._currentRunId = result.model_run_id;
        this.$execRunId.textContent = this._currentRunId;
      } else if (this._currentRequestId) {
        this.$execRunId.textContent = this._currentRequestId;
      }

      const status = result?.status || result?.request_status || result?.run_status || "unknown";
      this.$execStatus.innerHTML = `<span class="badge ${badgeClass(status)}">${status.toUpperCase()}</span>`;

      // Started time — backend uses created_at_utc
      const started = result?.created_at_utc || result?.started_at_utc || result?.started_at;
      this.$execStarted.textContent = fmtDate(started);

      // Duration — compute from created_at_utc and completed_at_utc
      const completed = result?.completed_at_utc || result?.completed_at;
      if (started && completed) {
        const ms = new Date(completed) - new Date(started);
        if (ms >= 0) {
          const secs = Math.round(ms / 1000);
          this.$execDuration.textContent = secs < 60 ? `${secs}s` : `${Math.floor(secs / 60)}m ${secs % 60}s`;
        }
      } else if (started) {
        const ms = Date.now() - new Date(started);
        if (ms >= 0) {
          const secs = Math.round(ms / 1000);
          this.$execDuration.textContent = secs < 60 ? `${secs}s (running)` : `${Math.floor(secs / 60)}m ${secs % 60}s (running)`;
        }
      }

      // Backend
      const backend = result?.execution_backend || result?.backend;
      if (backend) this.$execBackend.textContent = backend;

      // Row counts from response_json
      const respJson = result?.response_json;
      if (respJson) {
        const jdbcResult = respJson?.result;
        if (Array.isArray(jdbcResult) && jdbcResult.length > 0) {
          // JDBC returns [{:next.jdbc/update-count N}] for DML
          const updateCount = jdbcResult[0]?.["next.jdbc/update-count"] ?? jdbcResult[0]?.update_count ?? jdbcResult[0]?.["update-count"];
          if (updateCount != null) {
            this.$execRowCount.textContent = String(updateCount);
          } else {
            this.$execRowCount.textContent = `${jdbcResult.length} result(s)`;
          }
        } else if (respJson?.error) {
          this.$execRowCount.textContent = "error";
        }
      }

      const log = result?.log || result?.output || result?.message || "";
      if (log) this.$execLog.textContent += log + "\n";

      const terminal = ["completed", "succeeded", "failed", "error", "cancelled", "timed_out"];
      if (terminal.includes(status.toLowerCase())) {
        this._stopPolling();
        this.$execLog.textContent += `\n--- Run ${status} ---\n`;
        if (status.toLowerCase() === "succeeded") {
          this.$previewDataBtn.style.display = "inline-block";
        }
      }
    } catch (e) {
      this.$execLog.textContent += `Poll error: ${e.message || e}\n`;
    }
  }

  _startPolling() {
    this._stopPolling();
    this._pollCount = 0;
    this.$stopPollBtn.style.display = "inline-block";
    this._pollTimer = setInterval(() => {
      this._pollCount += 1;
      if (this._pollCount > 360) {
        this._stopPolling();
        this.$execStatus.innerHTML = '<span class="badge badge-failed">POLL TIMEOUT</span>';
        this.$execLog.textContent += "\n--- Auto polling timed out; use Poll Status to continue checking this run. ---\n";
        return;
      }
      this._pollRun();
    }, 5000);
    this._pollRun(); // immediate first poll
  }

  _stopPolling() {
    if (this._pollTimer) { clearInterval(this._pollTimer); this._pollTimer = null; }
    this._pollCount = 0;
    this.$stopPollBtn.style.display = "none";
  }

  async _previewTargetData() {
    const p = this._currentProposal;
    if (!p) { alert("No proposal selected."); return; }
    const pid = p.proposal_id || p.id;
    this.$previewDataBtn.disabled = true;
    this.$execPreview.style.display = "block";
    this.$execPreview.innerHTML = '<span class="spinner"></span> Loading preview...';
    try {
      const cfg = this._proposalConfig(p);
      const data = await request(`${cfg.previewRoute}?proposal_id=${pid}&limit=10`);
      const rows = data?.rows || [];
      if (!rows.length) {
        this.$execPreview.innerHTML = "<em>No rows found in target table.</em>";
        return;
      }
      const cols = Object.keys(rows[0]);
      let html = `<div style="font-size:12px;margin-bottom:4px;"><strong>${data.target_table || "Target"}</strong> — ${data.row_count} row(s)</div>`;
      html += '<table style="width:100%;border-collapse:collapse;font-size:12px;"><thead><tr>';
      for (const c of cols) html += `<th style="border:1px solid #ddd;padding:4px 6px;background:#f5f5f5;text-align:left;">${this._esc(c)}</th>`;
      html += "</tr></thead><tbody>";
      for (const row of rows) {
        html += "<tr>";
        for (const c of cols) {
          const v = row[c];
          const text = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
          html += `<td style="border:1px solid #ddd;padding:4px 6px;">${this._esc(text)}</td>`;
        }
        html += "</tr>";
      }
      html += "</tbody></table>";
      this.$execPreview.innerHTML = html;
    } catch (e) {
      this.$execPreview.innerHTML = `<span style="color:#dc2626;">Preview failed: ${this._esc(e.message || e)}</span>`;
    } finally {
      this.$previewDataBtn.disabled = false;
    }
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
    const explicitGid = window.data?.gid;
    if (explicitGid != null && explicitGid !== "") return explicitGid;

    // Fallback for a just-created unsaved graph where panelItems still holds the graph shell.
    const items = getPanelItems();
    if (items.length === 1 && !items[0]?.btype && items[0]?.id) return items[0].id;
    return null;
  }

  _setDetailStatus(msg, loading = false) {
    this.$detailStatus.innerHTML = (loading ? '<span class="spinner"></span>' : "") + (msg ? `<span>${this._esc(msg)}</span>` : "");
  }

  _esc(s) {
    if (!s) return "";
    const d = document.createElement("div");
    d.textContent = String(s);
    return d.innerHTML
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }
}

customElements.define("modeling-console", ModelingConsole);
