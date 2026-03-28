/**
 * aiAssistCard.js — Shared AI-assist trigger + card pattern for Shadow DOM components.
 *
 * Provides:
 *   - aiAssistCSS       — inline <style> string for Shadow DOM injection
 *   - renderAiTrigger() — button HTML for "Explain with AI" / "Refine recommendation"
 *   - renderAiCard()    — collapsible result card from the normalized AI envelope
 *   - bindAiTriggers()  — wires up click handlers inside a Shadow root
 */

import { request } from "./library/utils.js";

// ---------------------------------------------------------------------------
// Shared inline CSS (injected into Shadow DOM <style> blocks)
// ---------------------------------------------------------------------------

export const aiAssistCSS = `
/* ── AI Assist trigger ── */
.ai-trigger {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 4px 12px; margin-top: 8px;
  border: 1px solid rgba(99,102,241,0.3); border-radius: 6px;
  background: rgba(99,102,241,0.06); color: #6366f1;
  font-size: 11px; font-weight: 600; font-family: 'DM Sans', sans-serif;
  cursor: pointer; transition: all 0.2s ease;
}
.ai-trigger:hover { background: rgba(99,102,241,0.12); border-color: #6366f1; }
.ai-trigger:disabled { opacity: 0.5; cursor: not-allowed; }
.ai-trigger .ai-icon { font-size: 13px; }

/* ── AI Assist card ── */
.ai-card {
  margin-top: 10px; padding: 14px 16px;
  background: #fff; border: 1px solid #e2e4ea; border-radius: 10px;
  border-left: 3px solid #6366f1;
  box-shadow: 0 1px 3px rgba(0,0,0,0.04);
  font-family: 'DM Sans', sans-serif; font-size: 12px;
}
.ai-card-header {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 8px;
}
.ai-card-title {
  font-size: 12px; font-weight: 600; color: #1a1d26;
}
.ai-confidence {
  display: inline-block; padding: 2px 8px; border-radius: 10px;
  font-size: 10px; font-weight: 600; font-family: 'JetBrains Mono', monospace;
}
.ai-confidence.high   { background: rgba(15,169,104,0.1); color: #0fa968; }
.ai-confidence.medium { background: rgba(217,119,6,0.1);  color: #d97706; }
.ai-confidence.low    { background: rgba(139,145,163,0.1); color: #8b91a3; }

.ai-card-summary { color: #1a1d26; line-height: 1.5; margin-bottom: 6px; }

.ai-card details { margin-top: 6px; font-size: 11px; color: #5c6070; }
.ai-card details summary {
  cursor: pointer; font-weight: 600; color: #6366f1; font-size: 11px;
}
.ai-card details ul { margin: 6px 0 0 16px; padding: 0; list-style: none; }
.ai-card details li { padding: 2px 0; display: flex; gap: 6px; }
.ai-card details li::before { content: "→"; color: #6366f1; }

.ai-card .ai-warnings {
  margin-top: 8px; padding: 8px 10px;
  background: rgba(245,158,11,0.06); border: 1px solid rgba(245,158,11,0.2);
  border-radius: 6px; font-size: 11px; color: #92400e;
}

.ai-card .ai-source-badge {
  display: inline-block; margin-top: 6px; padding: 1px 6px; border-radius: 4px;
  font-size: 9px; font-weight: 600; font-family: 'JetBrains Mono', monospace;
  background: #f5f6f8; color: #8b91a3;
}

/* ── Cherry-pick edits (P2) ── */
.ai-edits { margin-top: 10px; border-top: 1px solid #eceef2; padding-top: 8px; }
.ai-edits-title { font-size: 11px; font-weight: 600; color: #1a1d26; margin-bottom: 6px; }
.ai-edit-row {
  display: flex; align-items: flex-start; gap: 8px; padding: 6px 8px;
  border: 1px solid #eceef2; border-radius: 6px; margin-bottom: 4px;
  background: #fafbfc; font-size: 11px;
}
.ai-edit-row:hover { background: #f0f1f4; }
.ai-edit-row input[type="checkbox"] { margin-top: 2px; accent-color: #6366f1; }
.ai-edit-row .edit-type {
  display: inline-block; padding: 1px 6px; border-radius: 4px;
  font-size: 9px; font-weight: 600; font-family: 'JetBrains Mono', monospace;
  background: rgba(99,102,241,0.1); color: #6366f1;
}
.ai-edit-row .edit-desc { flex: 1; color: #374151; }
.ai-apply-bar {
  display: flex; gap: 8px; margin-top: 8px; align-items: center;
}
.ai-apply-btn {
  padding: 5px 14px; border: 1px solid #6366f1; border-radius: 6px;
  background: #6366f1; color: #fff; font-size: 11px; font-weight: 600;
  font-family: 'DM Sans', sans-serif; cursor: pointer; transition: all 0.2s;
}
.ai-apply-btn:hover { background: #4f46e5; }
.ai-apply-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.ai-undo-btn {
  padding: 5px 14px; border: 1px solid #e2e4ea; border-radius: 6px;
  background: #fff; color: #374151; font-size: 11px; font-weight: 600;
  font-family: 'DM Sans', sans-serif; cursor: pointer; transition: all 0.2s;
}
.ai-undo-btn:hover { background: #f5f6f8; }

.ai-loading {
  display: flex; align-items: center; gap: 8px;
  margin-top: 10px; padding: 10px 14px;
  background: rgba(99,102,241,0.04); border: 1px solid rgba(99,102,241,0.15);
  border-radius: 8px; font-size: 11px; color: #6366f1;
}
.ai-loading .spinner {
  width: 14px; height: 14px; border: 2px solid rgba(99,102,241,0.2);
  border-top-color: #6366f1; border-radius: 50%;
  animation: ai-spin 0.6s linear infinite;
}
@keyframes ai-spin { to { transform: rotate(360deg); } }
`;

// ---------------------------------------------------------------------------
// Render helpers
// ---------------------------------------------------------------------------

/**
 * Render a compact AI trigger button.
 * @param {string} label   — e.g. "Explain with AI", "Refine recommendation"
 * @param {string} action  — action identifier, stored as data-ai-action
 * @param {number} index   — endpoint/proposal index, stored as data-ai-index
 */
export function renderAiTrigger(label, action, index) {
  return `<button class="ai-trigger" data-ai-action="${action}" data-ai-index="${index}">
    <span class="ai-icon">&#9733;</span> ${escHtml(label)}
  </button>`;
}

/**
 * Render a loading placeholder.
 */
export function renderAiLoading(message = "Analyzing...") {
  return `<div class="ai-loading"><div class="spinner"></div>${escHtml(message)}</div>`;
}

/**
 * Render the full AI-assist result card from a normalized envelope.
 * Renders both the standard envelope fields AND task-specific fields
 * (record_grain, pk_reasoning, likely_causes, column_reasoning, etc.).
 *
 * @param {object} env — normalized envelope with optional task-specific keys
 * @param {object} opts — { title }
 */
export function renderAiCard(env, opts = {}) {
  if (!env) return "";
  const title = opts.title || "AI Analysis";
  const conf = typeof env.confidence === "number" ? env.confidence : 0;
  const confClass = conf >= 0.8 ? "high" : conf >= 0.5 ? "medium" : "low";
  const confLabel = conf >= 0.8 ? "High confidence" : conf >= 0.5 ? "Medium" : "Low";

  // Standard sections
  const recsHtml = (env.recommendations || []).length
    ? `<details><summary>Recommendations (${env.recommendations.length})</summary>
        <ul>${env.recommendations.map(r => `<li>${escHtml(typeof r === "string" ? r : r.text || JSON.stringify(r))}</li>`).join("")}</ul>
       </details>`
    : "";

  const questionsHtml = (env.open_questions || []).length
    ? `<details><summary>Open questions (${env.open_questions.length})</summary>
        <ul>${env.open_questions.map(q => `<li>${escHtml(q)}</li>`).join("")}</ul>
       </details>`
    : "";

  const warningsHtml = (env.warnings || []).length
    ? `<div class="ai-warnings">${env.warnings.map(w => `<div>&#9888; ${escHtml(w)}</div>`).join("")}</div>`
    : "";

  // ── Task-specific sections ──

  // P1-A: Explain Preview — record_grain, pk_reasoning, watermark_reasoning, explode_reasoning, field_notes
  const previewHtml = renderDetailSection("Record grain", env.record_grain)
    + renderDetailSection("PK reasoning", env.pk_reasoning)
    + renderDetailSection("Watermark reasoning", env.watermark_reasoning)
    + renderDetailSection("Explode reasoning", env.explode_reasoning)
    + renderObjectArray("Field notes", env.field_notes, fn => `<strong>${escHtml(fn.field || fn.column || "")}</strong>: ${escHtml(fn.note || fn.reasoning || "")}`);

  // P1-B: Suggest Keys — primary_key_fields, watermark_column, grain_label, alternatives
  let keysHtml = "";
  if (env.primary_key_fields?.length) {
    keysHtml += `<div style="padding:3px 0;"><strong>Primary key:</strong> <code style="font-family:'JetBrains Mono',monospace;font-size:11px;background:#f5f6f8;padding:2px 6px;border-radius:4px;">${escHtml(env.primary_key_fields.join(", "))}</code></div>`;
  }
  if (env.watermark_column) {
    keysHtml += `<div style="padding:3px 0;"><strong>Watermark:</strong> <code style="font-family:'JetBrains Mono',monospace;font-size:11px;background:#f5f6f8;padding:2px 6px;border-radius:4px;">${escHtml(env.watermark_column)}</code></div>`;
  }
  if (env.grain_label) {
    keysHtml += `<div style="padding:3px 0;"><strong>Grain:</strong> ${escHtml(env.grain_label)}</div>`;
  }
  keysHtml += renderObjectArray("Alternatives", env.alternatives, a =>
    `${escHtml((a.fields || []).join(", "))}${a.watermark ? " / WM=" + escHtml(a.watermark) : ""} — ${escHtml(a.reason || "")}`);

  // P1-C: Explain Proposal — business_shape, materialization_reasoning, key_reasoning, column_reasoning
  const proposalHtml = renderDetailSection("Business shape", env.business_shape)
    + renderDetailSection("Materialization reasoning", env.materialization_reasoning)
    + renderDetailSection("Key reasoning", env.key_reasoning)
    + renderObjectArray("Column reasoning", env.column_reasoning, c =>
        `<strong>${escHtml(c.column || c.target_column || "")}</strong>: ${escHtml(c.reasoning || "")}`);

  // P1-D: Explain Validation — likely_causes, suggested_actions
  const validationHtml = renderStringArray("Likely causes", env.likely_causes)
    + renderStringArray("Suggested actions", env.suggested_actions);

  const taskSpecific = previewHtml + keysHtml + proposalHtml + validationHtml;

  const source = env.debug?.source || "";
  const sourceBadge = source
    ? `<span class="ai-source-badge">${escHtml(source.replace(/_/g, " "))}</span>`
    : "";

  return `<div class="ai-card">
    <div class="ai-card-header">
      <span class="ai-card-title">${escHtml(title)}</span>
      <span class="ai-confidence ${confClass}">${confLabel} (${Math.round(conf * 100)}%)</span>
    </div>
    <div class="ai-card-summary">${escHtml(env.summary || "")}</div>
    ${taskSpecific}
    ${recsHtml}
    ${questionsHtml}
    ${warningsHtml}
    ${sourceBadge}
  </div>`;
}

// Render a labeled paragraph if value is non-empty
function renderDetailSection(label, value) {
  if (!value) return "";
  return `<div style="padding:4px 0;"><strong>${escHtml(label)}:</strong> ${escHtml(value)}</div>`;
}

// Render a labeled collapsible list of strings
function renderStringArray(label, arr) {
  if (!arr?.length) return "";
  return `<details><summary>${escHtml(label)} (${arr.length})</summary>
    <ul>${arr.map(s => `<li>${escHtml(s)}</li>`).join("")}</ul></details>`;
}

// Render a labeled collapsible list of objects using a formatter function
function renderObjectArray(label, arr, formatter) {
  if (!arr?.length) return "";
  return `<details><summary>${escHtml(label)} (${arr.length})</summary>
    <ul>${arr.map(item => `<li>${formatter(item)}</li>`).join("")}</ul></details>`;
}

// ---------------------------------------------------------------------------
// Event wiring
// ---------------------------------------------------------------------------

/**
 * Bind AI trigger buttons inside a Shadow root.
 * @param {ShadowRoot} root
 * @param {function} handler — (action, index, buttonEl) => Promise<void>
 */
export function bindAiTriggers(root, handler) {
  root.querySelectorAll(".ai-trigger").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      const action = btn.dataset.aiAction;
      const index = Number(btn.dataset.aiIndex);
      btn.disabled = true;
      try {
        await handler(action, index, btn);
      } finally {
        btn.disabled = false;
      }
    });
  });
}

/**
 * Call an AI endpoint and return the normalized envelope.
 * @param {string} url — e.g. "/aiExplainPreviewSchema"
 * @param {object} body
 * @returns {Promise<object>} — the AI envelope
 */
export async function callAiEndpoint(url, body) {
  return request(url, { method: "POST", body });
}

// ---------------------------------------------------------------------------
// Cherry-pick edits (P2)
// ---------------------------------------------------------------------------

/**
 * Flatten the envelope's `edits` object into a uniform list of edit descriptors.
 * Each entry: { type, description, key, value, category }
 */
export function flattenEdits(edits) {
  if (!edits || typeof edits !== "object") return [];
  const items = [];
  for (const [cat, arr] of Object.entries(edits)) {
    if (cat === "processing_policy" || cat === "materialization") {
      items.push({ type: cat, description: describeEdit(cat, edits[cat]), key: cat, value: edits[cat], category: cat });
      continue;
    }
    if (!Array.isArray(arr)) continue;
    for (const entry of arr) {
      items.push({ type: cat, description: describeEdit(cat, entry), key: cat, value: entry, category: cat });
    }
  }
  return items;
}

function describeEdit(category, entry) {
  const col = entry?.target_column || entry?.column || entry?.source_field || "";
  switch (category) {
    case "column_adds": return `Add column ${col} (${entry?.type || ""})`;
    case "column_modifies": return `Modify ${col}: ${entry?.field || ""} → ${entry?.new_value || ""}`;
    case "column_removes": return `Remove column ${col}`;
    case "mapping_adds": return `Add mapping ${entry?.source_field || ""} → ${col}`;
    case "mapping_modifies": return `Update mapping for ${col}`;
    case "dimensions": return `Add dimension: ${col}`;
    case "measures": return `Add measure: ${col} (${entry?.aggregation || ""})`;
    case "processing_policy": return `Update processing policy`;
    case "materialization": return `Set materialization: ${entry}`;
    default: return `${category}: ${JSON.stringify(entry).slice(0, 80)}`;
  }
}

/**
 * Render the AI card with a cherry-pick edits section and apply/undo bar.
 * Returns HTML string. The caller must bind events via `bindAiEditActions`.
 *
 * @param {object} env — normalized AI envelope (with .edits)
 * @param {object} opts — { title, editContainerId }
 */
export function renderAiEditsCard(env, opts = {}) {
  const baseCard = renderAiCard(env, opts);
  const edits = flattenEdits(env?.edits);
  if (!edits.length) return baseCard;

  const cid = opts.editContainerId || "ai-edit-list";
  let editsHtml = `<div class="ai-edits" id="${cid}">
    <div class="ai-edits-title">Proposed edits (${edits.length})</div>`;
  edits.forEach((edit, i) => {
    editsHtml += `<label class="ai-edit-row">
      <input type="checkbox" checked data-edit-index="${i}" />
      <span class="edit-type">${escHtml(edit.type.replace(/_/g, " "))}</span>
      <span class="edit-desc">${escHtml(edit.description)}</span>
    </label>`;
  });
  editsHtml += `<div class="ai-apply-bar">
    <button class="ai-apply-btn" data-ai-apply>Apply selected</button>
    <button class="ai-undo-btn" data-ai-undo style="display:none;">Undo</button>
  </div></div>`;

  // Insert edits section before the closing </div> of the card
  return baseCard.replace(/<\/div>\s*$/, editsHtml + "</div>");
}

/**
 * Bind apply/undo events inside a container. Call after rendering `renderAiEditsCard`.
 *
 * @param {Element} container — the parent element containing the rendered card
 * @param {object} opts
 *   edits: object[] — the flattened edit array (from flattenEdits), used to resolve
 *          checkbox indices back to actual edit objects
 *   onApply(selectedEdits: object[]) — called with the actual edit objects for checked rows
 *   onUndo() — called when "Undo" is clicked
 */
export function bindAiEditActions(container, { edits, onApply, onUndo }) {
  const applyBtn = container.querySelector("[data-ai-apply]");
  const undoBtn = container.querySelector("[data-ai-undo]");
  if (!applyBtn) return;

  applyBtn.addEventListener("click", () => {
    const checkboxes = container.querySelectorAll(".ai-edit-row input[type=checkbox]");
    const selectedEdits = [];
    checkboxes.forEach(cb => {
      if (cb.checked) {
        const idx = Number(cb.dataset.editIndex);
        if (edits && edits[idx]) selectedEdits.push(edits[idx]);
      }
    });
    if (!selectedEdits.length) return;
    applyBtn.disabled = true;
    if (undoBtn) undoBtn.style.display = "";
    onApply(selectedEdits);
  });

  if (undoBtn) {
    undoBtn.addEventListener("click", () => {
      undoBtn.style.display = "none";
      applyBtn.disabled = false;
      onUndo();
    });
  }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function escHtml(s) {
  if (typeof s !== "string") return String(s ?? "");
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
