/**
 * Pipeline Chat Component
 *
 * A floating chat panel for intent-based pipeline creation.
 * User types natural language, system returns a preview, user approves or edits.
 */

const CHAT_TEMPLATE = `
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  :host { display: none; }
  :host([open]) { display: block; }

  * { box-sizing: border-box; }

  .chat-backdrop {
    position: fixed; inset: 0; z-index: 9998;
    background: rgba(26,29,38,0.18);
    backdrop-filter: blur(2px);
  }

  .chat-panel {
    position: fixed; right: 20px; bottom: 20px; top: 60px;
    width: 520px; min-width: 380px; max-width: 90vw;
    z-index: 9999;
    background: #ffffff;
    border: 1px solid #e2e4ea;
    border-radius: 14px;
    box-shadow: 0 8px 40px rgba(0,0,0,0.12), 0 0 0 1px rgba(0,0,0,0.04);
    display: flex; flex-direction: column;
    font-family: 'DM Sans', -apple-system, sans-serif;
    font-size: 13px; color: #1a1d26;
    overflow: hidden;
  }

  .chat-resize-handle {
    position: absolute; left: -4px; top: 0; bottom: 0; width: 8px;
    cursor: ew-resize; z-index: 1;
  }
  .chat-resize-handle:hover { background: rgba(59,125,221,0.08); border-radius: 4px; }

  /* ── Header ── */
  .chat-header {
    padding: 14px 20px;
    border-bottom: 1px solid #eceef2;
    display: flex; justify-content: space-between; align-items: center;
    background: rgba(255,255,255,0.9);
    backdrop-filter: blur(12px);
  }
  .chat-header-left {
    display: flex; align-items: center; gap: 10px;
  }
  .chat-logo {
    width: 28px; height: 28px; border-radius: 6px;
    background: linear-gradient(135deg, #3b7ddd, #0ea5c7);
    display: flex; align-items: center; justify-content: center;
    font-size: 13px; font-weight: 700; color: white;
  }
  .chat-title { font-size: 14px; font-weight: 600; color: #1a1d26; }
  .chat-subtitle { font-size: 10.5px; color: #8b91a3; font-weight: 500; }
  .close-btn {
    width: 28px; height: 28px; border-radius: 6px; border: 1px solid #e2e4ea;
    background: #f5f6f8; color: #5c6070; font-size: 16px;
    cursor: pointer; display: flex; align-items: center; justify-content: center;
    transition: 0.2s;
  }
  .close-btn:hover { background: #e2e4ea; color: #1a1d26; }

  /* ── Messages ── */
  .chat-messages {
    flex: 1; overflow-y: auto; padding: 16px 20px;
    display: flex; flex-direction: column; gap: 14px;
  }
  .chat-messages::-webkit-scrollbar { width: 5px; }
  .chat-messages::-webkit-scrollbar-thumb { background: #e2e4ea; border-radius: 3px; }

  .msg {
    max-width: 94%; padding: 12px 16px; border-radius: 10px;
    line-height: 1.6; white-space: pre-wrap; font-size: 12.5px;
  }
  .msg.user {
    align-self: flex-end;
    background: #3b7ddd; color: white;
    border-bottom-right-radius: 4px;
    font-weight: 500;
  }
  .msg.system {
    align-self: flex-start;
    background: #f5f6f8; color: #1a1d26;
    border: 1px solid #eceef2;
    border-bottom-left-radius: 4px;
  }
  .msg.error {
    background: rgba(229,72,77,0.08); color: #e5484d;
    border-color: rgba(229,72,77,0.2);
  }

  /* ── Preview Block ── */
  .preview-block {
    background: #f9fafb; border: 1px solid #e2e4ea; border-radius: 8px;
    padding: 12px 14px; margin-top: 10px;
    font-family: 'JetBrains Mono', monospace; font-size: 11px;
    white-space: pre; overflow-x: auto; max-height: 420px; overflow-y: auto;
    color: #1a1d26; line-height: 1.7;
  }

  /* ── Coverage Badge ── */
  .coverage-badge {
    display: inline-flex; align-items: center; gap: 5px;
    padding: 3px 10px; border-radius: 10px; font-size: 10px;
    font-weight: 600; font-family: 'JetBrains Mono', monospace;
    margin-top: 8px;
  }
  .coverage-ready { background: rgba(15,169,104,0.1); color: #0fa968; }
  .coverage-partial { background: rgba(217,119,6,0.1); color: #d97706; }
  .coverage-blocked { background: rgba(229,72,77,0.1); color: #e5484d; }

  /* ── Assumptions ── */
  .assumptions {
    margin-top: 10px; padding-left: 0; list-style: none; font-size: 11px; color: #5c6070;
  }
  .assumptions li {
    padding: 3px 0; display: flex; align-items: flex-start; gap: 6px;
  }
  .assumptions li::before {
    content: ''; display: inline-block; width: 5px; height: 5px;
    background: #8b91a3; border-radius: 50%; margin-top: 5px; flex-shrink: 0;
  }

  /* ── Action Buttons ── */
  .action-row {
    display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap;
  }
  .btn {
    height: 30px; padding: 0 14px; border-radius: 6px;
    border: 1px solid #e2e4ea; background: #fff; color: #5c6070;
    font-family: 'DM Sans', sans-serif; font-size: 11.5px; font-weight: 500;
    cursor: pointer; display: inline-flex; align-items: center; gap: 5px;
    transition: 0.2s; white-space: nowrap;
  }
  .btn:hover { background: #f0f1f4; color: #1a1d26; border-color: #8b91a3; }
  .btn-apply {
    background: #3b7ddd; border-color: #3b7ddd; color: white; font-weight: 600;
  }
  .btn-apply:hover { background: #2d6bc4; border-color: #2d6bc4; }
  .btn-green {
    background: #0fa968; border-color: #0fa968; color: white; font-weight: 600;
  }
  .btn-green:hover { background: #0d9a5e; }
  .btn-sdp {
    background: rgba(124,92,252,0.08); border-color: rgba(124,92,252,0.2); color: #7c5cfc;
  }
  .btn-sdp:hover { background: rgba(124,92,252,0.15); }

  /* ── Input Area ── */
  .chat-input-row {
    padding: 14px 20px;
    border-top: 1px solid #eceef2;
    display: flex; gap: 10px;
    background: #fafbfc;
  }
  .chat-input-row textarea {
    flex: 1;
    border: 1px solid #e2e4ea; border-radius: 8px;
    padding: 10px 14px;
    font-size: 12.5px; font-family: 'DM Sans', sans-serif;
    color: #1a1d26; background: #fff;
    resize: none; min-height: 42px; max-height: 100px;
    outline: none; transition: 0.2s;
  }
  .chat-input-row textarea::placeholder { color: #8b91a3; }
  .chat-input-row textarea:focus { border-color: #3b7ddd; box-shadow: 0 0 0 3px rgba(59,125,221,0.1); }
  .send-btn {
    height: 42px; padding: 0 18px; border-radius: 8px;
    background: #3b7ddd; border: none; color: white;
    font-family: 'DM Sans', sans-serif; font-size: 12.5px; font-weight: 600;
    cursor: pointer; white-space: nowrap; transition: 0.2s;
    display: flex; align-items: center; gap: 6px;
  }
  .send-btn:hover { background: #2d6bc4; }
  .send-btn:disabled { opacity: 0.4; cursor: default; }
  .send-btn svg { width: 14px; height: 14px; }

  .typing { color: #8b91a3; font-style: italic; font-size: 11.5px; }

  /* ── Welcome ── */
  .welcome { text-align: center; padding: 20px 0; }
  .welcome-icon {
    width: 44px; height: 44px; margin: 0 auto 12px;
    background: linear-gradient(135deg, rgba(59,125,221,0.1), rgba(14,165,199,0.1));
    border-radius: 12px; display: flex; align-items: center; justify-content: center;
    font-size: 22px;
  }
  .welcome h3 { font-size: 14px; font-weight: 600; margin-bottom: 6px; color: #1a1d26; }
  .welcome p { font-size: 12px; color: #5c6070; line-height: 1.6; max-width: 340px; margin: 0 auto; }
  .welcome em { color: #3b7ddd; font-style: normal; font-weight: 500; }

  /* ── Suggestions ── */
  .suggestions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 14px; justify-content: center; }
  .suggestion {
    padding: 6px 12px; border-radius: 20px; font-size: 11px; font-weight: 500;
    background: rgba(59,125,221,0.06); border: 1px solid rgba(59,125,221,0.15);
    color: #3b7ddd; cursor: pointer; transition: 0.2s;
  }
  .suggestion:hover { background: rgba(59,125,221,0.12); border-color: #3b7ddd; }
</style>

<div class="chat-backdrop"></div>
<div class="chat-panel">
  <div class="chat-resize-handle" id="resizeHandle"></div>
  <div class="chat-header">
    <div class="chat-header-left">
      <div class="chat-logo">P</div>
      <div>
        <div class="chat-title">Pipeline Builder</div>
        <div class="chat-subtitle">Intent-based medallion pipeline creation</div>
      </div>
    </div>
    <button class="close-btn" id="closeChat">&times;</button>
  </div>
  <div class="chat-messages" id="chatMessages">
    <div class="msg system">
      <div class="welcome">
        <div class="welcome-icon">&#9889;</div>
        <h3>Build a pipeline from a sentence</h3>
        <p>Describe what data you want to ingest, transform, and analyze. I'll generate the full Bronze, Silver, and Gold pipeline.</p>
        <div class="suggestions">
          <span class="suggestion" data-text="Pull Samsara fleet/vehicles and fleet/drivers into Bronze on Databricks, create vehicle and driver Silver tables, and build daily fleet utilization Gold">Fleet analytics</span>
          <span class="suggestion" data-text="Ingest Samsara fleet/vehicles into Bronze on Databricks">Quick vehicle ingest</span>
          <span class="suggestion" data-text="Build driver safety analytics from Samsara with daily safety scores">Safety analytics</span>
        </div>
      </div>
    </div>
  </div>
  <div class="chat-input-row">
    <textarea id="chatInput" placeholder="Describe your pipeline..." rows="2"></textarea>
    <button class="send-btn" id="chatSend">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
      Send
    </button>
  </div>
</div>
`;

class PipelineChatComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._currentSpec = null;
  }

  connectedCallback() {
    this.shadowRoot.innerHTML = CHAT_TEMPLATE;
    this._messages = this.shadowRoot.getElementById("chatMessages");
    this._input = this.shadowRoot.getElementById("chatInput");
    this._sendBtn = this.shadowRoot.getElementById("chatSend");
    this._closeBtn = this.shadowRoot.getElementById("closeChat");
    this._backdrop = this.shadowRoot.querySelector(".chat-backdrop");
    this._panel = this.shadowRoot.querySelector(".chat-panel");
    this._resizeHandle = this.shadowRoot.getElementById("resizeHandle");

    this._closeBtn.addEventListener("click", () => this.close());
    this._backdrop.addEventListener("click", () => this.close());
    this._sendBtn.addEventListener("click", () => this._send());
    this._input.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); this._send(); }
    });

    // Suggestion pills
    this.shadowRoot.querySelectorAll(".suggestion").forEach(s => {
      s.addEventListener("click", () => {
        this._input.value = s.dataset.text;
        this._send();
      });
    });

    // Resize drag
    this._resizeHandle.addEventListener("pointerdown", (e) => {
      e.preventDefault();
      const startX = e.clientX;
      const startWidth = this._panel.offsetWidth;
      const onMove = (ev) => {
        const delta = startX - ev.clientX;
        const newWidth = Math.max(360, Math.min(window.innerWidth * 0.9, startWidth + delta));
        this._panel.style.width = newWidth + "px";
      };
      const onUp = () => {
        document.removeEventListener("pointermove", onMove);
        document.removeEventListener("pointerup", onUp);
      };
      document.addEventListener("pointermove", onMove);
      document.addEventListener("pointerup", onUp);
    });
  }

  open() { this.setAttribute("open", ""); this._input.focus(); }
  close() { this.removeAttribute("open"); }

  _addMessage(text, type = "system", extra = null) {
    const div = document.createElement("div");
    div.className = `msg ${type}`;
    if (typeof text === "string") {
      div.textContent = text;
    }
    if (extra) div.appendChild(extra);
    this._messages.appendChild(div);
    this._messages.scrollTop = this._messages.scrollHeight;
    return div;
  }

  _addPreviewMessage(data) {
    const div = document.createElement("div");
    div.className = "msg system";

    // Header
    const header = document.createElement("div");
    header.textContent = "Pipeline plan ready";
    header.style.cssText = "font-weight:600; font-size:13px; margin-bottom:10px;";
    div.appendChild(header);

    // Coverage badge
    const cov = data.preview?.coverage;
    if (cov?.status) {
      const badge = document.createElement("span");
      badge.className = "coverage-badge " + (cov.status === "ready" ? "coverage-ready" : cov.status === "partial" ? "coverage-partial" : "coverage-blocked");
      badge.textContent = cov.status === "ready" ? "Coverage: Ready" : cov.status === "partial" ? "Coverage: Partial" : "Coverage: " + cov.status;
      div.appendChild(badge);
    }

    // Preview block
    const pre = document.createElement("div");
    pre.className = "preview-block";
    pre.textContent = data.text || JSON.stringify(data.preview, null, 2);
    div.appendChild(pre);

    // Assumptions
    if (data.preview?.assumptions?.length) {
      const ul = document.createElement("ul");
      ul.className = "assumptions";
      for (const a of data.preview.assumptions) {
        const li = document.createElement("li");
        li.textContent = a;
        ul.appendChild(li);
      }
      div.appendChild(ul);
    }

    // Action buttons
    const actions = document.createElement("div");
    actions.className = "action-row";

    const applyBtn = document.createElement("button");
    applyBtn.className = "btn btn-apply";
    applyBtn.textContent = "Apply Pipeline";
    applyBtn.addEventListener("click", () => this._applyPipeline(data.spec));
    actions.appendChild(applyBtn);

    const sdpBtn = document.createElement("button");
    sdpBtn.className = "btn btn-sdp";
    sdpBtn.textContent = "Show SDP SQL";
    sdpBtn.addEventListener("click", () => this._showSdp(data.spec));
    actions.appendChild(sdpBtn);

    const editBtn = document.createElement("button");
    editBtn.className = "btn";
    editBtn.textContent = "Edit";
    editBtn.addEventListener("click", () => { this._input.focus(); this._input.placeholder = "Describe changes (e.g. make utilization weekly, add fleet/trips)..."; });
    actions.appendChild(editBtn);

    div.appendChild(actions);
    this._messages.appendChild(div);
    this._messages.scrollTop = this._messages.scrollHeight;
  }

  async _send() {
    const text = this._input.value.trim();
    if (!text) return;

    this._input.value = "";
    this._addMessage(text, "user");
    this._sendBtn.disabled = true;

    const typing = this._addMessage(
      this._currentSpec ? "Updating pipeline..." : "Planning pipeline...",
      "system"
    );
    typing.classList.add("typing");

    try {
      const isEdit = !!this._currentSpec;
      const url = isEdit ? "/pipeline/edit" : "/pipeline/from-nl";
      const body = isEdit
        ? { spec: this._currentSpec, text }
        : { text, use_mock: false };

      const resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const json = await resp.json();
      typing.remove();

      if (!json.ok) {
        this._addMessage("Error: " + (json.message || json.error), "system error");
        return;
      }

      this._currentSpec = json.data.spec;
      this._addPreviewMessage(json.data);
    } catch (e) {
      typing.remove();
      this._addMessage("Error: " + e.message, "system error");
    } finally {
      this._sendBtn.disabled = false;
      this._input.focus();
    }
  }

  async _applyPipeline(spec) {
    const typing = this._addMessage("Creating pipeline...", "system");
    typing.classList.add("typing");

    try {
      // Prompt for connection_id
      const connId = prompt("Enter the Databricks connection ID (from saved connections):");
      if (!connId) { typing.remove(); return; }

      const resp = await fetch("/pipeline/apply", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ spec, connection_id: parseInt(connId) }),
      });
      const json = await resp.json();
      typing.remove();

      if (!json.ok) {
        this._addMessage("Apply failed: " + (json.message || json.error), "system error");
        return;
      }

      const r = json.data;
      let msg = "Pipeline created:\n";
      if (r.bronze) {
        msg += `  Bronze graph #${r.bronze.graph_id} (v${r.bronze.graph_version})\n`;
        msg += `  API node: ${r.bronze.api_node_id}, Target node: ${r.bronze.target_node_id}\n`;
      }
      if (r.silver?.length) {
        msg += `  Silver proposals: ${r.silver.map(s => s.target_model).join(", ")} (${r.silver[0].status})\n`;
      }
      if (r.gold?.length) {
        msg += `  Gold proposals: ${r.gold.map(g => g.target_model).join(", ")} (${r.gold[0].status})\n`;
      }
      msg += "\nNothing has been executed. Use 'Run' in the API node or type 'run bronze' here.";
      this._addMessage(msg, "system");
    } catch (e) {
      typing.remove();
      this._addMessage("Apply failed: " + e.message, "system error");
    }
  }

  async _showSdp(spec) {
    try {
      const resp = await fetch("/pipeline/generate-sdp", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(spec),
      });
      const json = await resp.json();
      if (!json.ok) {
        this._addMessage("SDP generation failed: " + (json.message || json.error), "system error");
        return;
      }
      const pre = document.createElement("div");
      pre.className = "preview-block";
      pre.textContent = json.data.combined || "No SDP generated.";

      const div = document.createElement("div");
      div.className = "msg system";
      const header = document.createElement("div");
      header.textContent = "Databricks SDP SQL (export artifact):";
      header.style.fontWeight = "600";
      div.appendChild(header);
      div.appendChild(pre);
      this._messages.appendChild(div);
      this._messages.scrollTop = this._messages.scrollHeight;
    } catch (e) {
      this._addMessage("SDP generation failed: " + e.message, "system error");
    }
  }
}

customElements.define("pipeline-chat", PipelineChatComponent);

export default PipelineChatComponent;
