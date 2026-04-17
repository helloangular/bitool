const TRANSFORM_CHAT_TEMPLATE = `
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
    position: fixed; right: 20px; bottom: 20px; top: calc(var(--toolbar-h, 0px) + 20px);
    width: 980px; min-width: 380px; max-width: 90vw;
    z-index: 9999;
    background: #ffffff;
    border: 1px solid #e2e4ea;
    border-radius: 14px;
    box-shadow: 0 8px 40px rgba(0,0,0,0.12), 0 0 0 1px rgba(0,0,0,0.04);
    display: flex; flex-direction: column; overflow: hidden;
    font-family: 'DM Sans', -apple-system, sans-serif; color: #1a1d26;
  }

  .chat-resize-handle {
    position: absolute; left: -4px; top: 0; bottom: 0; width: 8px;
    cursor: ew-resize; z-index: 1;
  }
  .chat-resize-handle:hover { background: rgba(10,151,104,0.08); border-radius: 4px; }

  .chat-header {
    padding: 14px 20px;
    border-bottom: 1px solid #eceef2;
    display: flex; justify-content: space-between; align-items: center;
    background: rgba(255,255,255,0.92);
    backdrop-filter: blur(12px);
  }
  .chat-header-left { display: flex; align-items: center; gap: 10px; }
  .chat-logo {
    width: 28px; height: 28px; border-radius: 6px;
    background: linear-gradient(135deg, #0fa968, #3b7ddd);
    display: flex; align-items: center; justify-content: center;
    font-size: 13px; font-weight: 700; color: white;
  }
  .chat-title { font-size: 14px; font-weight: 600; }
  .chat-subtitle { font-size: 10.5px; color: #8b91a3; font-weight: 500; }
  .close-btn {
    width: 28px; height: 28px; border-radius: 6px; border: 1px solid #e2e4ea;
    background: #f5f6f8; color: #5c6070; font-size: 16px;
    cursor: pointer; display: flex; align-items: center; justify-content: center;
  }
  .close-btn:hover { background: #e2e4ea; color: #1a1d26; }

  .chat-messages {
    flex: 1; overflow-y: auto; padding: 16px 20px;
    display: flex; flex-direction: column; gap: 14px;
  }

  .msg {
    max-width: 94%; padding: 12px 16px; border-radius: 10px;
    line-height: 1.6; white-space: pre-wrap; font-size: 12.5px;
  }
  .msg.user {
    align-self: flex-end;
    background: #0fa968; color: white;
    border-bottom-right-radius: 4px; font-weight: 500;
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

  .welcome { text-align: center; padding: 20px 0; }
  .welcome-icon {
    width: 44px; height: 44px; margin: 0 auto 12px;
    background: linear-gradient(135deg, rgba(15,169,104,0.1), rgba(59,125,221,0.1));
    border-radius: 12px; display: flex; align-items: center; justify-content: center;
    font-size: 22px;
  }
  .welcome h3 { font-size: 14px; font-weight: 600; margin-bottom: 6px; }
  .welcome p { font-size: 12px; color: #5c6070; line-height: 1.6; max-width: 420px; margin: 0 auto; }
  .suggestions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 14px; justify-content: center; }
  .suggestion {
    padding: 6px 12px; border-radius: 20px; font-size: 11px; font-weight: 500;
    background: rgba(15,169,104,0.06); border: 1px solid rgba(15,169,104,0.15);
    color: #0fa968; cursor: pointer;
  }
  .suggestion:hover { background: rgba(15,169,104,0.12); border-color: #0fa968; }

  .preview-block {
    background: #f9fafb; border: 1px solid #e2e4ea; border-radius: 8px;
    padding: 12px 14px; margin-top: 10px;
    font-family: 'JetBrains Mono', monospace; font-size: 11px;
    white-space: pre-wrap; overflow-x: auto; max-height: 360px; overflow-y: auto;
    line-height: 1.7;
  }

  details {
    margin-top: 10px;
    border: 1px solid #e2e4ea;
    border-radius: 8px;
    background: #fff;
  }
  summary {
    cursor: pointer; list-style: none; padding: 10px 12px; font-size: 11.5px; font-weight: 600;
  }
  details pre {
    margin: 0; padding: 0 12px 12px; overflow: auto;
    font-family: 'JetBrains Mono', monospace; font-size: 11px; white-space: pre-wrap;
  }

  .badge {
    display: inline-flex; align-items: center; gap: 5px;
    padding: 3px 10px; border-radius: 10px; font-size: 10px;
    font-weight: 600; font-family: 'JetBrains Mono', monospace; margin-top: 8px;
    background: rgba(15,169,104,0.1); color: #0fa968;
  }
  .badge.warn { background: rgba(217,119,6,0.1); color: #d97706; }

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

  .action-row { display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; }
  .btn {
    height: 30px; padding: 0 14px; border-radius: 6px;
    border: 1px solid #e2e4ea; background: #fff; color: #5c6070;
    font-size: 11.5px; font-weight: 500; cursor: pointer;
  }
  .btn:hover { background: #f0f1f4; color: #1a1d26; border-color: #8b91a3; }
  .btn-apply {
    background: #0fa968; border-color: #0fa968; color: white; font-weight: 600;
  }
  .btn-apply:hover { background: #0d9a5e; border-color: #0d9a5e; }

  .chat-input-row {
    padding: 14px 20px; border-top: 1px solid #eceef2;
    display: flex; gap: 10px; background: #fafbfc;
  }
  .chat-input-row textarea {
    flex: 1; border: 1px solid #e2e4ea; border-radius: 8px;
    padding: 10px 14px; font-size: 12.5px; color: #1a1d26;
    resize: none; min-height: 42px; max-height: 100px; outline: none;
    font-family: 'DM Sans', sans-serif;
  }
  .chat-input-row textarea:focus { border-color: #0fa968; box-shadow: 0 0 0 3px rgba(15,169,104,0.1); }
  .chat-input-row textarea::placeholder { color: #8b91a3; }
  .send-btn {
    height: 42px; padding: 0 18px; border-radius: 8px;
    background: #0fa968; border: none; color: white;
    font-size: 12.5px; font-weight: 600; cursor: pointer;
    display: flex; align-items: center; gap: 6px;
  }
  .send-btn:hover { background: #0d9a5e; }
  .send-btn:disabled { opacity: 0.4; cursor: default; }

  .typing { color: #8b91a3; font-style: italic; font-size: 11.5px; }
</style>

<div class="chat-backdrop"></div>
<div class="chat-panel">
  <div class="chat-resize-handle" id="resizeHandle"></div>
  <div class="chat-header">
    <div class="chat-header-left">
      <div class="chat-logo">G</div>
      <div>
        <div class="chat-title">Graph Builder</div>
        <div class="chat-subtitle">Text to transform graph</div>
      </div>
    </div>
    <button class="close-btn" id="closeChat">&times;</button>
  </div>
  <div class="chat-messages" id="chatMessages">
    <div class="msg system">
      <div class="welcome">
        <div class="welcome-icon">&#9881;</div>
        <h3>Generate a transform graph from text</h3>
        <p>Describe the transform logic you want. I’ll generate a graph preview with Logic and Conditional nodes, then you can apply it to the canvas.</p>
        <div class="suggestions">
          <span class="suggestion" data-text="Create an API flow that normalizes customer name and amount, classifies VIP customers, and returns a discount and score.">Customer decision</span>
          <span class="suggestion" data-text="Build an invoice review transform that routes missing PO numbers to exceptions and computes payment priority.">Invoice review</span>
          <span class="suggestion" data-text="Generate a lead scoring transform that routes executive leads to account executives.">Lead routing</span>
        </div>
      </div>
    </div>
  </div>
  <div class="chat-input-row">
    <textarea id="chatInput" placeholder="Describe your transform graph..." rows="2"></textarea>
    <button class="send-btn" id="chatSend">Send</button>
  </div>
</div>
`;

class TransformChatComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._currentGil = null;
  }

  connectedCallback() {
    this.shadowRoot.innerHTML = TRANSFORM_CHAT_TEMPLATE;
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
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        this._send();
      }
    });

    this.shadowRoot.querySelectorAll(".suggestion").forEach((suggestion) => {
      suggestion.addEventListener("click", () => {
        this._input.value = suggestion.dataset.text || "";
        this._send();
      });
    });

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

  open() {
    this.setAttribute("open", "");
    this._input.focus();
  }

  close() {
    this.removeAttribute("open");
  }

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

  _makeDetails(summaryText, payload, { open = false } = {}) {
    const details = document.createElement("details");
    if (open) details.open = true;

    const summary = document.createElement("summary");
    summary.textContent = summaryText;
    details.appendChild(summary);

    const pre = document.createElement("pre");
    pre.textContent = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
    details.appendChild(pre);

    return details;
  }

  _addPreviewMessage(data) {
    const div = document.createElement("div");
    div.className = "msg system";

    const header = document.createElement("div");
    header.textContent = "Transform graph ready";
    header.style.cssText = "font-weight:600; font-size:13px; margin-bottom:10px;";
    div.appendChild(header);

    const badge = document.createElement("span");
    const warningCount = (data.validation && data.validation.warnings && data.validation.warnings.length) || 0;
    badge.className = warningCount ? "badge warn" : "badge";
    badge.textContent = warningCount ? `Valid with ${warningCount} warning(s)` : "Valid graph";
    div.appendChild(badge);

    const pre = document.createElement("div");
    pre.className = "preview-block";
    pre.textContent = data.text || "";
    div.appendChild(pre);

    if (data.preview && Array.isArray(data.preview.assumptions) && data.preview.assumptions.length) {
      const ul = document.createElement("ul");
      ul.className = "assumptions";
      for (const assumption of data.preview.assumptions) {
        const li = document.createElement("li");
        li.textContent = assumption;
        ul.appendChild(li);
      }
      div.appendChild(ul);
    }

    if (data.gil) {
      div.appendChild(this._makeDetails("Generated GIL", data.gil));
    }

    if (warningCount) {
      div.appendChild(this._makeDetails("Warnings", data.validation.warnings));
    }

    const actions = document.createElement("div");
    actions.className = "action-row";

    const applyBtn = document.createElement("button");
    applyBtn.className = "btn btn-apply";
    applyBtn.textContent = "Apply Graph";
    applyBtn.addEventListener("click", () => this._applyCurrentGraph());
    actions.appendChild(applyBtn);

    const reuseBtn = document.createElement("button");
    reuseBtn.className = "btn";
    reuseBtn.textContent = "Use Prompt Again";
    reuseBtn.addEventListener("click", () => this._input.focus());
    actions.appendChild(reuseBtn);

    div.appendChild(actions);
    this._messages.appendChild(div);
    this._messages.scrollTop = this._messages.scrollHeight;
  }

  _showValidationErrors(payload) {
    const errors = payload && payload.data && payload.data.validation && payload.data.validation.errors;
    if (errors && errors.length) {
      const details = this._makeDetails("Validation errors", errors, { open: true });
      this._addMessage("Generated graph is invalid.", "system error", details);
      return;
    }
    this._addMessage(`Error: ${payload && (payload.message || payload.error) || "Unknown error"}`, "system error");
  }

  async _send() {
    const text = this._input.value.trim();
    if (!text) return;

    this._input.value = "";
    this._addMessage(text, "user");
    this._sendBtn.disabled = true;

    const typing = this._addMessage("Planning transform graph...", "system");
    typing.classList.add("typing");

    try {
      const resp = await fetch("/transform/from-nl", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text })
      });
      const json = await resp.json();
      typing.remove();

      if (!json.ok) {
        this._showValidationErrors(json);
        return;
      }

      this._currentGil = json.data.gil;
      this._addPreviewMessage(json.data);
    } catch (e) {
      typing.remove();
      this._addMessage("Error: " + e.message, "system error");
    } finally {
      this._sendBtn.disabled = false;
      this._input.focus();
    }
  }

  async _applyCurrentGraph() {
    if (!this._currentGil) {
      this._addMessage("No graph proposal to apply yet.", "system error");
      return;
    }

    const typing = this._addMessage("Applying graph...", "system");
    typing.classList.add("typing");

    try {
      const resp = await fetch("/transform/apply", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ gil: this._currentGil })
      });
      const json = await resp.json();
      typing.remove();

      if (!json.ok) {
        this._showValidationErrors(json);
        return;
      }

      const result = json.data.result || {};
      if (result.panel) {
        const panelItems = Array.isArray(result.panel) ? result.panel : [result.panel];
        window.data.panelItems = panelItems;
        window.data.gid = result.graph_id || result["graph-id"] || result.graphId || null;
        window.data.ver = result.version || result.graph_version || result["graph-version"] || window.data.ver || null;
        localStorage.setItem("jqdata", JSON.stringify(window.data));

        const tree = document.querySelector("tree-component");
        if (tree) {
          if (typeof tree.renderItems === "function") {
            tree.renderItems();
          } else if (typeof tree.renderRectangles === "function") {
            tree.renderRectangles(panelItems);
          }
        }
      }

      this._addMessage(
        `Graph applied as #${result.graph_id || result["graph-id"] || "unknown"}.\nThe canvas has been updated with the generated graph.`,
        "system"
      );
      this.close();
    } catch (e) {
      typing.remove();
      this._addMessage("Apply failed: " + e.message, "system error");
    }
  }
}

customElements.define("transform-chat", TransformChatComponent);

export default TransformChatComponent;
