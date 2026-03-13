import { request } from "./library/utils.js";
import { setPanelItems } from "./library/utils.js";

// ---------------------------------------------------------------------------
// OpenAPI Import Modal
// ---------------------------------------------------------------------------
// Accepts JSON or YAML (YAML is converted via js-yaml loaded lazily from CDN).

let modal = null;

function getModal() {
  if (modal) return modal;

  modal = document.createElement("div");
  modal.id = "openapi-import-modal";
  modal.style.cssText = `
    display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5);
    z-index:9999; align-items:center; justify-content:center;
  `;

  modal.innerHTML = `
    <div style="background:#fff; border-radius:8px; padding:24px; width:600px;
                max-height:80vh; display:flex; flex-direction:column; gap:12px;
                box-shadow:0 8px 32px rgba(0,0,0,0.2);">
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <h3 style="margin:0; font-size:16px;">Import OpenAPI Spec</h3>
        <button id="oai-close" style="border:none;background:none;font-size:20px;cursor:pointer;">&#x2715;</button>
      </div>

      <div style="display:flex; flex-direction:column; gap:6px;">
        <label style="font-size:13px; font-weight:600;">Graph Name</label>
        <input id="oai-name" type="text" placeholder="My Imported API"
          style="border:1px solid #ddd; border-radius:4px; padding:6px 8px; font-size:13px;" />
      </div>

      <div style="display:flex; flex-direction:column; gap:6px;">
        <label style="font-size:13px; font-weight:600;">Import from URL</label>
        <div style="display:flex; gap:6px;">
          <input id="oai-url" type="text" placeholder="https://petstore3.swagger.io/api/v3/openapi.json"
            style="flex:1; border:1px solid #ddd; border-radius:4px; padding:6px 8px; font-size:13px;" />
          <button id="oai-fetch" style="padding:6px 12px; border:1px solid #2563eb; border-radius:4px;
            background:#eff6ff; color:#2563eb; cursor:pointer; font-size:13px; white-space:nowrap;">Fetch</button>
        </div>
      </div>

      <div style="display:flex; align-items:center; gap:8px;">
        <hr style="flex:1; border:none; border-top:1px solid #eee;" />
        <span style="font-size:11px; color:#999;">or paste below</span>
        <hr style="flex:1; border:none; border-top:1px solid #eee;" />
      </div>

      <div style="display:flex; flex-direction:column; gap:6px; flex:1; min-height:0;">
        <label style="font-size:13px; font-weight:600;">Paste spec (JSON or YAML)</label>
        <textarea id="oai-spec" placeholder='{"openapi":"3.0.0","info":{"title":"..."},"paths":{...}}'
          style="flex:1; min-height:200px; border:1px solid #ddd; border-radius:4px;
                 padding:8px; font-size:12px; font-family:monospace; resize:vertical;"></textarea>
      </div>

      <div style="display:flex; gap:8px; justify-content:flex-end; align-items:center;">
        <span id="oai-error" style="color:red; font-size:12px; flex:1;"></span>
        <button id="oai-cancel" style="padding:6px 16px; border:1px solid #ddd;
          border-radius:4px; background:#fff; cursor:pointer; font-size:13px;">Cancel</button>
        <button id="oai-import" style="padding:6px 16px; border:none; border-radius:4px;
          background:#2563eb; color:#fff; cursor:pointer; font-size:13px;">Import</button>
      </div>
    </div>
  `;

  document.body.appendChild(modal);

  modal.querySelector("#oai-close").addEventListener("click", closeModal);
  modal.querySelector("#oai-cancel").addEventListener("click", closeModal);
  modal.addEventListener("click", (e) => { if (e.target === modal) closeModal(); });
  modal.querySelector("#oai-fetch").addEventListener("click", doFetchUrl);
  modal.querySelector("#oai-import").addEventListener("click", doImport);

  return modal;
}

function openModal() {
  const m = getModal();
  m.style.display = "flex";
  m.querySelector("#oai-name").value = "";
  m.querySelector("#oai-url").value = "";
  m.querySelector("#oai-spec").value = "";
  m.querySelector("#oai-error").textContent = "";
}

async function doFetchUrl() {
  const m = getModal();
  const errorEl = m.querySelector("#oai-error");
  const urlInput = m.querySelector("#oai-url");
  const fetchBtn = m.querySelector("#oai-fetch");
  const specArea = m.querySelector("#oai-spec");
  const url = urlInput.value.trim();

  errorEl.textContent = "";
  if (!url) { errorEl.textContent = "Enter a URL first."; return; }

  fetchBtn.disabled = true;
  fetchBtn.textContent = "Fetching...";

  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const text = await resp.text();
    specArea.value = text;

    // Auto-fill graph name from spec title if empty
    const nameInput = m.querySelector("#oai-name");
    if (!nameInput.value.trim()) {
      try {
        const json = JSON.parse(text);
        if (json?.info?.title) nameInput.value = json.info.title;
      } catch (_) {}
    }
  } catch (err) {
    errorEl.textContent = `Failed to fetch: ${err.message}`;
  } finally {
    fetchBtn.disabled = false;
    fetchBtn.textContent = "Fetch";
  }
}

function closeModal() {
  if (modal) modal.style.display = "none";
}

async function specToJson(raw) {
  const trimmed = raw.trim();
  // Try JSON first
  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    return trimmed;
  }
  // YAML — load js-yaml lazily
  if (!window.jsyaml) {
    await new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = "https://cdn.jsdelivr.net/npm/js-yaml@4/dist/js-yaml.min.js";
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }
  const parsed = window.jsyaml.load(trimmed);
  return JSON.stringify(parsed);
}

async function doImport() {
  const m = getModal();
  const errorEl = m.querySelector("#oai-error");
  const importBtn = m.querySelector("#oai-import");
  const raw = m.querySelector("#oai-spec").value.trim();
  const name = m.querySelector("#oai-name").value.trim() || "Imported API";

  errorEl.textContent = "";
  if (!raw) { errorEl.textContent = "Paste a spec or fetch from a URL first."; return; }

  importBtn.disabled = true;
  importBtn.textContent = "Importing…";

  try {
    const specJson = await specToJson(raw);
    const data = await request("/importOpenApi", {
      method: "POST",
      body: { spec: specJson, graph_name: name },
    });
    const panel = data?.cp || data?.sp || data;
    setPanelItems(panel);
    closeModal();
  } catch (err) {
    errorEl.textContent = err?.message || "Import failed.";
  } finally {
    importBtn.disabled = false;
    importBtn.textContent = "Import";
  }
}

export function toggleOpenApiImport() {
  const m = getModal();
  if (m.style.display === "none" || !m.style.display) {
    openModal();
  } else {
    closeModal();
  }
}

// Make available globally for the inline onclick in index.html
window.toggleOpenApiImport = toggleOpenApiImport;
