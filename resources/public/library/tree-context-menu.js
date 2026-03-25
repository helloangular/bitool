import EventHandler from "./eventHandler.js";

// Maps tree labels to connector-config-modal form types
const connectorMap = {
  "kafka_stream":   { form: "kafka_stream",   parent: "Kafka" },
  "kafka_consumer": { form: "kafka_consumer",  parent: "Kafka" },
  "kafka":          null, // group node — show sub-menu
  "local_files":    { form: "local_files",     parent: "Files" },
  "remote_files":   { form: "remote_files",    parent: "Files" },
  "files":          null,
  "mainframe":      { form: "mainframe_files", parent: "Mainframe" },
  "mainframe_files":{ form: "mainframe_files", parent: "Mainframe" },
};

// DB connections — form type keys that match FORMS in connectorConfigModal.js
const dbmap = {
  postgresql: "postgresql",
  oracle: "oracle",
  sql_server: "sql_server",
  snowflake: "snowflake",
  databricks: "databricks",
  gcp: "bigquery",
};

function showConnectorPicker(parentLabel, x, y) {
  // Remove any existing picker
  const old = document.getElementById("_connPickerMenu");
  if (old) old.remove();

  const configs = {
    "Kafka": [
      { label: "Kafka Stream", form: "kafka_stream" },
      { label: "Kafka Consumer", form: "kafka_consumer" },
    ],
    "Files": [
      { label: "Local / Mounted Files", form: "local_files" },
      { label: "Remote Files (S3/Azure/SFTP)", form: "remote_files" },
    ],
    "RDBMS": [
      { label: "PostgreSQL", form: "postgresql" },
      { label: "Oracle", form: "oracle" },
      { label: "SQL Server", form: "sql_server" },
    ],
  };

  const items = configs[parentLabel];
  if (!items) return;

  const menu = document.createElement("div");
  menu.id = "_connPickerMenu";
  menu.style.cssText = `position:fixed;top:${y}px;left:${x}px;z-index:10000;
    background:#fff;border:1px solid #e2e4ea;box-shadow:0 8px 32px rgba(0,0,0,0.12);
    border-radius:10px;min-width:200px;padding:6px;font-family:'DM Sans',-apple-system,sans-serif;font-size:12.5px;`;

  for (const item of items) {
    const btn = document.createElement("div");
    btn.textContent = item.label;
    btn.style.cssText = "padding:7px 12px;cursor:pointer;border-radius:6px;font-weight:500;color:#1a1d26;transition:0.15s;";
    btn.addEventListener("mouseenter", () => { btn.style.background = "#f0f1f4"; btn.style.color = "#3b7ddd"; });
    btn.addEventListener("mouseleave", () => { btn.style.background = ""; btn.style.color = "#1a1d26"; });
    btn.addEventListener("click", () => {
      menu.remove();
      const modal = document.querySelector("connector-config-modal");
      if (modal) modal.open(item.form, parentLabel);
    });
    menu.appendChild(btn);
  }

  document.body.appendChild(menu);
  // Close on outside click
  const close = (e) => { if (!menu.contains(e.target)) { menu.remove(); document.removeEventListener("click", close); } };
  setTimeout(() => document.addEventListener("click", close), 0);
}

function showConnectionActions(element, label, connId, x, y) {
  const old = document.getElementById("_connPickerMenu");
  if (old) old.remove();

  const menu = document.createElement("div");
  menu.id = "_connPickerMenu";
  menu.style.cssText = `position:fixed;top:${y}px;left:${x}px;z-index:10000;
    background:#fff;border:1px solid #e2e4ea;box-shadow:0 8px 32px rgba(0,0,0,0.12);
    border-radius:10px;min-width:190px;padding:6px;font-family:'DM Sans',-apple-system,sans-serif;font-size:12.5px;`;

  const actions = [
    { label: "Edit Connection", action: () => {
      const dbtype = element.getAttribute("data-dbtype") || "";
      if (dbtype === "api") {
        // API connections use a separate component — pass conn_id for edit
        const apiComp = document.querySelector("api-connection-component");
        if (apiComp) {
          apiComp.setAttribute("data-conn-id", connId);
          apiComp.setAttribute("visibility", "open");
        }
      } else {
        const formType = dbmap[dbtype] || dbtype;
        const modal = document.querySelector("connector-config-modal");
        if (modal?.openForEdit) modal.openForEdit(formType, label, connId);
      }
    }},
    { label: "Test Connection", action: async () => {
      try {
        const res = await fetch("/testConnectionById", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ conn_id: connId }),
        });
        const data = await res.json();
        alert(data.status === "ok" ? "Connection OK!" : "Connection failed: " + (data.message || data.error));
      } catch (e) { alert("Test failed: " + e.message); }
    }},
    { label: "Delete Connection", action: async () => {
      if (!confirm(`Delete connection "${label}"?`)) return;
      try {
        await fetch("/deleteConnection", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ conn_id: connId }),
        });
        element.remove();
      } catch (e) { alert("Delete failed: " + e.message); }
    }},
  ];

  for (const item of actions) {
    const btn = document.createElement("div");
    btn.textContent = item.label;
    btn.style.cssText = "padding:7px 12px;cursor:pointer;border-radius:6px;font-weight:500;color:#1a1d26;transition:0.15s;";
    btn.addEventListener("mouseenter", () => { btn.style.background = "#f0f1f4"; btn.style.color = "#3b7ddd"; });
    btn.addEventListener("mouseleave", () => { btn.style.background = ""; btn.style.color = "#1a1d26"; });
    btn.addEventListener("click", () => { menu.remove(); item.action(); });
    menu.appendChild(btn);
  }

  document.body.appendChild(menu);
  const close = (e) => { if (!menu.contains(e.target)) { menu.remove(); document.removeEventListener("click", close); } };
  setTimeout(() => document.addEventListener("click", close), 0);
}

export function attachTreeContextMenu(treeEl, options = {}) {
  if (!treeEl) return;
  EventHandler.removeGroup("Tree-items");
  EventHandler.on(treeEl, "contextmenu", (event) => {
    const element = event.target.closest("smart-tree-item, smart-tree-items-group");
    if (!element) return;
    event.preventDefault();
    event.stopPropagation();

    const label = element.label || element.getAttribute("label") || "";
    const key = label.toLowerCase().replaceAll(" ", "_");

    // Saved connection items (have conn_id) — MUST check first before group/type matching
    const connId = element.getAttribute("data-conn_id");
    if (connId) {
      showConnectionActions(element, label, connId, event.clientX, event.clientY);
      return;
    }

    // DB connections — open connector-config-modal with the matching form type
    if (dbmap[key]) {
      const modal = document.querySelector("connector-config-modal");
      if (modal) modal.open(dbmap[key], label);
      return;
    }

    // API connections
    if (label === "API") {
      document.querySelector("api-connection-component")
        ?.setAttribute("visibility", "open");
      return;
    }

    // Single-form groups — open connector form directly
    const directForms = { "Snowflake": "snowflake", "Databricks": "databricks", "GCP": "bigquery" };
    if (directForms[label]) {
      const modal = document.querySelector("connector-config-modal");
      if (modal) modal.open(directForms[label], label);
      return;
    }

    // Group nodes — show sub-menu picker
    if (label === "Kafka" || label === "Files" || label === "RDBMS") {
      showConnectorPicker(label, event.clientX, event.clientY);
      return;
    }

    // Mainframe — open directly
    if (key === "mainframe" || key === "mainframe_files") {
      const modal = document.querySelector("connector-config-modal");
      if (modal) modal.open("mainframe_files", "Mainframe");
      return;
    }

    // Specific connector sub-items
    const conn = connectorMap[key];
    if (conn) {
      const modal = document.querySelector("connector-config-modal");
      if (modal) modal.open(conn.form, conn.parent);
      return;
    }

    // Fallback — generic DB dialog
    if (options.dialog && typeof options.showDialog === "function") {
      options.showDialog(element, options.dialog);
    }
  }, false, "Tree-items");
}
