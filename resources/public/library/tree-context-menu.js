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

// DB connections that open the db-config-form-modal
const dbmap = {
  sql_server: "azure_sql_database",
  oracle: "oracle_jdbc",
  mongodb: "mongodb_atlas",
  cassandra: "cassandra",
  postgresql: "postgresql",
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
  };

  const items = configs[parentLabel];
  if (!items) return;

  const menu = document.createElement("div");
  menu.id = "_connPickerMenu";
  menu.style.cssText = `position:fixed;top:${y}px;left:${x}px;z-index:10000;
    background:#fff;border:1px solid #d1d5db;box-shadow:0 6px 18px rgba(0,0,0,0.12);
    min-width:200px;padding:4px 0;font-family:Georgia,serif;font-size:13px;`;

  for (const item of items) {
    const btn = document.createElement("div");
    btn.textContent = item.label;
    btn.style.cssText = "padding:8px 14px;cursor:pointer;";
    btn.addEventListener("mouseenter", () => btn.style.background = "#f3f4f6");
    btn.addEventListener("mouseleave", () => btn.style.background = "");
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

    // DB connections
    if (dbmap[key]) {
      const dbform = document.querySelector("db-config-form-modal");
      if (dbform) dbform.open(`./database_forms/${dbmap[key]}.html`);
      else if (options.dialog && typeof options.showDialog === "function") {
        options.showDialog(element, options.dialog);
      }
      return;
    }

    // API connections
    if (label === "API") {
      document.querySelector("api-connection-component")
        ?.setAttribute("visibility", "open");
      return;
    }

    // Kafka group — show sub-menu picker
    if (label === "Kafka") {
      showConnectorPicker("Kafka", event.clientX, event.clientY);
      return;
    }

    // Files group — show sub-menu picker
    if (label === "Files") {
      showConnectorPicker("Files", event.clientX, event.clientY);
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
