import EventHandler from "./eventHandler.js";

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
    const dbmap = {
      sql_server: "azure_sql_database",
      mongodb: "mongodb_atlas"
    };

    if (dbmap[key]) {
      const dbform = document.querySelector("db-config-form-modal");
      dbform.open(`./database_forms/${dbmap[key]}.html`);
      return;
    }

    if (label === "API") {
      document.querySelector("api-connection-component")
        .setAttribute("visibility", "open");
      return;
    }

    if (options.dialog && typeof options.showDialog === "function") {
      options.showDialog(element, options.dialog);
    }
  }, false, "Tree-items");
}
