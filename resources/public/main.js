function findTreeParent(items, label) {
  for (const item of items) {
    if (item.label === label) return item;
    if (item.items) {
      const found = findTreeParent(item.items, label);
      if (found) return found;
    }
  }
  return null;
}

// Initialize global data
window.data = {
  gid: null,
  rectangles: [],
  associations: [],
  selectedRectangle: "",
  selectedAssociation: "",
  treeItems: [
    {
      label: "RDBMS",
      items: [
        { label: "PostgreSQL" },
        { label: "Oracle" },
        { label: "SQL Server" },
      ],
    },
    { label: "Snowflake" },
    { label: "Databricks" },
    { label: "GCP" },
    {
      label: "NoSQL",
      items: [
        { label: "MongoDB" },
        { label: "Cassandra" },
      ]
    },
    {
      label: "Kafka",
      items: [
        { label: "Kafka Stream", items: [] },
        { label: "Kafka Consumer", items: [] },
      ]
    },
    {
      label: "Files",
      items: [
        { label: "Local Files" },
        { label: "Remote Files" },
      ]
    },
    {
      label: "Mainframe",
      items: [
        { label: "Mainframe Files" },
      ]
    },
    { label: "Hadoop" },
    { label: "API", items: [] },
  ],
  connections: [],
  panelItems: [],
};

customElements.whenDefined("workflow-dialog").then(() => {
  const wfdialog = document.getElementById("wfDialog");

  wfdialog.setConnections(["MySQL1", "MySQL2", "Postgres"]);
  wfdialog.setValue({ name: "MyFlow", connection: "MySQL1" });

  wfdialog.addEventListener("save", async (e) => {
    try {
      const response = await fetch("/new-workflow", {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(e.detail)
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      console.log(response);
    } catch (error) {
      console.error('Error:', error);
    }
  });

  wfdialog.addEventListener("cancel", () => {
    console.log("CANCEL");
  });

  window.toggleWfDialog = function () {
    (wfdialog.style.display === "none")
      ? wfdialog.open()
      : wfdialog.close();
  };
});

const menu = document.getElementById('menu');
const dialog = document.getElementById('dialog');
const closeDialog = document.getElementById('closeDialog');
const TOOLBAR_OFFSET_SELECTOR = [
  "column-list-component",
  "target-component",
  "association-editor-component",
  "join-editor-component",
  "union-editor-component",
  "aggregation-editor-component",
  "filter-component",
  "function-component",
  "projection-component",
  "calculated-column-component",
  "sorter-editor",
  "transform-editor",
  "mapping-editor",
  "control-flow-component",
  "kafka-source-component",
  "file-source-component",
  "api-component",
  "api-connection-component",
  "endpoint-component",
  "response-builder-component",
  "validator-component",
  "auth-component",
  "db-execute-component",
  "rate-limiter-component",
  "cors-component",
  "logger-component",
  "cache-component",
  "event-emitter-component",
  "circuit-breaker-component",
  "scheduler-component",
  "webhook-component",
].join(", ");

function updateToolbarOffsets() {
  const toolbar = document.querySelector('.panel-btns');
  if (!toolbar) return null;
  const height = Math.ceil(toolbar.getBoundingClientRect().height || toolbar.offsetHeight || 0);
  document.documentElement.style.setProperty('--toolbar-h', height + 'px');
  document.querySelectorAll(TOOLBAR_OFFSET_SELECTOR).forEach((panel) => {
    if (getComputedStyle(panel).position !== "fixed") return;
    panel.style.top = `${height}px`;
    panel.style.height = `calc(100vh - ${height}px)`;
  });
  return toolbar;
}

function initToolbarOffsetTracking() {
  const toolbar = updateToolbarOffsets();
  if (!toolbar || window.__bitoolToolbarOffsetTracking) return;
  window.__bitoolToolbarOffsetTracking = true;
  requestAnimationFrame(updateToolbarOffsets);
  setTimeout(updateToolbarOffsets, 50);
  setTimeout(updateToolbarOffsets, 250);
  new ResizeObserver(updateToolbarOffsets).observe(toolbar);
  window.addEventListener('load', updateToolbarOffsets);
  window.addEventListener('resize', updateToolbarOffsets);
  window.visualViewport?.addEventListener('resize', updateToolbarOffsets);
}

// Open the dialog when the menu item is clicked
if (menu) {
  menu.addEventListener('click', () => {
    dialog.opened = true;
  });
}

// Close the dialog when "Cancel" is clicked
if (closeDialog) {
  closeDialog.addEventListener('click', () => {
    dialog.opened = false;
  });
}

if (document.readyState !== 'loading') {
  initToolbarOffsetTracking();
}

document.addEventListener('DOMContentLoaded', function () {
  initToolbarOffsetTracking();

  // Load saved connections into tree
  fetch("/listConnections")
    .then(r => r.json())
    .then(data => {
      if (!data.connections) return;
      // Group connections by treeParent
      const grouped = {};
      for (const conn of data.connections) {
        if (!conn.treeParent) continue;
        if (!grouped[conn.treeParent]) grouped[conn.treeParent] = [];
        const child = { label: conn.label, conn_id: conn.conn_id };
        if (conn.dbtype) child.dbtype = conn.dbtype;
        if (conn.nodetype) child.nodetype = conn.nodetype;
        grouped[conn.treeParent].push(child);
      }
      // Build new treeItems with deep-copied parent nodes so smart-tree detects changes
      const rebuildItems = (items) => items.map(item => {
        const copy = { ...item };
        if (copy.items) {
          copy.items = rebuildItems(copy.items);
          if (grouped[copy.label]) {
            copy.items = [...copy.items, ...grouped[copy.label]];
            delete grouped[copy.label];
          }
        } else if (grouped[copy.label]) {
          copy.items = [...grouped[copy.label]];
          delete grouped[copy.label];
        }
        return copy;
      });
      window.data.treeItems = rebuildItems(window.data.treeItems);
      const tryRender = () => {
        const tree = document.querySelector("tree-component");
        if (tree && tree.renderItems) tree.renderItems();
        else setTimeout(tryRender, 200);
      };
      tryRender();
    })
    .catch(e => console.warn("Failed to load connections:", e));

  const graphForms = document.querySelectorAll('.graph-form');

  graphForms.forEach(form => {
    form.addEventListener('submit', async function (e) {
      e.preventDefault();

      const formData = new FormData(form);
      const graphName = formData.get('graphname');

      try {
        const response = await fetch("/newgraph", {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            graphname: graphName
          })
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        form.reset();

        window.data.panelItems = [data];
        window.data.gid = data?.id ?? window.data.gid;
        localStorage.setItem('jqdata', JSON.stringify(window.data));

        const dialog = document.getElementById('dialog');
        if (dialog && dialog.contains(form)) {
          dialog.opened = false;
        }
      } catch (error) {
        console.error('Error:', error);
      }
    });
  });
});

window.addRectangle = function (name) {
  const item = {
    alias: name,
    id: null,
    parent: 0,
    x: 10,
    y: 10,
    conn_id: '123',
    panelItems: window.data.panelItems,
  };
  const tree = document.querySelector('tree-component');
  tree.sendRectangleData(item)
}

const panelBtns = document.querySelectorAll('.panel-btns>div>button');
addDragListeners(panelBtns);

// Position tooltips with fixed positioning so they escape overflow containers
document.querySelectorAll('.panel-btns .tooltip').forEach(tip => {
  const tt = tip.querySelector('.tooltiptext');
  if (!tt) return;
  tip.addEventListener('mouseenter', () => {
    const r = tip.getBoundingClientRect();
    tt.style.top = (r.bottom + 4) + 'px';
    tt.style.left = (r.left + r.width / 2 - 60) + 'px';
    tt.style.display = 'block';
  });
  tip.addEventListener('mouseleave', () => { tt.style.display = 'none'; });
});
function addDragListeners(groups) {
  Array.from(groups)
    .filter((element) => element.hasAttribute("data-label") && element.hasAttribute("data-conn_id"))
    .forEach((element) => {
      element.setAttribute("draggable", "true");
      element.addEventListener("dragstart", handleDragStart.bind(this));
    });
}

function handleDragStart(event) {
  const draggedItem = event.target;
  event.dataTransfer.setData(
    "application/json",
    JSON.stringify({
      from: "top",
      label: draggedItem.getAttribute("data-label"),
      conn_id: draggedItem.getAttribute("data-conn_id"),
      schema: draggedItem.getAttribute("data-schema"),
    })
  );
}

window.toggleScheduler = function () {
  const scheduler = document.querySelector('scheduler-wizard');
  if (scheduler.getAttribute("visibility") && scheduler.getAttribute("visibility") === "open") {
    scheduler.setAttribute("visibility", "close");
  } else {
    scheduler.setAttribute("visibility", "open");
  }
}

window.toggleModeling = function () {
  const mc = document.querySelector('modeling-console');
  if (mc.getAttribute("visibility") === "open") {
    mc.setAttribute("visibility", "close");
  } else {
    mc.setAttribute("visibility", "open");
  }
}

window.toggleLogic = function () {
  const lambda = document.querySelector('lambda-function-builder');
  if (lambda.getAttribute("visibility") && lambda.getAttribute("visibility") === "open") {
    lambda.setAttribute("visibility", "close");
  } else {
    lambda.setAttribute("visibility", "open");
  }
}

window.deployEndpoints = async function () {
  try {
    const response = await fetch("/deployEndpoints", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data?.error || `HTTP ${response.status}`);
    }

    if (!data?.count) {
      alert(`No Ep/Wh nodes found in graph ${data?.graph_id}. Add endpoints and deploy again.`);
      return;
    }

    const lines = (data.endpoints || [])
      .map((ep) => `${ep.method} ${ep.url}`)
      .join("\n");
    alert(`Deployed ${data.count} endpoint(s) for graph ${data.graph_id}:\n\n${lines}`);
  } catch (error) {
    console.error("deployEndpoints error:", error);
    alert(`Deploy failed: ${error.message || "Unknown error"}`);
  }
}

window.undo = function () {
  fetch("/undo")
    .then((response) => response.json())
    .then((data) => {
      window.data.panelItems = data;
    })
    .catch((error) => console.error(error));
}

window.redo = function () {
  fetch("/redo")
    .then((response) => response.json())
    .then((data) => {
      window.data.panelItems = data;
    })
    .catch((error) => console.error(error));
}

window.preview = function () {
  const grid = document.querySelector("smart-grid");
  if (grid.style.display && grid.style.display === "block") {
    grid.style.display = "none";
    return
  }
  grid.style.display = "block";
  grid.editing.enabled = true;
  grid.behavior = {
    columnResizeMode: 'growAndShrink'
  }
  grid.filtering = {
    enabled: true
  }
  fetch("/getPreview")
    .then((response) => response.json())
    .then((data) => {
      // const rectangle = window.data.rectangles.find(data => data.id == window.data.selectedRectangle);
      if (data.items && data.items.length > 0) {
        grid.dataSource = new Smart.DataAdapter({
          dataSource: data.items,
          dataFields: Object.entries(data.items[0]).map(([key, value]) => `${key}: ${typeof value}`),
        });
        grid.columns = Object.entries(data.items[0]).map((key) => { return { label: key[0].replace("_", " "), dataField: key[0], width: 130 } });
      }
    })
    .catch((error) => console.error(error))
}

const graphqlBuilderDialog = document.getElementById('graphqlBuilderDialog');

// Function to open/close the GraphQL builder modal
window.toggleGraphQLBuilder = function () {
  graphqlBuilderDialog.opened = !graphqlBuilderDialog.opened;
}

const gitlabPipelineDialog = document.getElementById('gitlabPipelineDialog');

window.toggleGitlabPipelineBuilder = function () {
  gitlabPipelineDialog.opened = !gitlabPipelineDialog.opened;
}

// Listen for the 'save-query' event from the builder component
document.addEventListener('save-query', (e) => {
  console.log('GraphQL Query Saved:', e.detail);
  // You can now use the payload (e.detail) to create a new node or update an existing one.
  // For example, you could add it to an 'api-connection' node's properties.
  graphqlBuilderDialog.opened = false; // Close the dialog on save
});

window.printCurrentRectangle = () => {
  return window.data.rectangles.find(r => r.id === window.data.selectedRectangle);
}

// ── Model List (Open existing model) ──

window.openModelList = async function () {
  const dialog = document.getElementById('modelListDialog');
  const loading = document.getElementById('modelListLoading');
  const table = document.getElementById('modelListTable');
  const tbody = document.getElementById('modelListBody');
  const empty = document.getElementById('modelListEmpty');
  const search = document.getElementById('modelSearchInput');

  // Reset state
  loading.style.display = 'block';
  table.style.display = 'none';
  empty.style.display = 'none';
  search.value = '';
  tbody.innerHTML = '';
  dialog.opened = true;

  try {
    const response = await fetch('/listModels');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const models = await response.json();
    models.sort((a, b) => (b.id || 0) - (a.id || 0));

    loading.style.display = 'none';

    if (!models.length) {
      empty.style.display = 'block';
      return;
    }

    const renderRows = (filter) => {
      const term = (filter || '').toLowerCase();
      tbody.innerHTML = '';
      let count = 0;
      models.forEach(m => {
        if (term && !(m.name || '').toLowerCase().includes(term) && !String(m.id).includes(term)) return;
        count++;
        const tr = document.createElement('tr');
        tr.style.cssText = 'cursor:pointer;';
        tr.innerHTML =
          `<td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f3f4f6;">${m.id}</td>` +
          `<td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f3f4f6;font-weight:500;">${m.name || '(unnamed)'}</td>` +
          `<td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f3f4f6;color:#6b7280;">v${m.version}</td>`;
        tr.addEventListener('mouseenter', () => { tr.style.background = '#f0f5ff'; });
        tr.addEventListener('mouseleave', () => { tr.style.background = ''; });
        tr.addEventListener('click', () => loadModel(m.id));
        tbody.appendChild(tr);
      });
      empty.style.display = count === 0 ? 'block' : 'none';
      table.style.display = count === 0 ? 'none' : 'table';
    };

    renderRows('');
    search.addEventListener('input', () => renderRows(search.value));

  } catch (err) {
    loading.textContent = 'Failed to load models';
    console.error('openModelList error:', err);
  }
};

async function loadModel(gid) {
  try {
    const response = await fetch('/graph', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ gid: gid })
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();

    window.data.panelItems = Array.isArray(data) ? data : [data];
    window.data.gid = gid;
    localStorage.setItem('jqdata', JSON.stringify(window.data));

    document.getElementById('modelListDialog').opened = false;
  } catch (err) {
    console.error('loadModel error:', err);
    alert('Failed to load model: ' + err.message);
  }
}
