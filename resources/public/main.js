// Initialize global data
window.data = JSON.parse(localStorage.getItem('jqdata')) || {
  rectangles: [],
  associations: [],
  selectedRectangle: "",
  selectedAssociation: "",
  treeItems: [
    {
      label: "RDBMS",
      items: [
        { label: "Oracle" },
        { label: "SQL Server" },
      ],
    },
    {
      label: "NoSQL",
      items: [
        { label: "MongoDB" },
        { label: "Cassandra" },
      ]
    },
    { label: "Kafka" },
    { label: "Hadoop" },
    { label: "API" },
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

document.addEventListener('DOMContentLoaded', function () {
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
function addDragListeners(groups) {
  const groupArray = Array.from(groups);
  for (let i = 0; i < 4; i++) {
    groupArray.pop();
  }
  groupArray.forEach((element) => {
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

window.toggleLogic = function () {
  const lambda = document.querySelector('lambda-function-builder');
  if (lambda.getAttribute("visibility") && lambda.getAttribute("visibility") === "open") {
    lambda.setAttribute("visibility", "close");
  } else {
    lambda.setAttribute("visibility", "open");
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