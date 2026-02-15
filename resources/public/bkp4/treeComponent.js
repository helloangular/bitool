import "./source/modules/smart.tree.js";
import "./source/modules/smart.window.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.button.js";
import "./source/modules/smart.dropdownlist.js";

class TreeComponent extends HTMLElement {
  constructor() {
    super();

    this.attachShadow({ mode: "open" });
    this.tree = null;
    this.dialog = null;
  }

  connectedCallback() {
    this.loadTemplate();
    this.observePanelItems();
  }

  async loadTemplate() {
    try {
      const response = await fetch("TreeComponent.html");
      const templateHTML = await response.text();

      const template = document.createElement("template");
      template.innerHTML = templateHTML;

      this.render(template);
    } catch (error) {
      console.error("Error loading template:", error);
    }
  }

  render(template) {
    try {
      const content = template.content.cloneNode(true);

      const form = content.getElementById("profileForm");

      this.dialog = content.querySelector(".dialog");
      this.tree = content.querySelector("smart-tree");
      this.renderItems();

      form.addEventListener("submit", (event) => {
        event.preventDefault();

        this.createConnection(this.item);
      });

      const droppableArea = content.querySelector(".dropable-area");

      const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
          if (
            mutation.type === "attributes" &&
            mutation.attributeName === "visibility"
          ) {
            const columnList = document.querySelector("column-list-component");
            if (columnList.getAttribute("visibility") === "open") {
              droppableArea.classList.add("column-list-open");
            } else {
              droppableArea.classList.remove("column-list-open");
            }
          }
        });
      });

      const columnList = document.querySelector("column-list-component");
      if (columnList) {
        observer.observe(columnList, { attributes: true });
      }

      droppableArea.addEventListener("dragover", (event) => {
        event.preventDefault();
        droppableArea.style.borderColor = "#666";
      });

      droppableArea.addEventListener("dragleave", () => {
        droppableArea.style.borderColor = "#ccc";
      });

      droppableArea.addEventListener("drop", async (event) => {
        event.preventDefault();

        const { label, conn_id, schema } = JSON.parse(
          event.dataTransfer.getData("application/json")
        );

        if (event.target.tagName === "RECTANGLE-COMPONENT") {
          const table2 = event.target.getAttribute("id");
 
          for (let i = 0; i < event.target.attributes.length; i++) {
      const attribute = event.target.attributes[i];
      console.log(`${attribute.name}: ${attribute.value}`);
    }

          try {
            const response = await fetch("/addtable", {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                table1: label,
                table2,
                conn_id,
                schema,
                panelItems: window.data.panelItems,
              }),
            });

            if (!response.ok) {
              throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();

            window.data.panelItems = data;
          } catch (error) {
            console.error("Error:", error);
          }

          return;
        }

        droppableArea.style.borderColor = "#ccc";

        window.data.panelItems = [
          ...window.data.panelItems,
          { alias: label, parent: 1, id: window.data.panelItems.length + 1 },
        ];
      });

      this.shadowRoot.appendChild(content);
    } catch (error) {
      console.error("Error rendering template:", error);
    }
  }

  handleDragStart(event) {
    const draggedItem = event.target;
    event.dataTransfer.setData(
      "application/json",
      JSON.stringify({
        label: draggedItem.label,
        conn_id: draggedItem.getAttribute("data-conn_id"),
        schema: draggedItem.getAttribute("data-schema"),
      })
    );
  }

  showDialog(item, dialog) {
    dialog.open();
    this.item = item;
  }

  findItemData(label, items) {
    for (const item of items) {
      if (item.label === label) {
        return item;
      }
      if (item.items && item.items.length > 0) {
        const found = this.findItemData(label, item.items);
        if (found) return found;
      }
    }
    return null;
  }

  renderItems() {
    if (!this.tree) return;

    this.tree.dataSource = window.data.treeItems;

    setTimeout(() => {
      const items = this.tree.querySelectorAll("smart-tree-item");
      const groups = this.tree.querySelectorAll("smart-tree-items-group");

      [...items, ...groups].forEach((element) => {
        const itemData = this.findItemData(
          element.label,
          window.data.treeItems
        );
        if (itemData) {
          Object.keys(itemData).forEach((key) => {
            if (key !== "label" && key !== "items") {
              element.setAttribute(`data-${key}`, itemData[key]);
            }
          });
        }
      });

      items.forEach((element) => {
        if (element.level === 1 || element.level === 2) {
          element.addEventListener("contextmenu", (event) => {
            event.preventDefault();
            event.stopPropagation();

            if (this.dialog) {
              this.showDialog(element, this.dialog);
            }
          });
        }
      });

      let maxLevel = 0;

      groups.forEach((element) => {
        if (element.level > maxLevel) {
          maxLevel = element.level;
        }
      });

      Array.from(groups)
        .filter((element) => element.level !== 1 && element.level === maxLevel)
        .forEach((element) => {
          element.setAttribute("draggable", "true");
          element.addEventListener(
            "dragstart",
            this.handleDragStart.bind(this)
          );
        });
    }, 100);
  }

  mapItems(item, treeItems, data) {
    return treeItems.map((treeItem) => {
      if (treeItem.label === item.label) {
        if (!treeItem.items) treeItem.items = [];

        return {
          ...treeItem,
          items: [...treeItem.items, data],
        };
      } else if (treeItem.items && treeItem.items.length > 0) {
        return {
          ...treeItem,
          items: this.mapItems(item, treeItem.items, data),
        };
      }

      return treeItem;
    });
  }

  async createConnection(item) {
    const formData = new FormData(this.dialog.querySelector("form"));
    const connection_name = formData.get("connection_name");
    const dbtype = formData.get("dbtype");
    const dbname = formData.get("dbname");
    const username = formData.get("username");
    const password = formData.get("password");
    const host = formData.get("host");
    const port = formData.get("port");
    const sid = formData.get("sid");
    const service = formData.get("serviceName");

    try {
      const response = await fetch("/save/save-conn", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          connection_name,
          username,
          password,
          dbname,
          dbtype,
          host,
          port,
          sid,
          service,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();

      window.data.treeItems = this.mapItems(
        item,
        window.data.treeItems,
        this.addTableMetadata(
          data["tree-data"],
          data["conn-id"],
          data["tree-data"]["items"][0]["label"]
        )
      );

      this.renderItems();

      this.dialog.close();
    } catch (error) {
      console.error("Error creating connection:", error);
    }
  }

  addTableMetadata(item, connId, schema) {
    if (item.items && item.items.length > 0) {
      const isTable = item.items.some((subItem) => subItem.items === null);
      if (isTable) {
        item.conn_id = connId;
        item.schema = schema;
      }
      item.items = item.items.map((subItem) =>
        this.addTableMetadata(subItem, connId, schema)
      );
    }
    return item;
  }

  calculateNodePositions(items) {
    const nodeMap = new Map(
      items.map((item) => [item.id, { ...item, children: [] }])
    );

    let root = null;

    nodeMap.forEach((node) => {
      if (node.parent === 0) {
        root = node;
      } else {
        const parent = nodeMap.get(node.parent);
        if (parent) {
          parent.children.push(node);
        }
      }
    });

    const baseX = 550;
    const baseY = 350;
    const xSpacing = 100;
    const ySpacing = 100;

    function assignPositions(node, level, position, totalPositions) {
      if (!node) return;

      node.x = baseX - level * xSpacing;

      const totalHeight = (totalPositions - 1) * ySpacing;
      node.y = baseY - totalHeight / 2 + position * ySpacing;

      if (node.children.length > 0) {
        node.children.forEach((child, index) => {
          assignPositions(child, level + 1, index, node.children.length);
        });
      }
    }

    if (root) {
      assignPositions(root, 0, 0, 1);
    }

    const positionedNodes = [];
    function flattenNodes(node) {
      if (!node) return;
      positionedNodes.push({
        id: node.id,
        alias: node.alias,
        parent: node.parent,
        x: node.x,
        y: node.y,
      });
      node.children.forEach((child) => flattenNodes(child));
    }
    flattenNodes(root);

    return positionedNodes;
  }

  observePanelItems() {
    if (!window.data) {
      window.data = {};
    }

    const originalPanelItems = window.data.panelItems || [];
    Object.defineProperty(window.data, "panelItems", {
      get() {
        return originalPanelItems;
      },
      set: (newValue) => {
        console.log("here");
        originalPanelItems.length = 0;
        originalPanelItems.push(...newValue);
        if (this.shadowRoot) {
          const droppableArea = this.shadowRoot.querySelector(".dropable-area");
          if (droppableArea) {
            while (droppableArea.firstChild) {
              droppableArea.removeChild(droppableArea.firstChild);
            }

            const positionedItems =
              this.calculateNodePositions(originalPanelItems);

            positionedItems.forEach((item) => {
              const rectangle = document.createElement("rectangle-component");
              rectangle.setAttribute("alias", item.alias);
              rectangle.setAttribute("id",item.id);
              rectangle.style.position = "absolute";
              rectangle.style.left = `${item.x}px`;
              rectangle.style.top = `${item.y}px`;
              droppableArea.appendChild(rectangle);
            });

            if (originalPanelItems.length > 0) {
              droppableArea.classList.add("has-items");
            } else {
              droppableArea.classList.remove("has-items");
            }
          }
        }
      },
    });
  }
}
customElements.define("tree-component", TreeComponent);
