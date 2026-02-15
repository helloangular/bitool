import "./source/modules/smart.tree.js";
import "./source/modules/smart.window.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.button.js";
import "./source/modules/smart.dropdownlist.js";
import EventHandler from "./library/eventHandler.js";
import { test, SvgConnector, FollowUpLine, PanZoom } from "./library/utils.js";


class TreeComponent extends HTMLElement {
  constructor() {
    super();
    // console.log("TreeComponent Initialize");
    this.attachShadow({ mode: "open" });
    this.tree = null;
    this.dialog = null;
    this.isShiftPressed = false;

    this.activeRect = null;
    this.dragging = false;
    this.startX = 0;
    this.startY = 0;
    this.startLeft = 0;
    this.startTop = 0;
    this.from = null;
    this.to = null;
    this.isMoved = false;
    this.isMouseDownForPan = false;

    this.droppedData = null;
    this.activeTarget = null;

    this.isResizing = false;
  }

  connectedCallback() {
    this.loadTemplate();
    this.observePanelItems();
    // this.testTreeComponent();
  }

  async loadTemplate() {
    try {
      const response = await fetch("TreeComponent.html");
      if (!response.ok) {
        throw new Error(`Failed to load template: ${response.status}`);
      }

      const templateHTML = await response.text();

      const template = document.createElement("template");
      template.innerHTML = templateHTML;
      this.render(template);
    } catch (error) {
      console.error("Error loading template:", error);
    }
  }

  render(template) {
    this.shadowRoot.appendChild(template.content.cloneNode(true));

    this.form = this.shadowRoot.getElementById("profileForm");
    this.dialog = this.shadowRoot.querySelector(".dialog");
    this.tree = this.shadowRoot.querySelector("smart-tree");
    this.droppableArea = this.shadowRoot.querySelector("#dropable-area");
    this.viewport = this.shadowRoot.querySelector("#viewport");
    this.popup = this.shadowRoot.querySelector("#optionsPopup");
    this.resizer = this.shadowRoot.querySelector(".resizer");

    if (!this.droppableArea || !this.viewport) {
      console.error("Missing required DOM elements in template");
      return;
    }

    this.panzoom = new PanZoom(this.droppableArea, this.viewport);
    this.connector = new SvgConnector(this.viewport);
    this.followUp = new FollowUpLine(this.connector.svg);
    this.renderItems();

    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        if (
          mutation.type === "attributes" &&
          mutation.attributeName === "visibility"
        ) {
          const columnList = document.querySelector("column-list-component");
          if (columnList.getAttribute("visibility") === "open") {
            this.droppableArea.classList.add("column-list-open");
          } else {
            this.droppableArea.classList.remove("column-list-open");
          }
        }
      });
    });

    const columnList = document.querySelector("column-list-component");
    if (columnList) {
      observer.observe(columnList, { attributes: true });
    }

    this.setUpEventListeners();
  }

  setUpEventListeners() {
    EventHandler.on(document, "keydown", (e) => {
      if (e.key === "Shift") {
        this.isShiftPressed = true;
      }
    }, {}, "Tree");

    EventHandler.on(document, "keyup", (e) => {
      if (e.key === "Shift") {
        this.isShiftPressed = false;
      }
    }, {}, "Tree");

    EventHandler.on(this.form, "submit", (event) => {
      event.preventDefault();
      this.createConnection(this.item);
    }, {}, "Tree");

    EventHandler.on(this.droppableArea, 'pointerdown', (ev) => {
      if (!this.dragging && !this.activeRect) {
        this.isMouseDownForPan = true
        this.panzoom.start(ev);
      }
    }, {}, 'Tree');

    EventHandler.on(this.resizer, 'pointerdown', () => {
      this.isResizing = true;
      document.body.style.cursor = 'col-resize';
    })

    EventHandler.on(window, 'pointermove', (ev) => {
      if (this.dragging) {
        const dx = ev.clientX - this.startX;
        const dy = ev.clientY - this.startY;
        this.activeRect.style.left = (this.startLeft + dx) + 'px';
        this.activeRect.style.top = (this.startTop + dy) + 'px';
        if (dx != 0 || dy != 0) this.isMoved = true;
        this.connector._scheduleUpdate();
        this.panzoom.checkAutoPan(ev.clientX, ev.clientY);
      } else if (this.isShiftPressed && this.followUp.previewLine) {
        this.followUp.preview(ev)
      } else if (this.isMouseDownForPan) {
        this.panzoom.update(ev);
      } else if (this.isResizing) {
        const newWidth = ev.clientX;
        const minWidth = 100;
        const maxWidth = window.innerWidth * 0.8;
        if (newWidth > minWidth && newWidth < maxWidth) {
          this.tree.style.width = newWidth + 'px';
        }
      }
    }, {}, 'Tree');

    EventHandler.on(window, 'pointerup', (ev) => {
      if (this.dragging && this.isMoved) {
        // manually handled because pointer event prevent click event in rectangle component. 
        this.activeRect.isDragged = this.isMoved;

        this.activeTarget = this.shadowRoot.elementFromPoint(ev.clientX, ev.clientY);
        if (this.activeTarget.tagName === "RECTANGLE-COMPONENT" && this.activeTarget !== this.activeRect) {
          try {
            this.droppedData = {
              label: this.activeRect.getAttribute("alias"),
              conn_id: "88",
              schema: "public",
            };

            this.addTableWithoutPopupIfPossible(ev);
          } catch (error) {
            this.droppedData = null;
            console.error(error);
          }
        }

        // Update position in data.
        for (const item of window.data.panelItems) {
          if (item.id === parseInt(this.activeRect.getAttribute("id"))) {
            item.x = parseInt(this.activeRect.style.left, 10);
            item.y = parseInt(this.activeRect.style.top, 10);
            this.saveRectPosition({ rect: item, panelItems: window.data.panelItems });
            this.drawLineFromParentToChild(window.data.panelItems);
            localStorage.setItem("jqdata", JSON.stringify(window.data));
            break;
          }
        }

        this.activeRect.style.transition = '';
        this.connector._scheduleUpdate();
        console.log("Position changed.")
      } else if (this.isShiftPressed && this.followUp.previewLine) {
        this.to = this.shadowRoot.elementFromPoint(ev.clientX, ev.clientY).getAttribute("id");
        if (this.from && this.to && this.to !== this.from) {
          this.followUp.end();
          this.connector.connect(parseInt(this.from), parseInt(this.to), { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });
          this.saveRectJoins(this.from, this.to);
          this.from = this.to = null;
          console.log("Line drawn");
        }
      } else if (this.isResizing) {
        this.isResizing = false;
        document.body.style.cursor = 'default';
      } else {
        this.panzoom.end(ev);
      }
      this.dragging = false;
      this.isMoved = false;
      this.activeRect = null;
      this.isMouseDownForPan = false;
    }, {}, 'Tree');

    let dragX = 0, dragY = 0;
    EventHandler.on(this.droppableArea, "dragover", (event) => {
      event.preventDefault();
      dragX = event.offsetX;
      dragY = event.offsetY;
      this.droppableArea.style.borderColor = "#666";

      // show popup near target 
      // if (!this.popup.classList.contains('show')) {
      //   const rect = event.target.getBoundingClientRect();
      //   this.popup.style.left = (rect.right + 6) + 'px';
      //   this.popup.style.top = (rect.top - this.droppableArea.getBoundingClientRect().top) + 'px';
      //   this.popup.classList.add('show');
      // }
    }, {}, 'Tree');

    EventHandler.on(this.droppableArea, "dragleave", () => {
      this.droppableArea.style.borderColor = "#ccc";
      this.popup.classList.remove("show");
    }, {}, 'Tree');

    EventHandler.on(this.droppableArea, "drop", (event) => {
      event.preventDefault();

      if (event.target.tagName === "RECTANGLE-COMPONENT" && !this.isShiftPressed) {
        try {
          this.droppedData = JSON.parse(event.dataTransfer.getData('application/json'));
          this.activeTarget = event.target;

          if (["O", "J"].includes(this.activeTarget.getAttribute("btype"))) {
            this.addTable('join');
          } else if (this.activeTarget.getAttribute("btype") === "U") {
            this.addTable('union');
          } else {
            if (!this.popup.classList.contains('show')) {
              const rect = this.activeTarget.getBoundingClientRect();
              this.popup.style.left = (rect.right + 6) + 'px';
              this.popup.style.top = (rect.top - this.droppableArea.getBoundingClientRect().top) + 'px';
              this.popup.classList.add('show');
            }
          }
        } catch (error) {
          this.droppedData = null;
          console.error(error);
        }
      } else {
        const { label, conn_id } = JSON.parse(event.dataTransfer.getData('application/json'));
        const rectInfo = {
          alias: label,
          id: null,
          parent: 0,
          x: 0,
          y: 0,
        };
        
        const rectangle = this.createRectangle(rectInfo);
        rectangle.style.left = `${dragX - rectangle.offsetWidth / 2}px`;
        rectangle.style.top = `${dragY - rectangle.offsetHeight / 2}px`;
        
        this.sendRectangleData({
          alias: rectInfo.alias,
          id: null,
          parent: rectInfo.parent,
          x: dragX - rectangle.offsetWidth / 2,
          y: dragY - rectangle.offsetHeight / 2,
          conn_id,
          panelItems: window.data.panelItems,
        });
      }

      this.droppableArea.style.borderColor = "#ccc";
    }, {}, 'Tree');

    this.popup.addEventListener('click', async (e) => {
      const btn = e.target.closest('button');
      if (!btn || !this.activeTarget) {
        btn.dataset.action = "cancel";
      };

      const action = btn.dataset.action;
      this.addTable(action);

      // hide popup after action (except cancel)
      if (action !== 'cancel') {
        console.log(`Action: ${action}, on target ${this.activeTarget.getAttribute("id")}, payload:`, this.droppedData);
      }
      this.popup.classList.remove('show');
    });
  }

  addTableWithoutPopupIfPossible(ev) {
    if (!this.activeRect && !this.activeTarget) return;
    const popupRequired = true;
    if (this.activeRect.getAttribute("btype") == "T" && this.activeTarget.getAttribute("btype") == "J") {
      this.popup.querySelector('button[data-action="join"]').display = "none";
      this.addTable('join');
      popupRequired = false;
    } else if (this.activeRect.getAttribute("btype") == "T" && this.activeTarget.getAttribute("btype") == "U") {
      this.popup.querySelector('button[data-action="union"]').display = "none";
      this.addTable('union');
      popupRequired = false;
    } else if (this.activeRect.getAttribute("btype") == "T" && this.activeTarget.getAttribute("btype") == "O") {
      this.popup.querySelector('button[data-action="join"]').display = "none";
      this.popup.querySelector('button[data-action="union"]').display = "none";
      this.addTable('join')
      popupRequired = false;
    }

    if (!this.popup.classList.contains('show') && popupRequired) {
      const rect = ev.target.getBoundingClientRect();
      this.popup.style.left = (rect.right + 6) + 'px';
      this.popup.style.top = (rect.top - this.droppableArea.getBoundingClientRect().top) + 'px';
      this.popup.classList.add('show');
    }
  }

  async addTable(action) {
    if (action === 'join' || action === 'union' || action === 'replace') {
      try {
        const response = await fetch("/addtable", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            table1: this.droppedData.label,
            table2: this.activeTarget.getAttribute("id"),
            action,
            conn_id: this.droppedData.conn_id,
            schema: this.droppedData.schema,
            panelItems: window.data.panelItems,
          }),
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();


        window.data.panelItems = data;
        localStorage.setItem("jqdata", JSON.stringify(window.data));
      } catch (error) {
        console.error("Error:", error);
      }
    }
    this.activeTarget = null;
    this.droppedData = null;
  }

  makeDraggable(el) {
    EventHandler.on(el, 'pointerdown', (ev) => {
      if (!this.isShiftPressed) {
        this.dragging = true;
        this.activeRect = el;
        this.startX = ev.clientX;
        this.startY = ev.clientY;
        this.startLeft = parseFloat(el.style.left || 0);
        this.startTop = parseFloat(el.style.top || 0);
        el.style.transition = 'none';
        el.style.zIndex = '-1';
      } else {
        this.from = ev.target.getAttribute("id");
        const fromEl = this.shadowRoot.getElementById(this.from);
        this.followUp.start(fromEl, ev);
      }
      ev.stopPropagation();
    }, {}, 'Tree');
  }

  sendRectangleData(data) {
    fetch("/addSingle", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data),
    })
      .then((response) => response.json())
      .then((data) => {
        window.data.panelItems = data;
        localStorage.setItem("jqdata", JSON.stringify(window.data));
      })
      .catch((error) => {
        console.error("Error:", error);
      });
  }

  saveRectPosition(data) {
    fetch("/moveSingle", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data),
    })
      .then((response) => response.json())
      .then((data) => {
        window.data.panelItems = data;
        localStorage.setItem("jqdata", JSON.stringify(window.data));
      })
      .catch((error) => {
        console.error("Error:", error);
      });
  }

  saveRectJoins(src, dest) {
    fetch("/saveRectJoin", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ src, dest, panelItems: window.data.panelItems }),
    })
      .then((response) => response.json())
      .then((data) => {
        window.data.panelItems = data;
        localStorage.setItem("jqdata", JSON.stringify(window.data));
      })
      .catch((error) => {
        console.error("Error:", error);
      });
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

    const waitForTreeItems = () => {
      const items = this.tree.querySelectorAll("smart-tree-item");
      const groups = this.tree.querySelectorAll("smart-tree-items-group");

      if (items.length === 0 || groups.length === 0) {
        requestAnimationFrame(waitForTreeItems); // keep checking
        return;
      }

      this.populateItemAttributes([...items, ...groups]);
      this.addContextMenuListeners(items);
      this.addDragListeners(groups);
    };

    waitForTreeItems();
  }

  populateItemAttributes(iterable) {
    iterable.forEach((element) => {
      const itemData = this.findItemData(element.label, window.data.treeItems);
      if (itemData) {
        Object.keys(itemData).forEach((key) => {
          if (key !== "label" && key !== "items") {
            element.setAttribute(`data-${key}`, itemData[key]);
          }
        });
      }
    });
  }

  addContextMenuListeners(items) {
    items.forEach((element) => {
      if (element.level === 1 || element.level === 2) {
        EventHandler.on(element, "contextmenu", (event) => {
          event.preventDefault();
          event.stopPropagation();

          if (this.dialog) {
            this.showDialog(element, this.dialog);
          }
        }, {}, 'Tree');
      }
    });
  }

  addDragListeners(groups) {
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
        EventHandler.on(element, "dragstart", this.handleDragStart.bind(this), {}, 'Tree');
      });
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
      localStorage.setItem("jqdata", JSON.stringify(window.data));

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
    // Build a tree structure from flat items array
    function buildTree(items, parentId = 0) {
      return items
        .filter((item) => item.parent === parentId)
        .map((item) => ({
          ...item,
          children: buildTree(items, item.id),
        }));
    }

    const WIDTH = 110;
    const SPACE = 20;
    // Recursively assign x, y positions to each node in the tree
    function assignPositions(parent, children) {
      for (let i = 0; i < children.length; i++) {
        if (parent.x < children[i].x + WIDTH + SPACE) {
          children[i].x = parent.x - WIDTH - SPACE;
        }
        if (children[i].children?.length > 0) {
          assignPositions(children[i], children[i].children);
        }
      }
    }

    const tree = buildTree(items);
    assignPositions(tree[0], tree[0].children);
    return tree;
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
        console.log('change in window.data observed.')
        this.connector.clear();
        originalPanelItems.length = 0;
        originalPanelItems.push(...newValue);
        if (this.droppableArea) {
          this.renderRectangles(originalPanelItems);

          if (originalPanelItems.length > 0) {
            this.droppableArea.classList.add("has-items");
          } else {
            this.droppableArea.classList.remove("has-items");
          }
        }
      },
    });
  }

  renderRectangles(data) {
    for (let i = this.viewport.children.length - 1; i > 0; i--) {
      this.viewport.removeChild(this.viewport.children[i]);
    }

    // Recursively create and append rectangles for tree-structured data
    function appendRectangles(items) {
      items.forEach((item) => {
        const rectangle = this.createRectangle(item);
        this.connector.addNode(rectangle, item.id);
        if (item.children && item.children.length > 0) {
          appendRectangles.call(this, item.children);
        }
      });
    }

    const positions = this.calculateNodePositions(data);
    appendRectangles.call(this, positions);
    this.drawLineFromParentToChild(data);
  }

  createRectangle(item) {
    const rectangle = document.createElement("rectangle-component");
    rectangle.setAttribute("alias", item.alias);
    rectangle.setAttribute("id", item.id);
    rectangle.setAttribute("parent", item.parent);
    rectangle.setAttribute("btype", item.btype);
    rectangle.style.position = "absolute";
    rectangle.style.left = `${item.x}px`;
    rectangle.style.top = `${item.y}px`;

    this.viewport.appendChild(rectangle);
    this.makeDraggable(rectangle);
    return rectangle;
  }

  drawLineFromParentToChild(originalPanelItems) {
    originalPanelItems.forEach((item) => {
      if (item.parent) {
        const childRectangle = this.viewport.querySelector(`[id="${item.id}"]`);
        const parentRectangle = this.viewport.querySelector(`[id="${item.parent}"]`);
        if (childRectangle && parentRectangle) {
          this.connector.connect(item.parent, item.id, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });
        }
      }
    });
  }

  testTreeComponent() {
    console.log("🧪 Running TreeComponent tests...");

    // Test Case 01: TreeComponent loads template (mock required)
    test(() => typeof this.loadTemplate, 'function', 'Test Case 01: loadTemplate method exists');

    // Test Case 02: ConnectedCallback initializes drag state
    test(this.isShiftPressed === false, true, 'Test Case 02: Component state initialized correctly');

    // Test Case 03: findItemData returns correct match
    test(() => {
      const sampleItems = [
        { label: 'A', items: [{ label: 'B' }, { label: 'C' }] },
        { label: 'D' }
      ];
      const foundItem = this.findItemData('C', sampleItems);
      return foundItem?.label === 'C'
    }, true, 'Test Case 03: findItemData found correct item');

    // Test Case 04: calculateNodePositions returns tree structure
    test(() => {
      const flatItems = [
        { id: 1, parent: 0, x: 0, y: 0 },
        { id: 2, parent: 1, x: 50, y: 50 },
        { id: 3, parent: 1, x: 100, y: 100 }
      ];
      const tree = this.calculateNodePositions(flatItems);
      return tree.length > 0 && tree[0].children?.length === 2
    }, true, 'Test Case 04: calculateNodePositions returned valid tree');

    // Test Case 05: addTableMetadata adds conn_id and schema
    test(() => {
      const item = {
        label: "schema",
        items: [{ label: "table", items: null }]
      };
      const enriched = this.addTableMetadata(item, "conn-1", "schema-1");
      return enriched.conn_id === "conn-1" && enriched.schema === "schema-1";
    }, true, 'Test Case 05: addTableMetadata added metadata');

    // Test Case 06: mapItems adds child correctly
    test(() => {
      const treeItems = [
        { label: "parent", items: [] }
      ];
      const mapped = this.mapItems({ label: "parent" }, treeItems, { label: "child" });
      return mapped[0].items.length === 1 && mapped[0].items[0].label === "child";
    }, true, 'Test Case 06: mapItems added child correctly.');

    console.log("🏁 TreeComponent tests completed.");
  }
}

customElements.define("tree-component", TreeComponent);
