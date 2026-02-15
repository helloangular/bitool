import EventHandler from "./library/eventHandler.js";
import FollowUpLine from "./library/follow-up-line.js";
import PanZoom from "./library/pan-zoom.js";
import SvgConnector from "./library/svg-connector.js";
import {getPanelItems, request, setPanelItems} from "./library/utils.js";
import { addTableMetadata, findItemData, getTreeItems, mapItems, updateTreeItems } from "./library/tree-store.js";
import { populateAttributes, renderTree } from "./library/tree-view.js";
import { attachTreeContextMenu } from "./library/tree-context-menu.js";
import "./source/modules/smart.button.js";
import "./source/modules/smart.dropdownlist.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.tree.js";
import "./source/modules/smart.window.js";

class TreeComponent extends HTMLElement {
  constructor() {
    super();
    // console.log("TreeComponent Initialize");
    this.attachShadow({mode: "open"});
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
    this._columnListObserver = null;
  }

  connectedCallback() {
    this.loadTemplate().then(() => console.log("Tree Template Loaded."));
    this.observePanelItems();
  }

  async loadTemplate() {
    try {
      const response = await fetch("TreeComponent.html");
      if (!response.ok) throw new Error(`Failed to load template: ${response.status}`);

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
      observer.observe(columnList, {attributes: true});
      this._columnListObserver = observer;
    }

    this.setUpEventListeners();
  }

  setUpEventListeners() {
    EventHandler.on(document, "keydown", (e) => {
      if (e.key === "Shift") {
        this.isShiftPressed = true;
      }
    }, false, "Tree");

    EventHandler.on(document, "keyup", (e) => {
      if (e.key === "Shift") {
        this.isShiftPressed = false;
      }
    }, false, "Tree");

    EventHandler.on(this.form, "submit", (event) => this.createConnection(event), false, "Tree");
    EventHandler.on(document, "api-connection-saved", (event) => this.handleApiConnectionSaved(event), false, "Tree");

    EventHandler.on(this.droppableArea, 'pointerdown', (ev) => {
      if (!this.dragging && !this.activeRect) {
        this.isMouseDownForPan = true
        this.panzoom.start(ev);
      }
    }, false, "Tree");

    EventHandler.on(this.resizer, 'pointerdown', () => {
      this.isResizing = true;
      document.body.style.cursor = 'col-resize';
    }, false, "Tree")

    EventHandler.on(window, 'pointermove', (ev) => {
      if (this.dragging) {
        const dx = ev.clientX - this.startX;
        const dy = ev.clientY - this.startY;
        this.activeRect.style.left = (this.startLeft + dx) + 'px';
        this.activeRect.style.top = (this.startTop + dy) + 'px';
        if (dx !== 0 || dy !== 0) this.isMoved = true;
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
    }, false, "Tree");

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

            this.activeRect.style.zIndex = '1';
            this.activeTarget.style.zIndex = '0';

            if (getPanelItems().find(item => item.parent == 1) && this.activeTarget.getAttribute("btype") === "O") {
              alert("Only one table is allowed under Output. Please use Join or Union.");
              this.activeTarget.setBorder("solid");
              return;
            }

            this.addTableWithoutPopupIfPossible(ev);
          } catch (error) {
            this.droppedData = null;
            console.error(error);
          }
        }

        // Update position in data.
        for (const item of getPanelItems()) {
          if (item.id === parseInt(this.activeRect.getAttribute("id"))) {
            item.x = parseInt(this.activeRect.style.left, 10);
            item.y = parseInt(this.activeRect.style.top, 10);
            this.saveRectPosition({rect: item, panelItems: getPanelItems()});
            this.drawLineFromParentToChild(getPanelItems());
            setPanelItems(getPanelItems());
            break;
          }
        }

        this.activeRect.style.transition = '';
        this.connector._scheduleUpdate();
        console.log("Position changed.")
      } else if (this.isShiftPressed && this.followUp.previewLine) {
        this.to = this.shadowRoot.elementFromPoint(ev.clientX, ev.clientY);
        if (this.from && this.to && this.to.getAttribute("conn_id") !== this.from.getAttribute("conn_id")) {
          this.followUp.end();
          this.connector.connect(this.from.getAttribute("conn_id"), this.to.getAttribute("conn_id"), {arrow: false, stroke: '#2b6cb0', strokeWidth: 2});
          this.saveRectJoins(this.from.getAttribute("id"), this.to.getAttribute("id"));
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
    }, false, "Tree");

    let dragX = 0, dragY = 0;
    EventHandler.on(this.droppableArea, "dragover", (event) => {
      event.preventDefault();
      dragX = event.offsetX;
      dragY = event.offsetY;
      this.droppableArea.style.borderColor = "#666";
    }, false, "Tree");

    EventHandler.on(this.droppableArea, "dragleave", () => {
      this.droppableArea.style.borderColor = "#ccc";
      this.popup.classList.remove("show");
    }, false, "Tree");

    EventHandler.on(this.droppableArea, "drop", (event) => {
      event.preventDefault();

      if (event.target.tagName === "RECTANGLE-COMPONENT" && !this.isShiftPressed) {
        try {
          this.droppedData = JSON.parse(event.dataTransfer.getData('application/json'));
          this.activeTarget = event.target;

          // Restrict adding table under Output if already a table exists.
          if (getPanelItems().find(item => item.parent == 1) && this.activeTarget.getAttribute("btype") === "O") {
            alert("Only one table is allowed under Output. Please use Join or Union.");
            this.activeTarget.setBorder("solid");
            return;
          }

          console.log({btype: this.activeTarget.getAttribute("btype"), from: this.droppedData.from});

          // Directly add table if possible, otherwise show popup.
          if (["O", "J", "Tg"].includes(this.activeTarget.getAttribute("btype"))) {
            this.addTable('join', this.droppedData.from);
          } else if (this.activeTarget.getAttribute("btype") === "U") {
            this.addTable('union', this.droppedData.from);
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
        const {label, conn_id} = JSON.parse(event.dataTransfer.getData('application/json'));
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
          panelItems: getPanelItems(),
        });
      }

      this.droppableArea.style.borderColor = "#ccc";
    }, false, "Tree");

    EventHandler.on(this.popup, 'click', async (e) => {
      const btn = e.target.closest('button');
      const action = (!btn || !this.activeTarget) ? 'cancel' : (btn.dataset.action || 'cancel');
      await this.addTable(action, this.droppedData?.from);

      this.popup.classList.remove('show');
    }, false, "Tree");
  }

  addTableWithoutPopupIfPossible(ev) {
    if (!this.activeRect || !this.activeTarget) return;
    let popupRequired = true;
    const hideButton = (selector) => {
      const b = this.popup.querySelector(selector);
      if (b) b.style.display = 'none';
    };
    console.log(this.activeRect.getAttribute("btype"), this.activeTarget.getAttribute("btype"));
    if (this.activeRect.getAttribute("btype") === "T" && this.activeTarget.getAttribute("btype") === "J") {
      hideButton('button[data-action="join"]');
      this.addTable('join', this.droppedData?.from);
      popupRequired = false;
    } else if (this.activeRect.getAttribute("btype") === "T" && this.activeTarget.getAttribute("btype") === "U") {
      hideButton('button[data-action="union"]');
      this.addTable('union', this.droppedData?.from);
      popupRequired = false;
    } else if (this.activeRect.getAttribute("btype") === "T" && (this.activeTarget.getAttribute("btype") === "O" || this.activeTarget.getAttribute("btype") === "Tg")) {
      hideButton('button[data-action="join"]');
      hideButton('button[data-action="union"]');
      this.addTable('join', this.droppedData?.from);
      popupRequired = false;
    } else {
      this.popup.querySelectorAll('button').forEach(btn => {
        if (btn.style.display === "none") btn.style.display = "block"
      });
    }

    if (!this.popup.classList.contains('show') && popupRequired) {
      const rect = this.activeTarget.getBoundingClientRect();
      this.popup.style.left = (rect.right + 6) + 'px';
      this.popup.style.top = (rect.top - this.droppableArea.getBoundingClientRect().top) + 'px';
      this.popup.classList.add('show');
    }
  }

  async addTable(action, from) {
    if (action === 'join' || action === 'union' || action === 'replace') {
      try {
        const resp = await request("/addtable", {
          method: "POST",
          body: {
            from,
            table1: this.droppedData.label,
            table2: this.activeTarget.getAttribute("id"),
            action,
            conn_id: this.droppedData.conn_id,
            schema: this.droppedData.schema,
            panelItems: getPanelItems(),
          }
        });

        setPanelItems(resp);
      } catch (error) {
        console.error("Error:", error);
      }
    } else {
      this.activeTarget.setBorder("solid");
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
        this.from = ev.target;
        const fromEl = this.shadowRoot.getElementById(ev.target.getAttribute("id"));
        this.followUp.start(fromEl, ev);
      }
      ev.stopPropagation();
    }, false, "Tree");
  }

  sendRectangleData(data) {
    request("/addSingle", {method: "POST", body: data})
      .then((resp) => {
        setPanelItems(resp);
      })
      .catch((error) => {
        console.error("Error:", error);
      });
  }

  saveRectPosition(data) {
    request("/moveSingle", {method: "POST", body: data})
      .then((resp) => {
        setPanelItems(resp);
      })
      .catch((error) => {
        console.error("Error:", error);
      });
  }

  saveRectJoins(src, dest) {
    request("/saveRectJoin", {method: "POST", body: {src, dest, panelItems: getPanelItems()}})
      .then((resp) => {
        setPanelItems(resp);
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
        from: "tree",
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

  renderItems() {
    if (!this.tree) return;
    const treeItems = getTreeItems();
    if (!Array.isArray(treeItems)) return;
    renderTree(this.tree, treeItems, {
      populateAttributes: (elements, items) =>
        populateAttributes(elements, items, findItemData),
      onContextMenuReady: () => attachTreeContextMenu(this.tree, {
        dialog: this.dialog,
        showDialog: this.showDialog.bind(this),
      }),
      onDragReady: (groups) => this.addDragListeners(groups),
    });
  }

  disconnectedCallback() {
    try {
      EventHandler.removeGroup("Tree");
    } catch (err) {
      console.warn('Error removing EventHandler group Tree:', err);
    }
    if (this._columnListObserver) {
      try {
        this._columnListObserver.disconnect();
      } catch (err) {
        console.warn('Error disconnecting columnList observer:', err);
      }
      this._columnListObserver = null;
    }
  }

  addDragListeners(groups) {
    Array.from(groups)
      .filter((element) => element.level !== 1 && element.getAttribute("data-conn_id"))
      .forEach((element) => {
        element.setAttribute("draggable", "true");
        EventHandler.on(element, "dragstart", this.handleDragStart.bind(this), false, "Tree-items");
      });
  }

  async handleApiConnectionSaved(event) {
    const values = event?.detail || {};
    try {
      const data = await request("/createApiConnection", {
        method: "POST",
        body: values
      });

      updateTreeItems((treeItems) => {
        if (!Array.isArray(treeItems)) return treeItems;
        const apiNode = findItemData("API", treeItems);
        if (!apiNode) return treeItems;
        return mapItems(
          apiNode,
          treeItems,
          addTableMetadata(
            data["tree-data"],
            data["conn-id"],
            data["tree-data"]?.items?.[0]?.label
          )
        );
      });
      this.renderItems();
    } catch (error) {
      console.error("Error creating API connection:", error);
    }
  }

  async createConnection(e) {
    e.preventDefault();
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
      const data = await request("/save/save-conn", {
        method: "POST",
        body: {
          connection_name,
          username,
          password,
          dbname,
          dbtype,
          host,
          port,
          sid,
          service,
        }
      });

      updateTreeItems((treeItems) => {
        if (!Array.isArray(treeItems)) return treeItems;
        return mapItems(
          this.item,
          treeItems,
          addTableMetadata(
            data["tree-data"],
            data["conn-id"],
            data["tree-data"]["items"][0]["label"]
          )
        );
      });

      this.renderItems();

      this.dialog.close();
    } catch (error) {
      console.error("Error creating connection:", error);
    }
  }

  calculateNodePositions(items) {
    function buildTree(items, parentId = 0) {
      return items
        .filter(item => item.parent === parentId || (Array.isArray(item.parent) && item.parent.includes(parentId)))
        .map(item => ({
          ...item,
          children: buildTree(items, item.id),
        }));
    }

    const WIDTH = 110;
    const HEIGHT = 50;
    const H_SPACE = 20;
    const V_SPACE = 30;

    function assignPositions(node, x, y) {
      node.x = x;
      node.y = y;

      const children = node.children || [];
      if (children.length === 0) return HEIGHT;

      let totalHeight = 0;
      const childHeights = [];

      for (let i = 0; i < children.length; i++) {
        const h = assignPositions(
          children[i],
          x - (WIDTH + H_SPACE),
          y + totalHeight
        );
        childHeights.push(h);
        totalHeight += h + V_SPACE;
      }

      totalHeight -= V_SPACE;
      const mid = totalHeight / 2;
      node.y += mid - HEIGHT / 2;

      return Math.max(totalHeight, HEIGHT);
    }

    const roots = buildTree(items);
    let currentY = (this.droppableArea.clientHeight - this.droppableArea.clientHeight / 2) - 22.5; // 22.5 is approx height/2 of a rectangle.
    const startX = 600; // keep all roots aligned vertically on the right

    roots.forEach((root) => {
      const subtreeHeight = assignPositions(root, startX, currentY);

      // add vertical margin dynamically based on subtree height
      currentY += subtreeHeight + V_SPACE * 3;
    });

    return roots;
  }

  observePanelItems() {
    if (!window.data) {
      window.data = {};
    }

    const originalPanelItems = getPanelItems();
    // Define a setter so any assignment to window.data.panelItems updates UI and connector
    Object.defineProperty(window.data, "panelItems", {
      configurable: true,
      enumerable: true,
      get() {
        return originalPanelItems;
      },
      set: (newValue) => {
        try {
          console.log('change in window.data observed.');
          if (this.connector && typeof this.connector.clear === 'function') this.connector.clear();
          originalPanelItems.length = 0;
          if (Array.isArray(newValue)) originalPanelItems.push(...newValue);
          if (this.droppableArea) {
            try {
              this.renderRectangles(originalPanelItems);
            } catch (err) {
              console.error('Error rendering rectangles after panelItems update:', err);
            }

            if (originalPanelItems.length > 0) {
              this.droppableArea.classList.add("has-items");
            } else {
              this.droppableArea.classList.remove("has-items");
            }
          }
        } catch (err) {
          console.error('Error in panelItems setter:', err);
        }
      },
    });
  }

  renderRectangles(data) {
    for (let i = this.viewport.children.length - 1; i > 0; i--) {
      this.viewport.removeChild(this.viewport.children[i]);
    }

    // To make sure no rectangle get created more than once.
    const createdList = Array.from({length: data.length}, () => false);
    // Recursively create and append rectangles for tree-structured data
    function appendRectangles(items) {
      items.forEach((item) => {
        const idx = data.findIndex(r => r.id === item.id);
        if (item.btype !== "TgT" && createdList[idx] === false) {
          const rectangle = this.createRectangle(item);
          const conn_id = this.connector.addNode(rectangle);
          rectangle.setAttribute("conn_id", conn_id);
          createdList[idx] = true;
        }
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
      if (item.parent && !Array.isArray(item.parent)) {
        const childRectangle = this.viewport.querySelector(`[id="${item.id}"]`);
        const parentRectangle = this.viewport.querySelector(`[id="${item.parent}"]`);
        if (childRectangle && parentRectangle) {
          this.connector.connect(parentRectangle.getAttribute("conn_id"), childRectangle.getAttribute("conn_id"), {arrow: false, stroke: '#2b6cb0', strokeWidth: 2});
        }
      } else if (Array.isArray(item.parent)) {
        const childRectangle = this.viewport.querySelector(`[id="${item.id}"]`);
        item.parent.forEach(id => {
          const parentRectangle = this.viewport.querySelector(`[id="${id}"]`);
          if (childRectangle && parentRectangle) {
            this.connector.connect(parentRectangle.getAttribute("conn_id"), childRectangle.getAttribute("conn_id"), {arrow: false, stroke: '#2b6cb0', strokeWidth: 2});
          }
        })
      }
    });
  }
}

customElements.define("tree-component", TreeComponent);
