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
    this.fromAnchor = null;
  }

  _connectorDebugEnabled() {
    return Boolean(window.BITOOL_DEBUG_CONNECTORS);
  }

  _connectorDebug(...args) {
    if (this._connectorDebugEnabled()) {
      console.warn("[TreeConnector]", ...args);
    }
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
      } else if (this.followUp.previewLine) {
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
      } else if (this.followUp.previewLine) {
        this.to = this._findRectangleAtPoint(ev.clientX, ev.clientY);
        this._connectorDebug("pointerup", {
          fromId: this.from?.getAttribute?.("id"),
          toId: this.to?.getAttribute?.("id"),
          fromConnId: this.from?.getAttribute?.("conn_id"),
          toConnId: this.to?.getAttribute?.("conn_id"),
          clientX: ev.clientX,
          clientY: ev.clientY,
        });
        if (this.from && this.to && this.to.getAttribute("conn_id") !== this.from.getAttribute("conn_id")) {
          this.followUp.end();
          this.connector.connect(this.from.getAttribute("conn_id"), this.to.getAttribute("conn_id"), {
            ...this._connectionOptionsForRectangles(this.from, this.to),
          });
          this.saveRectJoins(this.from.getAttribute("id"), this.to.getAttribute("id"));
          this.from = this.to = null;
          this.fromAnchor = null;
          console.log("Line drawn");
        } else if (this.followUp.previewLine) {
          this.followUp.end();
          this.fromAnchor = null;
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
      const dragPayload = event.dataTransfer?.getData('application/json') || "";

      if (event.target.tagName === "RECTANGLE-COMPONENT" && !this.isShiftPressed) {
        try {
          if (!dragPayload) {
            console.warn("Drop ignored: missing application/json payload");
            return;
          }
          this.droppedData = JSON.parse(dragPayload);
          this.activeTarget = event.target;

          // Non-table sources (API, Kafka, File) — use addSingle + connect
          if (this.droppedData.nodetype && this.droppedData.nodetype !== "table") {
            const alias = this.droppedData.nodetype;
            const targetId = this.activeTarget.getAttribute("id");
            const rect = this.activeTarget.getBoundingClientRect();
            const areaRect = this.droppableArea.getBoundingClientRect();
            const x = rect.left - areaRect.left - 200;
            const y = rect.top - areaRect.top;
            this.sendRectangleData({
              alias,
              id: null,
              parent: targetId,
              x, y,
              conn_id: this.droppedData.conn_id,
              panelItems: getPanelItems(),
            });
            this.activeTarget = null;
            this.droppedData = null;
            return;
          }

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
        if (!dragPayload) {
          console.warn("Drop ignored: missing application/json payload");
          return;
        }
        const {label, conn_id, nodetype} = JSON.parse(dragPayload);
        // Use nodetype (e.g. "kafka-source") as alias for backend btype lookup,
        // but display the connection name as the visual label
        const alias = nodetype || label;
        const rectInfo = {
          alias,
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

  _normalizeRectangleTarget(target) {
    if (!target) return null;
    if (target.tagName === "RECTANGLE-COMPONENT") return target;
    return target.closest?.("rectangle-component") || null;
  }

  _connectionOptionsForRectangles(fromRect, toRect) {
    return {
      arrow: false,
      stroke: '#3b7ddd',
      strokeWidth: 1.5,
    };
  }

  _relativeAnchorForPoint(rectEl, clientX, clientY) {
    const rect = rectEl.getBoundingClientRect();
    return {
      rx: rect.width ? (clientX - rect.left) / rect.width : 0.5,
      ry: rect.height ? (clientY - rect.top) / rect.height : 0.5,
    };
  }

  _findRectangleAtPoint(clientX, clientY) {
    const rectangles = Array.from(this.viewport.querySelectorAll("rectangle-component"));
    const candidates = rectangles.map((rect) => {
      const bounds = rect.getBoundingClientRect();
      const dx = clientX < bounds.left ? bounds.left - clientX
        : clientX > bounds.right ? clientX - bounds.right
          : 0;
      const dy = clientY < bounds.top ? bounds.top - clientY
        : clientY > bounds.bottom ? clientY - bounds.bottom
          : 0;
      const distance = Math.hypot(dx, dy);
      const contains =
        clientX >= bounds.left &&
        clientX <= bounds.right &&
        clientY >= bounds.top &&
        clientY <= bounds.bottom;
      return { rect, bounds, distance, contains };
    });

    const directHit = candidates.find(({ contains }) => contains);
    const nearest = [...candidates].sort((a, b) => a.distance - b.distance)[0] || null;
    const snapTolerance = 56;
    const hit = directHit || (nearest && nearest.distance <= snapTolerance ? nearest : null);

    this._connectorDebug("elementFromPoint", {
      clientX,
      clientY,
      rawTag: hit?.rect?.tagName || null,
      rawId: hit?.rect?.getAttribute?.("id") || null,
      hitCount: rectangles.length,
      nearestId: nearest?.rect?.getAttribute?.("id") || null,
      nearestDistance: nearest ? Number(nearest.distance.toFixed(2)) : null,
    });

    return hit?.rect || null;
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
      const isConnectorMode = this.isShiftPressed || ev.shiftKey;
      if (!isConnectorMode) {
        this.dragging = true;
        this.activeRect = el;
        this.startX = ev.clientX;
        this.startY = ev.clientY;
        this.startLeft = parseFloat(el.style.left || 0);
        this.startTop = parseFloat(el.style.top || 0);
        el.style.transition = 'none';
        el.style.zIndex = '-1';
      } else {
        this.from = this._normalizeRectangleTarget(ev.target) || el;
        this.fromAnchor = this._relativeAnchorForPoint(this.from, ev.clientX, ev.clientY);
        this._connectorDebug("pointerdown", {
          fromId: this.from?.getAttribute?.("id"),
          fromConnId: this.from?.getAttribute?.("conn_id"),
          fromAnchor: this.fromAnchor,
          isConnectorMode,
          rawTag: ev.target?.tagName || null,
          rawId: ev.target?.getAttribute?.("id") || null,
          clientX: ev.clientX,
          clientY: ev.clientY,
        });
        this.followUp.start(this.from, ev);
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
        nodetype: draggedItem.getAttribute("data-nodetype") || null,
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
        const treeData = data["tree-data"];
        treeData.conn_id = data["conn-id"];
        treeData.nodetype = treeData.nodetype || "api-connection";
        return mapItems(apiNode, treeItems, treeData);
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

    const BASE_WIDTH = 110;
    const WIDE_WIDTH = 230;
    const HEIGHT = 50;
    const H_SPACE = 50;
    const V_SPACE = 30;

    function nodeWidth(node) {
      return ["Ep", "Wh"].includes(node?.btype) ? WIDE_WIDTH : BASE_WIDTH;
    }

    function treeDepth(node) {
      const children = node.children || [];
      if (children.length === 0) return 1;
      return 1 + Math.max(...children.map(treeDepth));
    }

    function assignPositions(node, x, y) {
      node.x = x;
      node.y = y;

      const children = node.children || [];
      if (children.length === 0) return HEIGHT;

      let totalHeight = 0;
      const childHeights = [];

      for (let i = 0; i < children.length; i++) {
        const xStep = Math.max(nodeWidth(node), nodeWidth(children[i])) + H_SPACE;
        const h = assignPositions(
          children[i],
          x - xStep,
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
    const maxDepth = Math.max(1, ...roots.map(treeDepth));
    const maxStep = WIDE_WIDTH + H_SPACE;
    const startX = Math.max(600, 80 + ((maxDepth - 1) * maxStep)); // ensure left-most nodes remain visible with wider spacing

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
    requestAnimationFrame(() => {
      this.panzoom.fitToContent();
      // Re-compute connector paths after the transform settles
      requestAnimationFrame(() => this.connector._scheduleUpdate());
    });
  }

  createRectangle(item) {
    const rectangle = document.createElement("rectangle-component");
    rectangle.setAttribute("alias", item.alias);
    rectangle.setAttribute("id", item.id);
    rectangle.setAttribute("parent", item.parent);
    rectangle.setAttribute("btype", item.btype);
    const method = String(item.http_method || "").toUpperCase();
    const endpointLabel = (item.btype === "Ep" && method && item.route_path)
      ? `${method} ${item.route_path}`
      : (item.endpoint_label || item.route_path || item.webhook_path || "");
    if (endpointLabel) rectangle.setAttribute("endpoint-label", endpointLabel);
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
          this.connector.connect(
            parentRectangle.getAttribute("conn_id"),
            childRectangle.getAttribute("conn_id"),
            this._connectionOptionsForRectangles(parentRectangle, childRectangle)
          );
        }
      } else if (Array.isArray(item.parent)) {
        const childRectangle = this.viewport.querySelector(`[id="${item.id}"]`);
        item.parent.forEach(id => {
          const parentRectangle = this.viewport.querySelector(`[id="${id}"]`);
          if (childRectangle && parentRectangle) {
            this.connector.connect(
              parentRectangle.getAttribute("conn_id"),
              childRectangle.getAttribute("conn_id"),
              this._connectionOptionsForRectangles(parentRectangle, childRectangle)
            );
          }
        })
      }
    });
  }
}

customElements.define("tree-component", TreeComponent);
