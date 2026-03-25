import EventHandler from "./library/eventHandler.js";
import {closeOpenedPanel, getShortBtype, getBtypeIcon, setPanelItems, request} from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `
<style>
  .rectangle-container {
    width: 90px;
    height: 20px;
    padding: 10px;
    border: 1px solid #e2e4ea;
    border-radius: 8px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    text-align: center;
    overflow: hidden;
    word-wrap: break-word;
    white-space: normal;
    background: #ffffff;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    font-family: 'DM Sans', -apple-system, sans-serif;
    font-size: 11px;
    font-weight: 500;
    color: #1a1d26;
    transition: 0.15s ease;
  }
  .rectangle-container:hover {
    border-color: #3b7ddd;
    box-shadow: 0 2px 8px rgba(59,125,221,0.15);
  }
  .icon {
    width: 20px;
    height: 20px;
  }
  .rectangle-container.endpoint-node {
    width: 230px;
    padding: 8px 10px;
    border-left: 3px solid #3b7ddd;
  }
  #alias-info.endpoint-alias {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    font-size: 12px;
  }
  .endpoint-path {
    flex: 1 1 auto;
    min-width: 0;
    text-align: left;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    color: #1a1d26;
  }
  .endpoint-icon {
    flex: 0 0 auto;
    width: 20px;
    text-align: center;
    font-size: 14px;
    line-height: 1;
  }
</style>

<div class="rectangle-container">
  <div id="alias-info"></div>
</div>
`;

function ensureWindowDataShapes() {
  if (!window.data) window.data = {};
  if (!Array.isArray(window.data.rectangles)) window.data.rectangles = [];
}

function addObjectIfNotExists(obj) {
  ensureWindowDataShapes();
  const index = window.data.rectangles.findIndex(
    (rect) => String(rect.id) === String(obj.id)
  );
  if (index !== -1) {
    window.data.rectangles[index] = obj; // Replace the value
  } else {
    window.data.rectangles.push(obj); // Add the value if it doesn't exist
  }
}

class RectangleComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({mode: "open"});
    this.shadow.append(template.content.cloneNode(true));

    this.alias = null;
    this.btype = null;
    this.endpointLabel = "";

    this.isDragged = false;

    EventHandler.on(this, 'contextmenu', (e) => {
      e.preventDefault();
      this.openMenu(this.getAttribute("id"), e.clientX, e.clientY);
    }, false, 'Rectangle');

    EventHandler.on(this, "click", (e) => this.handleClick(e), false, 'Rectangle');

    this.container = this.shadow.querySelector(".rectangle-container");

    // Now bind listeners that rely on container
    EventHandler.on(this.container, "dragover", (event) => {
      event.preventDefault();
      this.setBorder("dashed");
    }, false, 'Rectangle');

    EventHandler.on(this.container, "dragleave", (event) => {
      event.preventDefault();
      this.setBorder("solid");
    }, false, 'Rectangle');
  }

  setBorder(type) {
    const border = `1.5px ${type} #3b7ddd`;
    if (this.container) {
      this.container.style.border = border;
    } else {
      const el = this.shadow.querySelector(".rectangle-container");
      if (el) el.style.border = border;
    }
  }

  static get observedAttributes() {
    return ["alias", "style", "btype", "endpoint-label"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "alias") {
      this.alias = newValue;
    } else if (name === "btype") {
      this.btype = newValue;
    } else if (name === "endpoint-label") {
      this.endpointLabel = newValue || "";
    }

    if (this.alias && this.btype) {
      const aliasEl = this.shadow.querySelector('#alias-info');
      const icon = getBtypeIcon(this.btype);
      const isEndpointLike = this.btype === getShortBtype("endpoint") || this.btype === getShortBtype("webhook");
      if (icon && !isEndpointLike) {
        aliasEl.classList.add('icon');
      } else {
        aliasEl.classList.remove('icon');
      }
      this.updateAlias(this.btype, this.alias);
    }
  }

  async handleClick(e) {
    e.preventDefault();
    if (this.isDragged) {
      e.stopPropagation();
      this.isDragged = false;
      return;
    };
    window.data.selectedRectangle = this.getAttribute("id") || "";

    let existingRectangle;
    try {
      this.style.cursor = "wait";
      const id = this.getAttribute("id");
      if (!id || id === "null" || id === "undefined") {
        console.warn("Rectangle has no server-assigned ID — skipping getItem");
        return;
      }
      const resp = await request(`/getItem?id=${id}`, {method: 'GET'});
      existingRectangle = resp;
      ensureWindowDataShapes();
      addObjectIfNotExists(existingRectangle);
    } catch (error) {
      console.error("Error fetching rectangle details:", error);
    } finally {
      this.style.cursor = "pointer";
    }

    const currentRect = (window.data.rectangles || []).find(
      (rect) => String(rect.id) === this.getAttribute("id")
    );
    if (!currentRect) return; // nothing to show

    window.data.selectedRectangle = currentRect.id;
    this.setComponentVisibility(currentRect.btype);
  }

  setComponentVisibility(btype) {
    closeOpenedPanel();
    switch (btype) {
      case getShortBtype('projection'):
        document.querySelector("function-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('function'):
        document.querySelector("lambda-function-builder")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('filter'):
        document.querySelector("filter-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('join'):
        document.querySelector("join-editor-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('aggregation'):
        document.querySelector("aggregation-editor-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('sorter'):
        document.querySelector("sorter-editor")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('union'):
        document.querySelector("union-editor-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('mapping'):
        document.querySelector("mapping-editor")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('api-connection'):
        document.querySelector("api-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('kafka-source'):
        document.querySelector("kafka-source-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('file-source'):
        document.querySelector("file-source-component")?.setAttribute("visibility", "open");
        break;
      case getShortBtype('conditionals'):
        document.querySelector('control-flow-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('target'):
        document.querySelector('target-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('endpoint'):
        document.querySelector('endpoint-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('response-builder'):
        document.querySelector('response-builder-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('validator'):
        document.querySelector('validator-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('auth'):
        document.querySelector('auth-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('db-execute'):
        document.querySelector('db-execute-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('rate-limiter'):
        document.querySelector('rate-limiter-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('cors'):
        document.querySelector('cors-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('logger'):
        document.querySelector('logger-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('cache'):
        document.querySelector('cache-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('event-emitter'):
        document.querySelector('event-emitter-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('circuit-breaker'):
        document.querySelector('circuit-breaker-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('scheduler'):
        document.querySelector('scheduler-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('webhook'):
        document.querySelector('webhook-component')?.setAttribute("visibility", "open");
        break;
      default:
        document.querySelector("column-list-component")?.setAttribute("visibility", "open");
    }
  }

  openMenu(id, x, y) {
    let menu = document.querySelector('floater-menu');
    if (!menu) {
      menu = document.createElement('floater-menu');
      document.body.appendChild(menu);
    }

    const techNodes = ['rate-limiter', 'cors', 'logger', 'cache', 'circuit-breaker'];
    // Source-like btypes that can have tech middleware children
    const techParentBtypes = ["Ep", "Wh", "T", "A", "Dx"];

    if (this.btype === "O") {
      this.hideMenuOptions(["target"], false);
    } else if (this.hasParent() || ["O", "Tg", "Mp"].includes(this.btype)) {
      this.hideMenuOptions(["output", "run", "schedule", "delete"], true);
    } else {
      Array.from(menu.menu.children).forEach((c) => {
        c.style.display = "flex";
      });
    }

    // Hide tech middleware items unless node is a source type (applies to all branches)
    if (!techParentBtypes.includes(this.btype)) {
      techNodes.forEach(label => {
        Array.from(menu.menu.children).forEach((c) => {
          if ((c.innerText || "").trim().toLowerCase() === label) c.style.display = "none";
        });
      });
    }

    if (this.btype === "Tg") this.hideMenuOptions(["run", "schedule"], false);

    menu.showMenu(id, x, y);
  }

  hasParent() {
    const parentAttr = this.getAttribute("parent");
    if (parentAttr === null || parentAttr === undefined) return false;
    const id = Number(parentAttr);
    return !Number.isNaN(id) && id > 0;
  }

  /**
   * 
   * @param {Array} list: ["output", "target", ...]
   * @param {Boolean} hide: if true hide listed options else hide all options except listed ones.
   * @returns {void} 
   */
  hideMenuOptions(list, hide) {
    let menu = document.querySelector('floater-menu');
    if (!menu || !menu.menu) return;
    Array.from(menu.menu.children).forEach((c) => {
      c.style.display = "flex";
    });
    if (hide) {
      Array.from(menu.menu.children).forEach((c) => {
        if (list.includes((c.innerText || "").trim().toLowerCase())) c.style.display = "none";
      });
    } else {
      Array.from(menu.menu.children).forEach((c) => {
        if (!list.includes((c.innerText || "").trim().toLowerCase())) c.style.display = "none";
      });
    }
  }

  updateAlias(btype, alias) {
    const icon = getBtypeIcon(btype);
    const aliasEl = this.shadow.querySelector("#alias-info");
    const isEndpointLike = btype === getShortBtype("endpoint") || btype === getShortBtype("webhook");
    const routeLabel = (this.endpointLabel || "").trim();
    const displayLabel = routeLabel || alias;

    // Apply btype-specific left accent color
    const btypeColors = {
      // Source nodes - blue
      T: "#3b7ddd", V: "#3b7ddd", Ap: "#3b7ddd", Kf: "#3b7ddd", Fs: "#3b7ddd",
      // Transform nodes - purple
      Fi: "#7c5cfc", P: "#7c5cfc", Fu: "#7c5cfc", A: "#7c5cfc", S: "#7c5cfc",
      J: "#7c5cfc", U: "#7c5cfc", Mp: "#7c5cfc", C: "#7c5cfc",
      // API/Endpoint nodes - cyan
      Ep: "#0ea5c7", Wh: "#0ea5c7", Rb: "#0ea5c7",
      // Infra nodes - amber
      Au: "#d97706", Vd: "#d97706", Rl: "#d97706", Cr: "#d97706",
      Lg: "#d97706", Cq: "#d97706", Ev: "#d97706", Ci: "#d97706",
      Dx: "#d97706", Sc: "#d97706",
      // Target - green
      Tg: "#0fa968",
      // Output - dark
      O: "#1a1d26",
    };
    const accentColor = btypeColors[btype] || "#3b7ddd";
    this.container.style.borderLeft = `3px solid ${accentColor}`;

    aliasEl.classList.remove("endpoint-alias");
    this.container.classList.remove("endpoint-node");

    if (isEndpointLike && displayLabel) {
      aliasEl.classList.add("endpoint-alias");
      this.container.classList.add("endpoint-node");
      aliasEl.textContent = "";

      const pathEl = document.createElement("span");
      pathEl.className = "endpoint-path";
      pathEl.textContent = displayLabel;

      const iconEl = document.createElement("span");
      iconEl.className = "endpoint-icon";
      iconEl.innerHTML = icon || "";

      aliasEl.append(pathEl, iconEl);
      this.setAttribute("title", displayLabel);
      return;
    }

    this.setAttribute("title", alias || "");
    if (icon) {
      aliasEl.innerHTML = icon;
    } else {
      aliasEl.textContent = alias;
    }
  }
}

class FloaterMenu extends HTMLElement {
  constructor() {
    super();

    // Attach a shadow DOM
    this.attachShadow({mode: 'open'});

    // Style the menu
    const style = document.createElement('style');
    style.textContent = `
      .menu {
        position: absolute;
        background-color: #ffffff;
        border: 1px solid #e2e4ea;
        border-radius: 10px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.12);
        padding: 6px;
        display: none;
        flex-direction: column;
        font-family: 'DM Sans', -apple-system, sans-serif;
        z-index: 1000;
      }
      .menu-item {
        padding: 7px 12px;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 6px;
        font-size: 12.5px;
        font-weight: 500;
        color: #1a1d26;
        transition: 0.15s;
      }
      .menu-item:hover {
        background-color: #f0f1f4;
        color: #3b7ddd;
      }
    `;

    // Create the menu container
    const menu = document.createElement('div');
    menu.className = 'menu';

    // Add menu items — includes add-child actions, implicit tech nodes, and delete
    ['filter', 'join', 'function', 'projection', 'aggregation', 'union', 'sorter', 'mapping',
      'target', 'conditionals', 'api-connection', 'kafka-source', 'file-source',
      'output', 'run', 'schedule',
      'rate-limiter', 'cors', 'logger', 'cache', 'circuit-breaker',
      'delete'].forEach((label) => {
        const item = this.createMenuItemAndAddListener(label);
        if (label === 'delete') {
          item.style.borderTop = '1px solid #eceef2';
          item.style.marginTop = '2px';
          item.style.paddingTop = '7px';
          item.style.color = '#e5484d';
        }
        menu.appendChild(item);
      });

    // Append styles and menu to the shadow root
    this.shadowRoot.append(style, menu);
    this.menu = menu;

    EventHandler.on(document, 'click', (event) => {
      // Hide the menu if the click is outside of it
      const path = event.composedPath ? event.composedPath() : (event.path || []);
      // If the composed path contains this floater menu, do not hide
      if (path && path.indexOf && path.indexOf(this) !== -1) return;
      this.hideMenu();
    }, {}, 'Rectangle');
  }

  createMenuItemAndAddListener(label) {
    const item = document.createElement('div');
    item.className = 'menu-item';
    item.textContent = label;

    // Event listener for clicks on menu items
    EventHandler.on(item, 'click', (e) => this.handleItemClick(this.getAttribute("id"), label), false, 'FloaterMenu');
    return item
  }

  showMenu(id, x, y) {
    this.menu.style.display = 'flex';
    this.menu.style.left = `${x}px`;
    this.menu.style.top = `${y}px`;
    this.setAttribute("id", id);
  }

  hideMenu() {
    if (this.menu.style.display === 'flex') this.menu.style.display = 'none';
  }

  async handleItemClick(id, label) {
    try {
      if (label === "run") {
        request("/run", {method: 'POST', body: {id: this.getAttribute("id")}});
        this.hideMenu();
        return;
      } else if (label === "schedule") {
        window.toggleScheduler();
        this.hideMenu();
        return;
      } else if (label === "delete") {
        const data = await request("/removeNode", {method: 'POST', body: {id}});
        const panel = data?.cp || data?.sp || data;
        setPanelItems(panel);
        closeOpenedPanel();
        this.hideMenu();
        return;
      }

      const data = await request("/addFilter", {method: 'POST', body: {label, item: id}});
      // prefer cp, then sp, then the return itself.
      const panel = data?.cp || data?.sp || data;
      setPanelItems(panel);

      if (data?.cp && data.cp.length === 0) {
        throw new Error("No data found in CP");
      }

      if (data?.rp) {
        window.data.selectedRectangle = data.rp.id;
        addObjectIfNotExists(data.rp);
      }

      closeOpenedPanel();

      // open panel for clicked label, guard for absent node
      const map = {
        aggregation:       'aggregation-editor-component',
        function:          'function-component',
        projection:        'function-component',
        join:              'join-editor-component',
        union:             'union-editor-component',
        sorter:            'sorter-editor',
        mapping:           'mapping-editor',
        conditionals:      'control-flow-component',
        'api-connection':  'api-component',
        'kafka-source':    'kafka-source-component',
        'file-source':     'file-source-component',
        'rate-limiter':    'rate-limiter-component',
        cors:              'cors-component',
        logger:            'logger-component',
        cache:             'cache-component',
        'circuit-breaker': 'circuit-breaker-component',
        target:            'target-component',
      };
      const elementSelector = map[label] || 'filter-component';
      const el = document.querySelector(elementSelector);
      if (el) el.setAttribute('visibility', 'open');
    } catch (error) {
      console.error('Error on menu action:', error);
    }

    this.hideMenu();
  }
}

customElements.define("rectangle-component", RectangleComponent);
customElements.define('floater-menu', FloaterMenu);
