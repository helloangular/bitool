import EventHandler from "./library/eventHandler.js";
import {closeOpenedPanel, getShortBtype, getBtypeIcon, setPanelItems, request} from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `
<style>
  .rectangle-container {
    width: 90px; /* Fixed width */
    height: 20px; /* Fixed height */
    padding: 10px;
    border: 3px solid #004466;
    border-radius: 6px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    text-align: center; /* Center-align the text */
    overflow: hidden; /* Hide overflowing content */
    word-wrap: break-word; /* Ensure text wraps within the container */
    white-space: normal; /* Allow text to wrap */
  }
  .icon {
    width: 20px;
    height: 20px;
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
    if (this.container) {
      this.container.style.border = `3px ${type} #004466`;
    } else {
      const el = this.shadow.querySelector(".rectangle-container");
      if (el) el.style.border = `3px ${type} #004466`;
    }
  }

  static get observedAttributes() {
    return ["alias", "style", "btype"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "alias") {
      this.alias = newValue;
    } else if (name === "btype") {
      this.btype = newValue;
    }

    if (this.alias && this.btype) {
      const aliasEl = this.shadow.querySelector('#alias-info');
      const icon = getBtypeIcon(this.btype);
      if (icon) {
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
        document.querySelector("function-component")?.setAttribute("visibility", "open");
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
      case getShortBtype('conditionals'):
        document.querySelector('control-flow-component')?.setAttribute("visibility", "open");
        break;
      case getShortBtype('target'):
        document.querySelector('target-component')?.setAttribute("visibility", "open");
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

    if (this.btype === "O") {
      this.hideMenuOptions(["target"], false);
    } else if (this.hasParent() || ["O", "Tg", "Mp"].includes(this.btype)) {
      this.hideMenuOptions(["output", "run", "schedule"], true);
    } else {
      Array.from(menu.menu.children).forEach((c) => {
        c.style.display = "flex";
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
        background-color: white;
        border: 1px solid #ccc;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        padding: 10px;
        display: none;
        flex-direction: column;
        z-index: 1000;
      }
      .menu-item {
        padding: 8px;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .menu-item:hover {
        background-color: #f0f0f0;
      }
    `;

    // Create the menu container
    const menu = document.createElement('div');
    menu.className = 'menu';

    // Add some sample icons (menu items)
    ['filter', 'join', 'function', 'projection', 'aggregation', 'union', 'sorter', 'mapping',
      'target', 'conditionals', 'api-connection', 'output', 'run', 'schedule'].forEach((label) => {
        const item = this.createMenuItemAndAddListener(label);
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
    // Send an AJAX request
    try {
      if (label === "run") {
        request("/run", {method: 'POST', body: {id: this.getAttribute("id")}});
        this.hideMenu();
        return;
      } else if (label === "schedule") {
        window.toggleScheduler();
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
        aggregation: 'aggregation-editor-component',
        function: 'function-component',
        projection: 'function-component',
        join: 'join-editor-component',
        union: 'union-editor-component',
        sorter: 'sorter-editor',
        mapping: 'mapping-editor',
        conditionals: 'control-flow-component',
        'api-connection': 'api-component',
      };
      const elementSelector = map[label] || 'filter-component';
      const el = document.querySelector(elementSelector);
      if (el) el.setAttribute('visibility', 'open');
    } catch (error) {
      console.error('Error on adding filter:', error);
    }

    // Hide the menu after clicking
    this.hideMenu();
  }
}

customElements.define("rectangle-component", RectangleComponent);
customElements.define('floater-menu', FloaterMenu);
