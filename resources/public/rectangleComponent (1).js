import "./source/modules/smart.grid.js";

import EventHandler from "./library/eventHandler.js";
import { closeOpenedPanel, getShortBtype, getBtypeIcon } from "./library/utils.js";

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
    width: 10px;
    height: 15px;
  }
</style>

<div class="rectangle-container">
  <div id="alias-info"></div>
</div>
`;

function addObjectIfNotExists(obj) {
  const index = window.data.rectangles.findIndex(
    (rect) => rect.id === obj.id
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
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.alias = null;
    this.btype = null;

    this.isDragged = false;

    EventHandler.on(this, 'contextmenu', (e) => {
      e.preventDefault();
      this.openMenu(this.getAttribute("id"), e.clientX, e.clientY);
    }, {}, 'Rectangle');

    EventHandler.on(this, "click", (e) => this.handleClick(e), {}, 'Rectangle');

    EventHandler.on(this.shadow, "dragover", (event) => {
      event.preventDefault();
      this.shadow.querySelector(".rectangle-container").style.border = "3px dashed #004466";
    }, {}, 'Rectangle');

    EventHandler.on(this.shadow, "dragleave", (event) => {
      event.preventDefault();
      this.shadow.querySelector(".rectangle-container").style.border = "3px solid #004466";
    }, {}, 'Rectangle');

    this.updateAlias(this.getAttribute("btype") || "");
    this.container = this.shadow.querySelector(".rectangle-container");

    // this.testRectangleComponent();
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
      if(getBtypeIcon(this.btype)) this.container.classList.add("icon");
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

      const response = await fetch(`/getItem?id=${this.getAttribute("id")}`,
        {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
          },
        }
      );

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      existingRectangle = await response.json();
      if (!window.data.rectangles) {
        window.data.rectangles = [];
      }

      addObjectIfNotExists(existingRectangle);
    } catch (error) {
      console.error(error);
    } finally {
      this.style.cursor = "pointer";
    }

    const currentRect = window.data.rectangles?.find(
      (rect) => String(rect.id) === this.getAttribute("id")
    );
    window.data.selectedRectangle = currentRect.id;
    this.setComponentVisibility(currentRect.btype);
  }

  setComponentVisibility(btype) {
    closeOpenedPanel();
    switch (btype) {
      case getShortBtype('projection'):
        document.querySelector("function-component").setAttribute("visibility", "open");
        break;
      case getShortBtype('function'):
        document.querySelector("function-component").setAttribute("visibility", "open");
        break;
      case getShortBtype('filter'):
        document.querySelector("filter-component").setAttribute("visibility", "open");
        break;
      case getShortBtype('join'):
        document.querySelector("join-editor-component").setAttribute("visibility", "open");
        break;
      case getShortBtype('aggregation'):
        document.querySelector("aggregation-editor-component").setAttribute("visibility", "open");
        break;
      case getShortBtype('sorter'):
        document.querySelector("sorter-editor").setAttribute("visibility", "open");
        break;
      case getShortBtype('union'):
        document.querySelector("union-editor-component").setAttribute("visibility", "open");
        break;
      case getShortBtype('mapping'):
        document.querySelector("mapping-editor").setAttribute("visibility", "open");
        break;
      case getShortBtype('api-connection'):
        document.querySelector("api-component").setAttribute("visibility", "open");
        break;
      case getShortBtype('conditionals'):
        document.querySelector('control-flow-component').setAttribute("visibility", "open");
        break;
      default:
        document.querySelector("column-list-component").setAttribute("visibility", "open");
    }
  }

  openMenu(id, x, y) {
    let menu = document.querySelector('floater-menu');
    if (!menu) {
      menu = document.createElement('floater-menu');
      document.body.appendChild(menu);
    }
    menu.showMenu(id, x, y);
  }

  updateAlias(btype, alias) {
    this.shadow.querySelector("#alias-info").innerHTML = getBtypeIcon(btype) || alias;
  }

  testRectangleComponent() {
    console.log("🧪 Running RectangleComponent Tests...");

    // Test Case 01: Alias renders correctly
    const testAlias = "Test Alias";
    this.setAttribute("alias", testAlias);
    const aliasText = this.shadow.querySelector("#alias-info").textContent;
    console.log(aliasText === testAlias
      ? "✅ Test Case 01: Alias rendered correctly"
      : "❌ Test Case 01: Alias not rendered correctly");

    // Test Case 02: Drag over changes border to dashed
    const container = this.shadow.querySelector(".rectangle-container");
    const dragOverEvent = new DragEvent("dragover", { bubbles: true });
    this.shadow.dispatchEvent(dragOverEvent);
    console.log(container.style.border.includes("dashed")
      ? "✅ Test Case 02: Dragover border is dashed"
      : "❌ Test Case 02: Dragover border is not dashed");

    // Test Case 03: Drag leave restores solid border
    const dragLeaveEvent = new DragEvent("dragleave", { bubbles: true });
    this.shadow.dispatchEvent(dragLeaveEvent);
    console.log(container.style.border.includes("solid")
      ? "✅ Test Case 03: Dragleave border restored to solid"
      : "❌ Test Case 03: Dragleave border not restored to solid");

    // Test Case 04: Context menu triggers openMenu
    let openMenuCalled = false;
    this.openMenu = () => {
      openMenuCalled = true;
      console.log("ℹ️ openMenu called");
    };
    this.dispatchEvent(new MouseEvent("contextmenu", { clientX: 100, clientY: 100, bubbles: true }));
    console.log(openMenuCalled
      ? "✅ Test Case 04: Context menu triggered openMenu"
      : "❌ Test Case 04: Context menu did NOT trigger openMenu");

    // Test Case 05: handleClick fetch simulation
    // Mock window.data and fetch
    // if (!window.data) window.data = {};
    // window.data.rectangles = [];
    // window.data.selectedRectangle = "";

    const testId = "999";
    this.setAttribute("id", testId);

    globalThis.fetch = async (url) => ({
      ok: true,
      json: async () => ({ id: testId, btype: "filter" })
    });

    this.setComponentVisibility = (btype, rect) => {
      console.log(`✅ Test Case 05: setComponentVisibility called with type '${btype}' and id '${rect.id}'`);
    };

    this.handleClick().then(() => {
      const selected = window.data.selectedRectangle;
      console.log(selected === testId
        ? "✅ Test Case 06: Rectangle selected ID set correctly"
        : "❌ Test Case 06: Rectangle selected ID not set correctly");
    });
  }
}

class FloaterMenu extends HTMLElement {
  constructor() {
    super();

    // Attach a shadow DOM
    this.attachShadow({ mode: 'open' });

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
    ['filter', 'join', 'function', 'grid', 'aggregation', 'union', 'sorter', 'mapping', 'target', 'conditionals', 'api-connection'].forEach((label) => {
      const item = this.createMenuItemAndAddListener(label);
      menu.appendChild(item);
    });

    // Append styles and menu to the shadow root
    this.shadowRoot.append(style, menu);
    this.menu = menu;

    EventHandler.on(document, 'click', (event) => {
      // Hide the menu if the click is outside of it
      this.hideMenu();
    }, {}, 'Rectangle');
  }

  createMenuItemAndAddListener(label) {
    const item = document.createElement('div');
    item.className = 'menu-item';
    item.textContent = label;

    // Event listener for clicks on menu items
    EventHandler.on(item, 'click', () => this.handleItemClick(this.getAttribute("id"), label), {}, 'Rectangle');
    return item
  }

  showMenu(id, x, y) {
    this.menu.style.display = 'flex';
    this.menu.style.left = `${x}px`;
    this.menu.style.top = `${y}px`;
    this.setAttribute("id", id);
  }

  hideMenu() {
    this.menu.style.display == 'flex' && (this.menu.style.display = 'none');
  }

  handleItemClick(id, label) {
    // Send an AJAX request
    fetch("/addFilter", {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label, item: id }),
    })
    .then((response) => response.json())
    .then((data) => {
      // console.log('Server response:', data);
        window.data.panelItems = data.cp ? data.cp : data.sp ? data.sp : data;
        localStorage.setItem('jqdata', JSON.stringify(window.data));
        
        if (data.cp && data.cp.length == 0) {
          throw new Error("No data found in CP");
        }
        
        if (data.rp) {
          window.data.selectedRectangle = data.rp.id
          addObjectIfNotExists(data.rp);
        }
        
        closeOpenedPanel();
        if (label == "aggregation") {
          document.querySelector("aggregation-editor-component").setAttribute("visibility", "open");
        } else if (label == "function") {
          document.querySelector("function-component").setAttribute("visibility", "open");
        } else if (label == "join") {
          document.querySelector('join-editor-component').setAttribute("visibility", "open");
        } else if (label == "union") {
          document.querySelector("union-editor-component").setAttribute("visibility", "open");
        } else if (label == "sorter") {
          document.querySelector("sorter-editor").setAttribute("visibility", "open");
        } else if (label == "mapping") {
          document.querySelector("mapping-editor").setAttribute("visibility", "open");
        } else if (label == "conditionals") {
          document.querySelector("control-flow-component").setAttribute("visibility", "open");
        } else if (label == "api-connection") {
          document.querySelector("api-component").setAttribute("visibility", "open");
        } else {
          document.querySelector("filter-component").setAttribute("visibility", "open");
        }
      })
      .catch((error) => {
        console.error('Error:', error);
      });

    // Hide the menu after clicking
    this.hideMenu();
  }
}

customElements.define("rectangle-component", RectangleComponent);
customElements.define('floater-menu', FloaterMenu);
