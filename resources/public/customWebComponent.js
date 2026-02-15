import "./source/modules/smart.dropdownbutton.js";
import "./source/modules/smart.tooltip.js";

import EventHandler from "./library/eventHandler.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />

<smart-card class="basic-card">
  <div class="card-content">
    <div class="inner">
      <div class="child data-type"></div>
      <div class="child business-name"></div>
    </div>
    <div class="inner">
      <div class="child key"></div>

      <div class="child association"></div>

      <div class="child aggregation" style="font-size:10px;"></div>

      <div class="child info" id="aa" style="cursor: pointer"></div>

      <smart-tooltip id="tooltip" arrow position="left">
        <div class="flex margin">
          <h3 class="business-name"></h3>
          <div>
            <div class="margin">
              <div style="font-weight: 600">Business Name:</div>
              <div id="business-name-info"></div>
            </div>

            <div class="margin">
              <div style="font-weight: 600">Technical Name:</div>
              <div id="technical-name-info"></div>
            </div>

            <div class="margin">
              <div style="font-weight: 600">Semantic Type:</div>
              <div id="semantic-type-info"></div>
            </div>

            <div class="margin">
              <div style="font-weight: 600">Data Type:</div>
              <div id="data-type-info"></div>
            </div>

            <div class="margin">
              <div style="font-weight: 600">Key:</div>
              <div id="key-info"></div>
            </div>
          </div>
        </div>
      </smart-tooltip>

      <div class="child dropDownMenu" style="cursor: pointer"></div>

      <div class="child" id="transformed" style="color:green;font-weight:900;"></div>

      <smart-tooltip id="dropDowntooltip" position="left" open-mode="click">
      </smart-tooltip>
    </div>
  </div>
</smart-card>
`;

class CustomWebComponent extends HTMLElement {
  constructor() {
    super();
    this.initializeAttributes();
    this.attachShadow({mode: "open"})
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this.cacheElements();
    this.updateElements();
  }

  initializeAttributes() {
    const attributes = [
      "business_name", "technical_name", "semantic_type", "data_type", "key",
      "info", "aggregation", "parent_object", "column_type", "association", "alias"
    ];
    attributes.forEach(attr => this[attr] = this.getAttribute(attr) || this.getDefaultValue(attr));
  }

  getDefaultValue(attr) {
    const defaults = {
      key: "no", info: "yes", aggregation: "SUM", association: "no"
    };
    return defaults[attr] || "";
  }

  cacheElements() {
    this.content = this.shadowRoot.querySelector(".card-content");
    this.business_nameElements = this.shadowRoot.querySelectorAll(".business-name");
    this.data_typeElement = this.shadowRoot.querySelector(".data-type");
    this.keyElement = this.shadowRoot.querySelector(".key");
    this.aggregationElement = this.shadowRoot.querySelector(".aggregation");
    this.infoElement = this.shadowRoot.querySelector(".info");
    this.tooltip = this.shadowRoot.querySelector("#tooltip");
    this.associationElement = this.shadowRoot.querySelector(".association");
  }

  updateElements() {
    this.updateBusinessName();
    this.updateDataTypeIcon();
    this.updateKeyIcon();
    this.updateAggregation();
    this.updateInfoIcon();
    this.updateTooltip();
  }

  updateBusinessName() {
    this.business_nameElements[0].textContent = this.alias.trim() !== "" ? `${this.business_name} as ${this.alias}` : this.business_name;
    this.business_nameElements[1].textContent = this.business_name;
  }

  updateDataTypeIcon() {
    if (this.data_typeElement) {
      this.data_typeElement.innerHTML = this.getData_typeIcon(this.data_type || this.column_type);
    }
  }

  updateKeyIcon() {
    if (this.keyElement) {
      this.keyElement.innerHTML = this.key === "yes" ? "&#128273;" : "";
    }
  }

  updateAggregation() {
    if (this.aggregationElement && this.column_type === "measure") {
      this.aggregationElement.innerHTML = this.aggregation;
    }
  }

  updateInfoIcon() {
    if (this.infoElement) {
      this.infoElement.innerHTML = this.info === "yes" ? "&#9432;" : "";
    }
  }

  updateTooltip() {
    this.tooltip.querySelector("#business-name-info").textContent = this.business_name;
    this.tooltip.querySelector("#technical-name-info").textContent = this.technical_name;
    this.tooltip.querySelector("#semantic-type-info").textContent = this.semantic_type;
    this.tooltip.querySelector("#data-type-info").textContent = this.data_type;
    this.tooltip.querySelector("#key-info").textContent = this.key;
    this.tooltip.selector = this.infoElement;
  }

  static get observedAttributes() {
    return [
      "business_name", "technical_name", "semantic_type", "data_type", "key",
      "parent_object", "column_type", "aggregation", "info", "padding",
      "association", "excluded", "draggable", "alias", "transform"
    ];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (oldValue === newValue) return;
    this[name] = newValue;
    this.updateAttribute(name, newValue);
  }

  updateAttribute(name, value) {
    switch (name) {
      case "alias":
        this.updateBusinessName();
        break;
      case "business_name":
        this.updateBusinessName();
        this.tooltip.querySelector("#business-name-info").textContent = value;
        break;
      case "technical_name":
        this.tooltip.querySelector("#technical-name-info").textContent = value;
        break;
      case "semantic_type":
        this.tooltip.querySelector("#semantic-type-info").textContent = value;
        break;
      case "data_type":
        this.updateDataTypeIcon();
        this.tooltip.querySelector("#data-type-info").textContent = value;
        break;
      case "key":
        this.updateKeyIcon();
        this.tooltip.querySelector("#key-info").textContent = value;
        break;
      case "info":
        this.updateInfoIcon();
        break;
      case "aggregation":
        this.updateAggregation();
        break;
      case "parent_object":
        this.getMenu();
        break;
      case "padding":
        this.content.style.padding = value;
        break;
      case "association":
        this.associationElement.textContent = value === "yes" ? "T" : "";
        break;
      case "excluded":
        this.updateExcluded(value);
        break;
      case "draggable":
        this.updateDraggable();
        break;
      case "transform":
        this.shadowRoot.querySelector("#transformed").textContent = value === "YES" ? "T" : "";
        break;
    }
  }

  updateExcluded(value) {
    if (value === "YES") {
      this.content.style.backgroundColor = "#f0f0f0";
      this.content.style.color = "#b3b3b3";
    } else if (value === "NO") {
      this.content.style.backgroundColor = "lightgray";
      this.content.style.color = "rgb(4, 60, 128)";
    }
    this.getMenu();
  }

  updateDraggable() {
    if (this.draggable) {
      const span = document.createElement("span");
      span.innerHTML = "<h3 class='child'>=</h3>";
      span.classList.add("inner");
      span.setAttribute("id", "dragBtn");
      this.shadowRoot.querySelector(".card-content").appendChild(span);
    }
  }

  connectedCallback() {
    this.parent_object = this.getAttribute("parent_object") || "";
  }

  getData_typeIcon(data_type) {
    const icons = {
      "integer": `<span><span style="font-size: 0.8em;">2</span><span style="font-size: 1.2em;">2</span></span>`,
      "long": `<span><span style="font-size: 0.8em;">2</span><span style="font-size: 1.2em;">2</span></span>`,
      "float": `<span>1<sup>23</sup></span>`,
      "double": `<span>1<sup>23</sup></span>`,
      "big decimal": `<span>1<sup>23</sup></span>`,
      "string": `<span><span style="font-size: 0.8em;">A</span><span style="font-size: 1.2em;">A</span></span>`,
      "text": `<span><span style="font-size: 0.8em;">A</span><span style="font-size: 1.2em;">A</span></span>`,
      "varchar": `<span><span style="font-size: 0.8em;">A</span><span style="font-size: 1.2em;">A</span></span>`,
      "association": `<span>&#129109;</span>`
    };
    return icons[data_type.toLowerCase()] || `<span>?</span>`;
  }

  getMenuIcon(parent_object) {
    const menuIcons = {
      "projection": this.getProjectionMenuIcon(),
      "union": this.getProjectionMenuIcon(),
      "aggregation": {
        "menuElement": `<div>None</div>`,
        "dropDownElement": `
          <div class="tooltip-item">SUM</div>
          <div class="tooltip-item">COUNT</div>
          <div class="tooltip-item">MIN</div>
          <div class="tooltip-item">MAX</div>
          <div class="tooltip-item">NONE</div>
        `
      },
      "function": this.getProjectionMenuIcon(),
      "calculated_column": {
        "menuElement": `<span class="icon arrow-icon">&#8594;</span>`,
        "dropDownElement": ``
      },
      "output": this.getOutputMenuIcon()
    };
    if (parent_object === "A" && ["integer", "long", "float", "double", "big decimal"].includes(this.data_type.toLowerCase())) {
      return menuIcons["aggregation"];
    } else if (parent_object === "P" || parent_object === "T" || parent_object === "Tg") {
      return menuIcons["projection"];
    } else if (parent_object === "U") {
      return menuIcons["union"];
    } else if (parent_object === "Fu") {
      return menuIcons["function"];
    } else if (parent_object === "O") {
      return menuIcons["output"];
    } else {
      return menuIcons[parent_object] || "";
    }
  }

  getProjectionMenuIcon() {
    if (this.column_type === "calculated") {
      return {
        menuElement: `<span class="icon menu-icon">&#8226;&#8226;&#8226;</span>`,
        dropDownElement: `
          <div class="tooltip-item">${this.getAttribute("excluded") === "YES" ? "Include Column" : "Exclude Column"}</div>
          <div class="tooltip-item">Add Alias</div>
          <div class="tooltip-item">Transform</div>
          <div class="tooltip-item">Details</div>
        `
      }
    } else {
      return {
        menuElement: `<span class="icon menu-icon">&#8226;&#8226;&#8226;</span>`,
        dropDownElement: `
          <div class="tooltip-item">${this.getAttribute("excluded") === "YES" ? "Include Column" : "Exclude Column"}</div>
          <div class="tooltip-item">Add Alias</div>
          <div class="tooltip-item">Transform</div>
        `
      }
    }
  }

  getOutputMenuIcon() {
    if (this.column_type === "measure") {
      return {
        menuElement: `<span class="icon menu-icon">&#8226;&#8226;&#8226;</span>`,
        dropDownElement: `
          <div class="tooltip-item change-to-attribute">Change to Attribute</div>
          <div class="tooltip-item">
            <div class="custom-dropdown">
              <div class="dropdown-button">Change Aggregation</div>
              <div class="custom-dropdown-content">
                <a class="aggregation-button">SUM</a>
                <a class="aggregation-button">COUNT</a>
                <a class="aggregation-button">MIN</a>
                <a class="aggregation-button">MAX</a>
                <a class="aggregation-button">NONE</a>
              </div>
            </div>
          </div>
        `
      };
    } else if (this.column_type === "attribute") {
      return {
        menuElement: `<span class="icon menu-icon">&#8226;&#8226;&#8226;</span>`,
        dropDownElement: `
          <div class="tooltip-item change-to-measure">Change to Measure</div>
          <div class="tooltip-item setkey-button" style="display:none;">Set as Key</div>
          <div class="tooltip-item add-text-association">Add Text Association</div>
          <div class="tooltip-item">
            <div class="custom-dropdown">
              <div class="dropdown-button">Semantic Type</div>
              <div class="custom-dropdown-content">
                <a class="semantictype-button">Calendar - Day</a>
                <a class="semantictype-button">Fiscal - Year</a>
                <a class="semantictype-button">Business Data - From</a>
                <a class="semantictype-button">Geolocation - Latitude</a>
                <a class="semantictype-button">Language</a>
              </div>
            </div>
          </div>
          <div class="tooltip-item edit-key" style="display:none">
            <div class="custom-dropdown">
              <div class="dropdown-button">Key</div>
              <div class="custom-dropdown-content">
                <a class="editkey-button">Edit Compound Key</a>
                <a class="removekey-button">Remove as Key</a>
              </div>
            </div>
          </div>
        `
      };
    }
    return "";
  }

  getMenu() {
    this.menuElement = this.shadowRoot.querySelector(".dropDownMenu");
    if (this.menuElement) {
      this.dropDowntooltip = this.shadowRoot.querySelector("#dropDowntooltip");
      this.clearPreviousTooltipContent();
      this.tooltipContent = this.createTooltipContent();
      this.dropDowntooltip.appendChild(this.tooltipContent);
      this.addTooltipClickListener();
      this.hideSmartButtonsContainer();
      this.addSemanticTypeDropDown();
      this.dropDowntooltip.selector = this.menuElement;
      this.updateTooltipItems();
    }
  }

  clearPreviousTooltipContent() {
    const previousTooltipContent = this.dropDowntooltip.querySelector(".tooltip-flex");
    if (previousTooltipContent) previousTooltipContent.remove();
  }

  createTooltipContent() {
    const tooltipContent = document.createElement("div");
    tooltipContent.classList.add("tooltip-flex");
    const menuIcon = this.getMenuIcon(this.parent_object);
    if (menuIcon) {
      this.menuElement.innerHTML = menuIcon.menuElement;
      tooltipContent.innerHTML = menuIcon.dropDownElement;
    }
    return tooltipContent;
  }

  addTooltipClickListener() {
    EventHandler.on(this.dropDowntooltip, 'click', (e) => {
      if (["SUM", "COUNT", "MIN", "MAX", "NONE"].includes(e.target.textContent)) {
        this.menuElement.innerHTML = `<div>${e.target.textContent}</div>`;
      }
      this.dropDowntooltip.visible = false;
    });
  }

  hideSmartButtonsContainer() {
    const smartButtonsContainer = this.shadowRoot.querySelector(".smart-buttons-container");
    if (smartButtonsContainer) smartButtonsContainer.style.display = "none";
  }

  addSemanticTypeDropDown() {
    const semantic_typeDropDown = this.shadowRoot.querySelector("#semantic_typeDropDown");
    if (semantic_typeDropDown) {
      const child = document.createElement("div");
      child.classList.add("tooltip-flex");
      child.innerHTML = `
        <div class="flex">
          <div class="tooltip-item">None</div>
          <div class="tooltip-item">Currency Mode</div>
          <div class="tooltip-item">Unit of Measure</div>
          <div class="tooltip-item">Text</div>
          <div class="tooltip-item">Language</div>
          <div class="tooltip-item">Geolocation - Longitude</div>
          <div class="tooltip-item">Geolocation - Latitude</div>
          <div class="tooltip-item">Geolocation - cartoid</div>
          <div class="tooltip-item">Geolocation - Normalized NameF</div>
        </div>`;
      semantic_typeDropDown.appendChild(child);
    }
  }

  updateTooltipItems() {
    const changeToMeasureItem = this.tooltipContent.querySelector(".change-to-measure");
    if (changeToMeasureItem && this.key === "yes") {
      changeToMeasureItem.style.backgroundColor = "lightgray";
    }
    const setKeyButton = this.tooltipContent.querySelector(".setkey-button");
    if (setKeyButton && this.key !== "yes") {
      setKeyButton.style.display = "block";
    }
    const editKeyButton = this.tooltipContent.querySelector(".edit-key");
    if (editKeyButton && this.key === "yes") {
      editKeyButton.style.display = "block";
    }
  }
}

customElements.define("custom-web-component", CustomWebComponent);
