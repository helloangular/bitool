import "./source/modules/smart.tooltip.js";
import "./source/modules/smart.dropdownbutton.js";
import "./source/modules/smart.dialog.js";
import "./source/modules/smart.menu.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />

<smart-card class="basic-card">
  <div class="card-content">
    <div class="inner">
      <div class="child data-type"></div>
      <div class="child business-name">
        <slot></slot>
      </div>
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

      <smart-tooltip id="dropDowntooltip" position="bottom" open-mode="click">
      </smart-tooltip>
    </div>
  </div>
</smart-card>
`;

class CustomWebComponent extends HTMLElement {
  constructor() {
    super();

    this.business_name = this.getAttribute("business_name") || "";
    this.technical_name = this.getAttribute("technical_name") || "";
    this.semantic_type = this.getAttribute("semantic_type") || "";
    this.data_type = this.getAttribute("data_type") || "";
    this.key = this.getAttribute("key") || "no";
    this.info = this.getAttribute("info") || "yes";
    this.aggregation = this.getAttribute("aggregation") || "SUM";
    this.parent_object = this.getAttribute("parent_object") || "";
    this.column_type = this.getAttribute("column_type") || "";
    this.association = this.getAttribute("association") || "no";

    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.appendChild(template.content.cloneNode(true));

    this.content = this.shadow.querySelector(".card-content");

    this.business_nameElements = this.shadow.querySelectorAll(".business-name");
    if (this.business_nameElements.length) {
      this.business_nameElements.forEach((element) => {
        element.textContent = this.business_name;
      });
    }

    this.data_typeElement = this.shadow.querySelector(".data-type");
    if (this.data_typeElement)
      this.data_typeElement.innerHTML = this.getData_typeIcon(
        this.data_type || this.column_type
      );

    this.keyElement = this.shadow.querySelector(".key");
    alert(this.key.toLowerCase());
    if (this.keyElement && this.key.toLowerCase() == "yes")
      this.keyElement.innerHTML = "&#128273;";

    this.aggregationElement = this.shadow.querySelector(".aggregation");
    if (this.aggregationElement && this.column_type === "measure")
      this.aggregationElement.innerHTML = this.aggregation;

    this.infoElement = this.shadow.querySelector(".info");
    this.infoElement.innerHTML = this.info === "yes" ? "&#9432;" : "";

    this.tooltip = this.shadow.querySelector("#tooltip");
    this.tooltip.querySelector("#business-name-info").textContent =
      this.business_name;
    this.tooltip.querySelector("#technical-name-info").textContent =
      this.technical_name;
    this.tooltip.querySelector("#semantic-type-info").textContent =
      this.semantic_type;
    this.tooltip.querySelector("#data-type-info").textContent = this.data_type;
    this.tooltip.querySelector("#key-info").textContent = this.key;

    this.tooltip.selector = this.infoElement;

    this.associationElement = this.shadow.querySelector(".association");
  }

  static get observedAttributes() {
    return [
      "business_name",
      "technical_name",
      "semantic_type",
      "data_type",
      "key",
      "parent_object",
      "column_type",
      "aggregation",
      "info",
      "padding",
      "association",
      "excluded",
    ];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    switch (name) {
      case "business_name":
        this.businessName = newValue;
        this.business_nameElements.forEach((element) => {
          element.textContent = newValue;
        });
        this.tooltip.querySelector("#business-name-info").textContent =
          newValue;
        break;
      case "technical_name":
        this.technical_name = newValue;
        this.tooltip.querySelector("#technical-name-info").textContent =
          newValue;
        break;
      case "semantic_type":
        this.semantic_type = newValue;
        this.tooltip.querySelector("#semantic-type-info").textContent =
          newValue;
        break;
      case "column_type":
        this.column_type = newValue;
        if (newValue === "association") {
          this.data_typeElement.innerHTML = this.getData_typeIcon(newValue);
          this.infoElement.remove();
          this.tooltip.remove();
        }
        break;
      case "data_type":
        this.data_type = newValue;
        this.tooltip.querySelector("#data-type-info").textContent = newValue;
        this.data_typeElement.innerHTML = this.getData_typeIcon(newValue);
        break;
      case "key":
        this.key = newValue;
        this.tooltip.querySelector("#key-info").textContent = newValue;
        if (newValue == "yes") this.keyElement.innerHTML = "&#128273;";
        else this.keyElement.innerHTML = "";
        break;
      case "info":
        this.info = newValue;
        if (newValue == "no") this.infoElement.innerHTML = "";
        break;
      case "aggregation":
        this.aggregation = newValue;
        if (this.column_type === "measure")
          this.aggregationElement.textContent = newValue;
        break;
      case "parent_object":
        this.parent_object = newValue;
        this.getMenu();
        break;
      case "padding":
        this.content.style.padding = newValue;
        break;
      case "association":
        this.association = newValue;
        this.associationElement.textContent = newValue === "yes" ? "T" : "";
        break;
      case "excluded":
        if (newValue == "true") {
          this.content.style.backgroundColor = "#f0f0f0";
          this.content.style.color = "#b3b3b3";
        }
        this.getMenu();
        break;
    }
  }

  connectedCallback() {
    this.parent_object = this.getAttribute("parent_object") || "";
  }

  getData_typeIcon(data_type) {
    switch (data_type.toLowerCase()) {
      case "integer":
      case "long":
        return `<span><span style="font-size: 0.8em;">2</span><span style="font-size: 1.2em;">2</span></span>`;
      case "float":
      case "double":
      case "big decimal":
        return `<span>1<sup>23</sup></span>`;
      case "string":
        return `<span><span style="font-size: 0.8em;">A</span><span style="font-size: 1.2em;">A</span></span>`;
      case "association":
        return `<span>&#129109;</span>`;
      default:
        return `<span>?</span>`;
    }
  }

  getMenuIcon(parent_object) {
    switch (parent_object) {
      case "Projection":
        const isExcluded = this.getAttribute("excluded") === "true";
        return {
          menuElement: `<span class="icon menu-icon">&#8226;&#8226;&#8226;</span>`,
          dropDownElement: `
          <div class="tooltip-item">${
            isExcluded ? "Include Column" : "Exclude Column"
          }</div>
          <div class="tooltip-item">Change Name</div>
        `,
        };
      case "Aggregation":
        if (
          ["integer", "long", "float", "double", "big decimal"].includes(
            this.data_type.toLowerCase()
          )
        ) {
          return {
            menuElement: `<div>None</div>`,
            dropDownElement: `  <div class="tooltip-item">SUM</div>
          <div class="tooltip-item">COUNT</div>
          <div class="tooltip-item">MIN</div>
          <div class="tooltip-item">MAX</div>
          <div class="tooltip-item">NONE</div>`,
          };
        }
        break;
      case "Calculated_column":
        return {
          menuElement: `<span class="icon arrow-icon">&#8594;</span>`,
          dropDownElement: ``,
        };
      case "Output":
        if (this.column_type === "measure") {
          return {
            menuElement: `<span class="icon menu-icon">&#8226;&#8226;&#8226;</span>`,
            dropDownElement: ` <div class="tooltip-item change-to-attribute">Change to Attribute</div>
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

            
        
       `,
          };
        } else if (this.column_type === "attribute") {
          return {
            menuElement: `<span class="icon menu-icon">&#8226;&#8226;&#8226;</span>`,
            dropDownElement: `<div class="tooltip-item change-to-measure">Change to Measure</div>
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
          `,
          };
        }
        break;
    }
    return "";
  }

  getMenu() {
    this.menuElement = this.shadow.querySelector(".dropDownMenu");
    if (this.menuElement) {
      this.dropDowntooltip = this.shadow.querySelector("#dropDowntooltip");

      const previousTooltipContent =
        this.dropDowntooltip.querySelector(".tooltip-flex");
      if (previousTooltipContent) previousTooltipContent.remove();

      this.tooltipContent = document.createElement("div");
      this.tooltipContent.classList.add("tooltip-flex");

      const menuIcon = this.getMenuIcon(this.parent_object);

      if (menuIcon) {
        this.menuElement.innerHTML = menuIcon.menuElement;
        this.tooltipContent.innerHTML = menuIcon.dropDownElement;
      }

      this.dropDowntooltip.appendChild(this.tooltipContent);

      if (this.shadow.querySelector(".smart-buttons-container"))
        this.shadow.querySelector(".smart-buttons-container").style.display =
          "none";

      const semantic_typeDropDown = this.shadow.querySelector(
        "#semantic_typeDropDown"
      );

      if (semantic_typeDropDown) {
        const child = document.createElement("div");
        child.classList.add("tooltip-flex");
        child.innerHTML = `<div class="flex">
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

      this.dropDowntooltip.selector = this.menuElement;

      const changeToMeasureItem =
        this.tooltipContent.querySelector(".change-to-measure");

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
}

customElements.define("custom-web-component", CustomWebComponent);
