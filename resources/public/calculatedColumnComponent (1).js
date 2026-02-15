import EventHandler from "./library/eventHandler.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .filter-container {
    display: flex;
    flex-direction: column;
    padding: 20px;
    height: 100%;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
    background-color: white;
    overflow:auto;
    scrollbar-width:none;
  }
  .header {
    display: flex;
    justify-content: space-between;
    margin-bottom: 20px;
  }
  .close-button {
    --smart-button-padding: 4px;
    --smart-border-width: 0;
    --smart-font-size: 20px;
    --smart-ui-state-hover: white;
  }
  .filter-group {
    width: 100%;
    margin-bottom: 20px;
  }
  .filter-name {
    margin-bottom: 10px;
  }
  .dropdown {
    width: 100%;
    padding: 13px 5px;
    border-radius: 5px;
    border-color: #e0e0e0;
    background-color: #fefefe;
    margin: 5px 0px;
  }
</style>
<div class="filter-container">
  <div class="header">
    <h3>Calculated Column</h3>
    <smart-button content="&#9747;" class="close-button"></smart-button>
  </div>
  <div style="display:flex;justify-content:flex-end;">
    <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;border-width:0px;"></smart-button>
  </div>
  <div class="filter-group">
    <div class="filter-name">
      <label>Business Name:</label>
      <smart-input id="businessName" class="outlined" style="width:100%"></smart-input>
    </div>
    <div class="filter-name">
      <label>Technical Name:</label>
      <smart-input id="technicalName" class="outlined" style="width:100%"></smart-input>
    </div>
    <div class="filter-name">
      <label>Data Type:</label>
      <select name="dropdown" class="dropdown">
        <option value="01">Integer</option>
        <option value="02">String</option>
        <option value="03">Boolean</option>
      </select>
    </div>
    <div class="filter-name">
      <label>Length:</label>
      <smart-input id="length" class="outlined" style="width:100%"></smart-input>
    </div>
  </div>
</div>`;

class CalculatedColumnComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.selectedRectangle = null;
    this.item = null;

    this.mainContainer = this.shadow.querySelector(".filter-container");
    this.filterName = this.shadow.querySelector("#filterName");
    this.closeButton = this.shadow.querySelector(".close-button");
    this.saveButton = this.shadow.querySelector("#saveButton");
    this.businessName = this.shadow.querySelector("#businessName");
    this.technicalName = this.shadow.querySelector("#technicalName");
    this.length = this.shadow.querySelector("#length");
    this.dropdown = this.shadow.querySelector("select");
    this.setupEventListeners();
  }

  connectedCallback() {
    this.updateVisibility();

    const component = document.createElement("expression-component");
    component.selectedRectangle = this.selectedRectangle;
    this.mainContainer.appendChild(component);

    this.expressionComponent = this.mainContainer.querySelector("expression-component");
  }

  static get observedAttributes() {
    return ["visibility", "item"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      this.updateVisibility();
      this.updateSelectedRectangle();
    } else if (name === "item") {
      this.item = JSON.parse(newValue);
      this.updateFields(this.item);
    }
  }

  updateFields(data) {
    this.businessName.value = data.business_name || "";
    this.technicalName.value = data.technical_name || "";
    this.length.value = String(data.length) || "";
    this.expressionComponent.expressionArea.setTextContent(data.expression || "");
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );
    this.expressionComponent.selectedRectangle = this.selectedRectangle;
  }

  setupEventListeners() {
    EventHandler.on(this.saveButton, "click", this.handleSaveButtonClick.bind(this));
    EventHandler.on(this.closeButton, "click", this.handleCloseButtonClick.bind(this));
  }

  handleSaveButtonClick() {
    this.save();
    this.setAttribute("visibility", "");
  }

  handleCloseButtonClick() {
    this.style.display = "none";
  }

  save() {
    const select = this.mainContainer.querySelector("select");
    const dataObj = {
      id: this.selectedRectangle.id,
      alias: this.businessName.value,
      business_name: this.businessName.value,
      technical_name: this.technicalName.value,
      data_type: select.querySelector("option[value='" + select.value + "']").innerText,
      length: this.length.value,
      expression: this.expressionComponent.expressionArea.getTextContent(),
      column_type: "calculated"
    }

    this.handleAjaxCallAndReRenderItems(dataObj);
    console.log(dataObj);
  }

  handleAjaxCallAndReRenderItems(dataObj) {
    fetch("/saveColumn", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(dataObj)
    }).then((response) => response.json())
      .then((data) => {
        this.dispatchEvent(new CustomEvent("new-column-added", { detail: data }));
      }).catch((error) => {
        alert("Error: " + error);
        console.error("Error:", error);
      });
  }

  updateVisibility() {
    this.style.display =
      this.getAttribute("visibility") == "open" ? "block" : "none";
  }
}

customElements.define("calculated-column-component", CalculatedColumnComponent);
