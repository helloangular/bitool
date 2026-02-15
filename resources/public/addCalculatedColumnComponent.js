// import "./source/modules/smart.listbox.js";
// import "./source/modules/smart.input.js";
// import "./source/modules/smart.button.js";
// import "./source/modules/smart.dropdownlist.js";

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
    overflow: scroll;
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
    <h3>Add Calculated Column</h3>
    <smart-button content="&#9747;" class="close-button"></smart-button>
  </div>
  <div style="display:flex;justify-content:flex-end">
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

class AddCalculatedColumnComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.mainContainer = this.shadow.querySelector(".filter-container");
    this.filterName = this.shadow.querySelector("#filterName");
    this.closeButton = this.shadow.querySelector(".close-button");
    this.saveButton = this.shadow.querySelector("#saveButton");
  }
  
  connectedCallback(){
    this.setupEventListeners();
    this.updateVisibility();

    const component = document.createElement("expression-component");
    this.mainContainer.appendChild(component);
  }

  setupEventListeners() {
    this.saveButton.addEventListener("click", () => {
      this.setAttribute("visibility", "");
      this.columnListComponent.setAttribute("visibility", "open");
      this.saveFilter();
    });

    this.closeButton.addEventListener("click", () => {
      this.style.display = "none";
    });
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      this.updateVisibility();
    }
  }

  updateVisibility() {
    this.style.display =
      this.getAttribute("visibility") == "open" ? "block" : "none";
  }
}

customElements.define("add-calculated-column-component", AddCalculatedColumnComponent);
