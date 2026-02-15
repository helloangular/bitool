import "./source/modules/smart.input.js";
import "./source/modules/smart.button.js";
import EventHandler from "./library/eventHandler.js";

const template = document.createElement("template");
template.innerHTML = `
<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
    .filter-container {
        display: flex;
        flex-direction: column;
        padding: 20px;
        height: 100%;
        box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
        background-color: white;
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
</style>
<div class="filter-container">
    <div class="header">
        <h3>Aggregation</h3>
        <smart-button content="&#9747;" class="close-button"></smart-button>
    </div>
    <div style="display:flex;justify-content:flex-end">
      <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;border-width:0px;"></smart-button>
    </div>
    <div class="filter-group">
        <div class="filter-name">
            <label>Name*</label>
            <smart-input name="name" class="outlined" style="width:100%" required></smart-input>
        </div>
    </div>
    <div style="overflow-y:auto;scrollbar-width:none;">
      <div class="filter-group">
          <h3>&#x1F897; Column (<span id="columns-count">0</span>)</h3>
          <smart-input id="searchInput" style="width:100%;height:40px" placeholder="Search"></smart-input>
          <div id="list-container"></div>
      </div>
      <div id="having-container">
          <h3>&#x1F897; Having</h3>
      </div>
    </div>
</div>
`;

class AggregationEditorComponent extends HTMLElement {
  static get observedAttributes() {
    return ["visibility"];
  }
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.shadowRoot.append(template.content.cloneNode(true));

    this.selectedRectangle = null;
    this.dirty = false;

    this.mainContainer = this.shadowRoot.querySelector(".filter-container");
    this.searchInput = this.shadowRoot.querySelector("#searchInput");
    this.havingContainer = this.shadowRoot.querySelector("#having-container");
    this.listContainer = this.shadowRoot.querySelector("#list-container");
    this.closeButton = this.shadowRoot.querySelector(".close-button");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.columnsCount = this.shadowRoot.querySelector("#columns-count");
    this.smartInput = this.mainContainer.querySelector("smart-input");
  }
  connectedCallback() {
    this.updateVisibility();
    // this.updateSelectedRectangle();
    // this.setUpEventListeners();
    this.open();
  }
  open() {
    this.dirty = false;
    this.saveButton.disabled = true;
  }
  renderContent() {
    this.clearContent(this.listContainer, "custom-web-component");
    this.clearContent(this.havingContainer, "expression-component");
    this.columnsCount.textContent = this.selectedRectangle?.items.length;
    for (const item of this.selectedRectangle?.items) {
      const component = document.createElement('custom-web-component');
      component.setAttribute("data_type", item.data_type);
      component.setAttribute("column_type", item.column_type);
      component.setAttribute("business_name", item.business_name);
      component.setAttribute("technical_name", item.technical_name);
      component.setAttribute("semantic_type", item.semantic_type);
      component.setAttribute("key", item.key);
      component.setAttribute("aggregation", item.aggregation || "");
      component.setAttribute("parent_object", this.selectedRectangle.btype);
      // component.setAttribute("parent_object", "Aggregation");
      if (item.af && ["integer", "long", "float", "double", "big decimal"].includes(item.data_type.toLowerCase())) {
        component.menuElement.innerHTML = `<div>${item.af}</div>`;
      }

      component.shadowRoot.querySelector("#dropDowntooltip").addEventListener("open", () => this.setDirty());

      this.listContainer.appendChild(component);

    }
    
    const component = document.createElement("expression-component");
    component.selectedRectangle = this.selectedRectangle;
    this.havingContainer.append(component);
    this.trackInputs();
  }
  clearContent(container, component) {
    const content = container.querySelectorAll(component);
    content.forEach((item) => {
      item.remove();
    });
  }
  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      this.updateVisibility();
      if (newValue === "open") {
        this.open();
        this.updateSelectedRectangle();
        this.renderContent();
      }
    }
  }
  isInputValidate() {
    if (this.mainContainer.querySelector("smart-input").value == "") {
      alert("Name is required");
      return false
    }
    return true
  }
  save() {
    const dataObj = {
      id: this.selectedRectangle.id,
      alias: this.smartInput.value,
      business_name: this.smartInput.value,
      technical_name: this.smartInput.value,
      columns: [],
      having: this.havingContainer.children[1].expressionArea.getTextContent().trim(),
    }

    const valideDataTypes = ["integer", "long", "float", "double", "big decimal"]
    for (const component of this.listContainer.childNodes) {
      const isDataTypeValide = valideDataTypes.includes(component.getAttribute('data_type').toLowerCase());
      dataObj.columns.push({
        tid: this.selectedRectangle.items.filter(item => item.business_name === component.getAttribute("business_name") && item.technical_name === component.getAttribute("technical_name"))[0].tid,
        data_type: component.getAttribute('data_type'),
        column_type: component.getAttribute("column_type"),
        business_name: component.getAttribute("business_name"),
        technical_name: component.getAttribute("technical_name"),
        semantic_type: component.getAttribute("semantic_type"),
        key: component.getAttribute("key"),
        function: isDataTypeValide ? component.menuElement.childNodes[0].textContent : undefined
      })
    }

    console.log(dataObj);
    this.handleAjaxCallAndReRenderContent(dataObj);
  }
  handleAjaxCallAndReRenderContent(dataObj) {
    fetch("/saveAggregation", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(dataObj)
    }).then((response) => response.json())
      .then((data) => {
        this.selectedRectangle = data;
      }).catch((error) => {
        alert("Error: " + error);
        console.error("Error:", error);
      });
  }
  updateVisibility() {
    this.style.display = this.getAttribute("visibility") == "open" ? "block" : "none";
  }
  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", this.handleCloseButtonClick.bind(this));
    EventHandler.on(this.saveButton, "click", this.handleSaveButtonClick.bind(this));
    EventHandler.on(this.searchInput, "changing", this.handleSearchInputChange.bind(this));
  }
  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("smart-input");
    const buttons = this.shadowRoot.querySelector("expression-component").shadowRoot.querySelectorAll("#otherContent smart-button");
    const functions = this.shadowRoot.querySelector("expression-component").shadowRoot.querySelectorAll(".function-item");
    inputs.forEach(el => {
      el.addEventListener("change", () => this.setDirty());
    });
    buttons.forEach(el => {
      el.addEventListener("click", () => this.setDirty());
    });
    functions.forEach(el => {
      el.addEventListener("click", () => this.setDirty());
    });
  }
  setDirty() {
    this.dirty = true;
    this.saveButton.disabled = false;
  }
  handleCloseButtonClick() {
    if (this.dirty) {
      alert("You must save your changes before closing.");
      return; // prevent closing
    }
    this.setAttribute("visibility", "close");
  }
  handleSaveButtonClick() {
    if (this.isInputValidate()) {
      this.save();
      this.dirty = false;
      this.saveButton.disabled = true;
    }
  }
  handleSearchInputChange(e) {
    const searchQuery = e.target.value;

    for (const item of this.listContainer.childNodes) {
      if (item.getAttribute("business_name").includes(searchQuery.toLowerCase())) {
        item.style.display = "block";
      } else {
        item.style.display = "none";
      }
    }
  }
  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );
    // fetch("getItem?id=6", {
    //   method: "GET",
    //   headers: {
    //     "Content-Type": "application/json",
    //   },
    // }).then((res) => res.json())
    //   .then((data) => {
    //     this.selectedRectangle = data;
    //     this.renderContent();
    //   })
  }
}

customElements.define("aggregation-editor-component", AggregationEditorComponent);