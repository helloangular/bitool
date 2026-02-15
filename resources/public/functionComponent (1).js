import "./source/modules/smart.button.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.listbox.js";
import "./customWebComponent.js";
import "./renameDialog.js";

import EventHandler from "./library/eventHandler.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .projection-container {
    display: flex;
    flex-direction: column;
    gap: 10px;
    height: 100%;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
  }
  .smart-button{
    --smart-button-padding: 4px;
    --smart-border-width: 0;
    --smart-font-size: 20px;
    --smart-ui-state-hover: white;
  }
  .content-area {
    padding: 10px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .dropdownbtn {
    width:30px;
    height:30px;
    border:none;
  }
  .dropdownlist {
    display: none;
    position: absolute;
    width: 200px;
    z-index: 1;
  }
</style>

<div class="projection-container">
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
  </div>
  <div style="display:flex;justify-content:space-between;">
    <h3 style="margin-left:10px;">Function</h3>
    <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;" disabled></smart-button>
  </div>
  <div class="content-area">
    <smart-input id="projectionName" class="outlined" style="width:100%" value=""></smart-input>
    <smart-drop-down-button dropDownOpenMode="dropDownButton" placeholder="+">
      <smart-list-item>Calculated Column</smart-list-item>
      <smart-list-item>Currency Conversion Column</smart-list-item>
    </smart-drop-down-button>
    <smart-input id="searchInput" class="outlined" style="width:100%" placeholder="Search"></smart-input>
  </div>
  <smart-list-box id="itemsList" selection-mode="none" style="width:100%;height:100%"></smart-list-box>
</div>`;

class FunctionComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.selectedRectangle = null;
    this.selectedAssociation = null;
    this.transformations = [];
    this.excludedColumns = []; // Initialize as an array
    this.renamedColumns = new Map();
    this.dirty = false;

    this.cacheElements();
    this.setUpEventListeners();
    this.updateVisibility();
    // this.updateSelectedRectangle();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue === "open") {
        this.updateSelectedRectangle();
        this.renderItems();
        this.dirty = false;
        this.saveButton.disabled = true;
      }
      this.updateVisibility();
    }
  }

  cacheElements() {
    this.closeButton = this.shadow.querySelector("#closeButton");
    this.saveButton = this.shadow.querySelector("#saveButton");
    this.searchInput = this.shadow.querySelector("#searchInput");
    this.itemsList = this.shadow.querySelector("#itemsList");
    this.projectionName = this.shadow.querySelector("#projectionName");
    this.columnListComponent = document.querySelector("column-list-component");
    this.dropDown = this.shadow.querySelector("smart-drop-down-button");
    this.renameDialog = document.querySelector("rename-dialog");
    this.calculatedColumnComponent = document.querySelector("calculated-column-component");
    this.transformEditor = document.querySelector("transform-editor");
  }

  setUpEventListeners() {
    EventHandler.on(this.dropDown.children[0], 'click', (e) => {
      this.calculatedColumnComponent.setAttribute("visibility", "open");
    });

    EventHandler.on(this.calculatedColumnComponent, "new-column-added", (e) => {
      this.selectedRectangle = e.detail;
      this.renderItems();
      this.setDirty();
    }, {}, "Function");

    EventHandler.on(this.transformEditor, "transform-data", (e) => {
      this.transformations.push(e.detail);
      this.setDirty();
    }, {}, "Function");

    EventHandler.on(this.closeButton, "click", () => {
      if (this.dirty) {
        alert("You must save your changes before closing.");
        return; // prevent closing
      }
      this.setAttribute("visibility", "close");
    }, {}, "Function");

    EventHandler.on(this.saveButton, "click", () => {
      this.save();
      this.dirty = false;
      this.saveButton.disabled = true;
      // this.columnListComponent.setAttribute("visibility", "open");
    }, {}, "Function");

    EventHandler.on(this.searchInput, "changing", (e) => {
      this.handleSearchInputChange(e.detail.value.toLowerCase());
    }, {}, "Function");

    EventHandler.on(this.renameDialog, "rename", (e) => {
      if (this.getAttribute("visibility") == "open") this.handleRename(e.detail);
      this.setDirty();
    }, {}, "Function");

    this.trackInputs();
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("smart-input");
    inputs.forEach(el => {
      // el.addEventListener("input", () => this.setDirty());
      el.addEventListener("change", () => this.setDirty());
    });
  }

  setDirty() {
    this.dirty = true;
    this.saveButton.disabled = false;
  }

  save() {
    const dataObj = {
      id: this.selectedRectangle.id,
      alias: this.projectionName.value,
      items: this.selectedRectangle.items,
      excludedColumns: this.excludedColumns,
      renamedColumns: Array.from(this.renamedColumns.values()),
      tranform: this.transformations
    };

    console.log(dataObj);
    this.handleAjaxCall(dataObj);
  }

  handleAjaxCall(dataObj) {
    fetch("/saveFunction", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(dataObj)
    }).then((response) => response.json())
      .then((data) => {
        this.selectedRectangle = data;
      }).catch((error) => {
        console.error("Error:", error);
      });
  }

  handleSearchInputChange(searchQuery) {
    const items = this.itemsList.querySelectorAll("smart-list-item");
    Array.from(items).forEach((item) => {
      const component = item.querySelector("custom-web-component");
      const businessName = component.getAttribute("business_name").toLowerCase();
      item.style.display = businessName.includes(searchQuery)
        ? "block"
        : "none";
    });
  }

  handleDropdownItemClick({ item, itemText, businessName }) {
    console.log(item)
    if (itemText === "Exclude Column") {
      this.handleExcludeColumn(businessName);
    } else if (itemText === "Include Column") {
      this.handleIncludeColumn(businessName);
    } else if (itemText === "Change Name") {
      this.handleChangeNameClick(businessName);
    } else if (itemText === "Details") {
      this.calculatedColumnComponent.setAttribute("visibility", "open");
      this.calculatedColumnComponent.setAttribute("item", JSON.stringify(item));
    } else if (itemText === "Transform") {
      console.log("open transform")
      const transformEditor = document.querySelector("transform-editor");
      transformEditor.setAttribute("visibility", "open");
      transformEditor.setAttribute("item", JSON.stringify(item));
    }
  }

  handleChangeNameClick(businessName) {
    const item = this.selectedRectangle.items.find(
      (item) => item.business_name === businessName
    );
    if (item) {
      this.renameDialog.setAttribute("business-name", item.business_name);
      this.renameDialog.setAttribute("technical-name", item.technical_name);
      this.renameDialog.setAttribute("visibility", "open");
    }
  }

  handleRename({
    oldBusinessName,
    oldTechnicalName,
    newBusinessName,
    newTechnicalName
  }) {
    const item = this.selectedRectangle.items.find(
      (item) => item.business_name === oldBusinessName
    );
    if (item) {
      item.business_name = newBusinessName;
      item.technical_name = newTechnicalName;

      this.renamedColumns.set(oldBusinessName, {
        tid: item.tid,
        oldBusinessName,
        oldTechnicalName,
        newBusinessName,
        newTechnicalName,
      });
      this.renderItems();
    }
  }

  updateVisibility() {
    this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );
    // fetch("getItem?id=7", {
    //   method: "GET",
    //   headers: {
    //     "Content-Type": "application/json",
    //   },
    // }).then((res) => res.json())
    //   .then((data) => {
    //     this.selectedRectangle = data;
    //     this.renderItems();
    //   })
  }

  renderItems() {
    this.itemsList.querySelectorAll("smart-list-item").forEach((item) => item.remove());

    this.selectedRectangle.items.forEach((item) => {
      const listItem = document.createElement("smart-list-item");
      const component = this.createComponent(item, this.selectedRectangle.btype);
      listItem.appendChild(component);
      this.itemsList.appendChild(listItem);

      const dropdownTooltip = component.shadowRoot.querySelector("#dropDowntooltip");
      if (dropdownTooltip) {
        EventHandler.on(dropdownTooltip, "click", (e) => {
          const tooltipItem = e.target.closest(".tooltip-item");
          if (tooltipItem) {
            this.handleDropdownItemClick({
              item,
              itemText: tooltipItem.textContent,
              businessName: item.business_name
            })
            this.setDirty();
          }
        }, {}, "Function");
      }
    });
  }

  createComponent(item, btype) {
    const component = document.createElement("custom-web-component");
    component.setAttribute("data_type", item.data_type || "");
    component.setAttribute("column_type", item.column_type || "");
    component.setAttribute("business_name", item.business_name || "");
    component.setAttribute("technical_name", item.technical_name || "");
    component.setAttribute("semantic_type", item.semantic_type || "");
    component.setAttribute("key", item.key || "");
    component.setAttribute("aggregation", item.aggregation || "");
    component.setAttribute("parent_object", btype || "P");
    if (item.excluded) {
      component.setAttribute("excluded", item.excluded === "YES" ? true : false);
    } else {
      const isExcluded = this.excludedColumns.some(excludedItem => excludedItem.business_name === item.business_name);
      component.setAttribute("excluded", isExcluded);
    }
    return component;
  }

  handleExcludeColumn(businessName) {
    const item = this.selectedRectangle.items.find(item => item.business_name === businessName);
    if (item) {
      this.excludedColumns.push({ tid: item.tid, technical_name: item.technical_name, business_name: item.business_name });
      this.itemsList.querySelectorAll("custom-web-component").forEach((component) => {
        if (component.getAttribute("business_name") === businessName) {
          component.setAttribute("excluded", true);
        }
      });
    }
  }

  handleIncludeColumn(businessName) {
    this.excludedColumns = this.excludedColumns.filter(excludedItem => excludedItem.business_name !== businessName);
    this.itemsList.querySelectorAll("custom-web-component").forEach((component) => {
      if (component.getAttribute("business_name") === businessName) {
        component.setAttribute("excluded", false);
      }
    });
  }
}

customElements.define("function-component", FunctionComponent);
