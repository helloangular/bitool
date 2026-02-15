import "./source/modules/smart.button.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.listbox.js";
import "./customWebComponent.js";
import "./renameDialog.js";

import EventHandler from "./library/eventHandler.js";
import { request } from "./library/utils.js";
import StateManager from "./library/state-manager.js"

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
    overflow-y: auto;
    scrollbar-width: none;
  }
  ul {
    list-style: none;
    width: 100%;
    padding: 0;
    overflow-y: auto;
    scrollbar-width: none;
  }
  li custom-web-component {
    width: inherit;
  }
</style>

<div class="projection-container">
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
  </div>
  <div style="display:flex;justify-content:space-between;">
    <h3 style="margin-left:10px;">Function</h3>
    <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;"></smart-button>
  </div>
  <div class="content-area">
    <smart-input id="projectionName" name="alias" class="outlined" style="width:100%" value=""></smart-input>
    <smart-drop-down-button drop-down-open-mode="dropDownButton" placeholder="+">
      <smart-list-item>Calculated Column</smart-list-item>
      <smart-list-item>Currency Conversion Column</smart-list-item>
    </smart-drop-down-button>
    <smart-input id="searchInput" name="search" class="outlined" style="width:100%" placeholder="Search"></smart-input>
    <ul id="itemsList"></ul>
  </div>
</div>`;

// <smart-list-box id="itemsList" selection-mode="none" style="width:100%;height:100%"></smart-list-box>

class FunctionComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
  }

  connectedCallback() {
    this.shadowRoot.append(template.content.cloneNode(true));

    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.searchInput = this.shadowRoot.querySelector("#searchInput");
    this.itemsList = this.shadowRoot.querySelector("#itemsList");
    this.projectionName = this.shadowRoot.querySelector("#projectionName");
    this.columnListComponent = document.querySelector("column-list-component");
    this.dropDown = this.shadowRoot.querySelector("smart-drop-down-button");
    this.renameDialog = document.querySelector("rename-dialog");
    this.calculatedColumnComponent = document.querySelector("calculated-column-component");
    this.transformEditor = document.querySelector("transform-editor");
    this.selectedRectangle = null;
    this.state = null;

    this.close();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility" && newValue === "open") {
      this.open();
      this.updateSelectedRectangle();
      this.renderItems();
      this.updateFields();
      this.setUpEventListeners();
    } else if (name === "visibility") {
      this.close();
      if (this.style.visibility === "hidden") {
        this.resetFields();
        EventHandler.removeGroup("Function");
      }
    }
  }

  updateFields() {
    this.projectionName.value = this.selectedRectangle.alias;
    this.state = new StateManager(this.selectedRectangle);
  }

  resetFields() {
    this.projectionName.value = "";
    this.searchInput.value = "";
    this.itemsList.querySelectorAll("li").forEach((item) => item.remove());
    this.state.commit();
}

  setUpEventListeners() {
    EventHandler.on(this.dropDown.children[0], 'click', (e) => {
      if (e.target.textContent !== "" && e.target.textContent == "Calculated Column") {
        this.calculatedColumnComponent.setAttribute("visibility", "open");
      }
    }, false, "Function");

    EventHandler.on(this.calculatedColumnComponent, "new-column-added", (e) => {
      this.selectedRectangle = e.detail;
      this.renderItems();
    }, false, "Function");

    EventHandler.on(this.transformEditor, "transform-data", (e) => {
      const index = this.selectedRectangle.items.findIndex(t => t.business_name === e.detail.businessName);
      if (index != -1) {
        this.selectedRectangle.items[index]["transform"] = e.detail.transformations;
      }
      this.state.updateField("items", this.selectedRectangle.items);

      // Setting column is transformed or transform removed.
      this.itemsList.querySelectorAll("custom-web-component").forEach((component) => {
        if (component.getAttribute("business_name") === e.detail.businessName) {
          if (e.detail.transformations.length > 0) {
            component.setAttribute("transform", "YES");
          } else {
            component.setAttribute("transform", "NO");
          }
        }
      });

      this.setDirty();
      this.renderItems();
    }, false, "Function");

    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "Function");

    EventHandler.on(this.saveButton, "click", this.save.bind(this), false, "Function");

    EventHandler.on(this.searchInput, "changing", (e) => {
      this.handleSearchInputChange(e.detail.value.toLowerCase());
    }, false, "Function");

    EventHandler.on(this.renameDialog, "rename", (e) => {
      if (this.getAttribute("visibility") == "open") this.addAlias(e.detail);
    }, false, "Function");
    
    EventHandler.on(this.shadowRoot, "click", (e) => {
      this.closeItemDropdowns(e.target.business_name); 
    }, false, "Function");

    this.trackInputs();
  }

  open() {
    this.style.visibility = "visible";
    this.reset();
  }

  reset() {
    this.saveButton.disabled = true;
  }

  close() {
    if (this.state && this.state.isDirty()) {
      const wantToSave = confirm("Looks like you’ve made changes. Would you like to save them before leaving this panel?");
      if (!wantToSave) {
        this.style.visibility = "hidden";
        this.closeItemDropdowns();
      }
      return; // prevent closing
    }
    this.style.visibility = "hidden";
    this.closeItemDropdowns();
  }

  save() {
    // get values.
    const values = this.getValues();
    console.log(values);
    // make save request.
    request("/saveFunction", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      this.state.commit();
      this.reset();
      this.setAttribute("visibility", "close");
    }).catch((error) => console.error("Error:", error));
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll('smart-input:not([name="search"])');
    inputs.forEach(el => {
      EventHandler.on(el, "input", (e) => {
        if (e.target.name !== "search") this.state.updateField(e.target.name, e.target.value);
        this.setDirty()
      }, false, "Function");
    });
  }

  setDirty() {
    this.saveButton.disabled = !this.state.isDirty();
  }

  getValues() {
    return {
      id: this.selectedRectangle.id,
      ...this.state.updatedFields
    }
  }

  handleSearchInputChange(searchQuery) {
    const items = this.itemsList.querySelectorAll("li");
    Array.from(items).forEach((item) => {
      const component = item.querySelector("custom-web-component");
      const businessName = component.getAttribute("business_name").toLowerCase();
      item.style.display = businessName.includes(searchQuery)
        ? "block"
        : "none";
    });
  }

  handleDropdownItemClick({ item, itemText, businessName }) {
    if (itemText === "Exclude Column") {
      this.handleExcludeColumn(businessName);
    } else if (itemText === "Include Column") {
      this.handleIncludeColumn(businessName);
    } else if (itemText === "Add Alias") {
      this.openDialog(businessName);
    } else if (itemText === "Details") {
      this.calculatedColumnComponent.setAttribute("visibility", "open");
      this.calculatedColumnComponent.setAttribute("item", JSON.stringify(item));
    } else if (itemText === "Transform") {
      this.transformEditor.setAttribute("visibility", "open");
      this.transformEditor.setAttribute("item", JSON.stringify(item));
    }
  }

  openDialog(businessName) {
    const item = this.selectedRectangle.items.find(
      (item) => item.business_name === businessName
    );
    if (item) {
      this.renameDialog.setAttribute("business-name", item.business_name);
      this.renameDialog.setAttribute("visibility", "open");
    }
  }

  addAlias({ businessName, alias }) {
    const item = this.selectedRectangle.items.find(
      (item) => item.business_name === businessName
    );

    if (item) {
      // Directly update the alias on the found item.
      // An empty string for the alias will effectively remove it.
      item.alias = alias;
    }

    this.state.updateField("items", this.selectedRectangle.items);
    this.setDirty();
    this.renderItems();
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );
  }

  renderItems() {
    this.itemsList.querySelectorAll("li").forEach((item) => item.remove());

    this.selectedRectangle.items.forEach((item) => {
      const listItem = document.createElement("li");
      const component = this.createComponent(item, this.selectedRectangle.btype);
      listItem.appendChild(component);
      this.itemsList.appendChild(listItem);

      const dropdown = component.shadowRoot;
      EventHandler.on(dropdown, "click", (e) => {
        if (e.target.closest('.tooltip-item')) {
          this.handleDropdownItemClick({
            item,
            itemText: e.target.textContent,
            businessName: item.business_name
          });
        }
      }, false, "Function");
    });
  }

  closeItemDropdowns(business_name) {
    this.itemsList.querySelectorAll("custom-web-component").forEach((component) => {
      if (component.business_name !== business_name) {
        const dropdown = component.shadowRoot.querySelector("#dropDowntooltip");
        dropdown.close();
      }
    });
  }

  createComponent(item, btype) {
    const component = document.createElement("custom-web-component");
    component.setAttribute("data_type", item.data_type || "");
    component.setAttribute("column_type", item.column_type || "");
    component.setAttribute("business_name", item.alias && item.alias.trim() ? item.business_name + " as " + item.alias : item.business_name);
    component.setAttribute("technical_name", item.technical_name || "");
    component.setAttribute("semantic_type", item.semantic_type || "");
    component.setAttribute("key", item.key || "");
    component.setAttribute("aggregation", item.aggregation || "");
    component.setAttribute("parent_object", btype || "P");
    component.setAttribute("excluded", item.excluded);
    component.setAttribute("transform", item.transform && item.transform.length > 0 ? "YES" : "NO");
    return component;
  }

  handleExcludeColumn(businessName) {
    const index = this.selectedRectangle.items.findIndex(item => item.business_name === businessName);
    if (index !== -1) {
      this.selectedRectangle.items[index].excluded = "YES";
      this.itemsList.querySelectorAll("custom-web-component").forEach((component) => {
        if (component.getAttribute("business_name") === businessName) {
          component.setAttribute("excluded", "YES");
        }
      });
      this.state.updateField("items", this.selectedRectangle.items);
      this.setDirty();
    }
  }

  handleIncludeColumn(businessName) {
    const index = this.selectedRectangle.items.findIndex(item => item.business_name === businessName);
    if (index !== -1) {
      this.selectedRectangle.items[index].excluded = "NO";
      this.itemsList.querySelectorAll("custom-web-component").forEach((component) => {
        if (component.getAttribute("business_name") === businessName) {
          component.setAttribute("excluded", "NO");
        }
      });
      this.state.updateField("items", this.selectedRectangle.items);
      this.setDirty();
    }
  }
}

customElements.define("function-component", FunctionComponent);
