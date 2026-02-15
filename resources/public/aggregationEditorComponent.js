import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";
import "./source/modules/smart.button.js";
import "./source/modules/smart.input.js";

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
            <smart-input name="alias" class="outlined" style="width:100%" required></smart-input>
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
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
  }

  connectedCallback() {
    this.shadowRoot.append(template.content.cloneNode(true));

    this.mainContainer = this.shadowRoot.querySelector(".filter-container");
    this.searchInput = this.shadowRoot.querySelector("#searchInput");
    this.havingContainer = this.shadowRoot.querySelector("#having-container");
    this.listContainer = this.shadowRoot.querySelector("#list-container");
    this.closeButton = this.shadowRoot.querySelector(".close-button");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.columnsCount = this.shadowRoot.querySelector("#columns-count");
    this.smartInput = this.mainContainer.querySelector("smart-input");

    this.selectedRectangle = null;
    this.state = null;

    this.close();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    // console.log(`${name} -> ${oldValue} -> ${newValue}`);
    if (name === "visibility") {
      if (newValue === "open") {
        this.open();
        this.updateSelectedRectangle();
        this.renderContent();
        this.updateFields();
        this.setUpEventListeners();
      } else {
        this.close();
        if (this.style.visibility === "hidden") EventHandler.removeGroup("Aggregation");
      }
    }
  }

  updateFields() {
    if (this.selectedRectangle == null) {
      setTimeout(() => {
        this.updateFields();
      }, 100);
      return;
    }
    this.smartInput.value = this.selectedRectangle.alias;
    this.havingContainer.querySelector("expression-component").expressionArea.setTextContent(this.selectedRectangle.having || "");
    this.state = new StateManager(this.selectedRectangle);
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "Aggregation");
    EventHandler.on(this.saveButton, "click", this.save.bind(this), false, "Aggregation");
    EventHandler.on(this.searchInput, "changing", this.handleSearchInputChange.bind(this), false, "Aggregation");
    EventHandler.on(this.shadowRoot, "click", (e) => this.closeItemDropdowns(e.target.business_name), false, "Aggregation");
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
      if (!wantToSave) this.style.visibility = "hidden";
      return; // prevent closing
    }
    this.style.visibility = "hidden";
  }

  save() {
    if (!this.isInputValidate()) return;
    // get values.
    const values = this.getValues();
    console.log(values);
    // make save request.
    request("/saveAggregation", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      this.state.commit();
      this.reset();
      this.setAttribute("visibility", "close");
    })
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("smart-input");
    const expressionComponent = this.shadowRoot.querySelector("expression-component");
    const textarea = expressionComponent.expressionArea.textarea;
    const buttons = expressionComponent.shadowRoot.querySelectorAll("#otherContent smart-button");
    const functionsList = expressionComponent.shadowRoot.querySelector("#functionsList");
    [...inputs, textarea].forEach(el => {
      EventHandler.on(el, "input", (e) => {
        e.target.tagName === "TEXTAREA" ?
          this.state.updateField("having", e.target.value) :
          this.state.updateField(e.target.name, e.target.value);
        this.setDirty()
      }, false, "Aggregation");
    });
    buttons.forEach(el => {
      EventHandler.on(el, "click", () => {
        this.state.updateField("having", this.shadowRoot.querySelector("expression-component").expressionArea.getTextContent());
        this.setDirty()
      }, false, "Aggregation");
    });
    EventHandler.on(functionsList, "click", (e) => {
      if (e.target.closest(".function-item")) {
        this.state.updateField("having", this.shadowRoot.querySelector("expression-component").expressionArea.getTextContent());
        this.setDirty();
      }
    }, false, "Aggregation");
  }

  setDirty() {
    this.saveButton.disabled = !this.state.isDirty();
  }

  getValues() {
    const values = {
      id: this.selectedRectangle.id,
      ...this.state.updatedFields,
      group: this.getNonAggregatedColumns(this.selectedRectangle.items)
    }
    
    return values;
  }

  getNonAggregatedColumns(items) {
    return items.filter(item => (!item.af || item.af === "None" || item.af.trim() === "")).map(item => ({
      tid: item.tid,
      business_name: item.business_name,
      technical_name: item.technical_name
    }));
  }

  renderContent() {
    this.shadowRoot.querySelectorAll("smart-input").forEach(si => si.value = "");
    this.clearContent(this.listContainer, "custom-web-component");
    this.clearContent(this.havingContainer, "expression-component");

    this.columnsCount.textContent = this.selectedRectangle.items.length;
    for (const item of this.selectedRectangle.items) {
      const component = document.createElement('custom-web-component');
      component.setAttribute("data_type", item.data_type);
      component.setAttribute("column_type", item.column_type);
      component.setAttribute("business_name", item.business_name);
      component.setAttribute("technical_name", item.technical_name);
      component.setAttribute("semantic_type", item.semantic_type);
      component.setAttribute("key", item.key);
      component.setAttribute("aggregation", item.aggregation || "");
      component.setAttribute("parent_object", this.selectedRectangle.btype);
      if (item.af && ["integer", "long", "float", "double", "big decimal"].includes(item.data_type.toLowerCase())) {
        component.menuElement.innerHTML = `<div>${item.af}</div>`;
      }

      this.listContainer.appendChild(component);

      EventHandler.on(component.shadowRoot, "click", (e) => {
        if (e.target.closest('.tooltip-item')) {
          this.handleDropdownItemClick({
            itemText: e.target.textContent,
            businessName: item.business_name
          });
        }
      }, false, "Aggregation");
    }

    const component = document.createElement("expression-component");
    component.selectedRectangle = this.selectedRectangle;
    this.havingContainer.append(component);
  }

  clearContent(container, component) {
    const content = container.querySelectorAll(component);
    content.forEach((item) => {
      item.remove();
    });
  }

  closeItemDropdowns(business_name) {
    this.listContainer.querySelectorAll("custom-web-component").forEach((component) => {
      if (component.business_name !== business_name) {
        const dropdown = component.shadowRoot.querySelector("#dropDowntooltip");
        dropdown.close();
      }
    });
  }

  isInputValidate() {
    if (this.mainContainer.querySelector("smart-input").value == "") {
      alert("Name is required");
      return false
    }
    return true
  }

  handleDropdownItemClick({ itemText, businessName }) {
    const index = this.selectedRectangle.items.findIndex(item => item.business_name === businessName);
    if (index !== -1) {
      this.selectedRectangle.items[index].af = itemText;
      this.state.updateField("items", this.selectedRectangle.items);
      this.setDirty();
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
  }
}

customElements.define("aggregation-editor-component", AggregationEditorComponent);
