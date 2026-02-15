import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

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
      <smart-input id="businessName" name="business_name" class="outlined" style="width:100%"></smart-input>
    </div>
    <div class="filter-name">
      <label>Technical Name:</label>
      <smart-input id="technicalName" name="technical_name" class="outlined" style="width:100%"></smart-input>
    </div>
    <div class="filter-name">
      <label>Data Type:</label>
      <select name="data_type" class="dropdown">
        <option value="01">Integer</option>
        <option value="02">String</option>
        <option value="03">Boolean</option>
      </select>
    </div>
    <div class="filter-name">
      <label>Length:</label>
      <smart-input id="length" name="length" class="outlined" style="width:100%"></smart-input>
    </div>
  </div>
</div>`;

class CalculatedColumnComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
  }

  connectedCallback() {
    this.shadowRoot.append(template.content.cloneNode(true));

    this.mainContainer = this.shadowRoot.querySelector(".filter-container");
    this.filterName = this.shadowRoot.querySelector("#filterName");
    this.closeButton = this.shadowRoot.querySelector(".close-button");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.businessName = this.shadowRoot.querySelector("#businessName");
    this.technicalName = this.shadowRoot.querySelector("#technicalName");
    this.length = this.shadowRoot.querySelector("#length");
    this.dropdown = this.shadowRoot.querySelector("select");
    this.selectedRectangle = null;
    this.item = null;
    this.state = null;

    this.close();

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
      if (newValue === "open") {
        this.open();
        this.updateSelectedRectangle();
        this.setupEventListeners();

        // clear form inputs.
        this.updateFields({});
        // Initialize state.
        this.state = new StateManager({
          business_name: "",
          technical_name: "",
          length: "",
          data_type: this.dropdown.selectedOptions[0].textContent,
          having: ""
        });
      } else {
        this.close();
        if (this.style.visibility === "hidden") EventHandler.removeGroup("Calculated-Column");
      }
    } else if (name === "item") {
      this.item = JSON.parse(newValue);
      this.updateFields(this.item);
    }
  }

  setupEventListeners() {
    EventHandler.on(this.saveButton, "click", this.save.bind(this), false, "Calculated-Column");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "Calculated-Column");
    this.trackInputs();
  }

  open() {
    this.style.visibility = "visible";
    this.resetFields();
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
    // get values.
    const values = this.getValues();
    console.log(values);
    // make save request.
    request("/saveColumn", {
      method: "POST",
      body: values
    }).then((data) => {
      this.dispatchEvent(new CustomEvent("new-column-added", { detail: data }));
      this.state.commit();
      this.reset();
      this.setAttribute("visibility", "close");
    })
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("smart-input, select");
    const buttons = this.expressionComponent.shadowRoot.querySelectorAll("#otherContent smart-button");
    const functionsList = this.expressionComponent.shadowRoot.querySelector("#functionsList");
    const textarea = this.expressionComponent.expressionArea.textarea;
    [...inputs, textarea].forEach(el => {
      EventHandler.on(el, "input", (e) => {
        if (e.target.tagName === "SELECT") {
          this.state.updateField(e.target.name, e.target.selectedOptions[0].textContent);
        } else {
          e.target.tagName === "TEXTAREA" ?
            this.state.updateField("having", e.target.value) :
            this.state.updateField(e.target.name, e.target.value);
        }
        this.setDirty();
      }, false, "Calculated-Column");
    });
    buttons.forEach(el => {
      EventHandler.on(el, "click", () => {
        this.state.updateField("having", this.expressionComponent.expressionArea.getTextContent());
        this.setDirty()
      }, false, "Calculated-Column");
    });
    EventHandler.on(functionsList, "click", (e) => {
      if (e.target.closest(".function-item")) {
        this.state.updateField("having", this.expressionComponent.expressionArea.getTextContent());
        this.setDirty()
      }
    }, false, "Calculated-Column");
  }

  setDirty() {
    this.saveButton.disabled = !this.state.isDirty();
  }

  getValues() {
    const select = this.mainContainer.querySelector("select");
    return {
      id: this.selectedRectangle.id,
      alias: this.businessName.value,
      business_name: this.businessName.value,
      technical_name: this.technicalName.value,
      data_type: select.querySelector("option[value='" + select.value + "']").innerText,
      length: this.length.value,
      expression: this.expressionComponent.expressionArea.getTextContent(),
      column_type: "calculated"
    }
  }

  updateFields(data) {
    this.businessName.value = data.business_name || "";
    this.technicalName.value = data.technical_name || "";
    this.length.value = data.length ? data.length.toString() : "0";
    const index = Array.from(this.dropdown.children).findIndex(c => c.textContent.toLowerCase() === data.data_type);
    if (index !== -1) this.dropdown.selectedIndex = index;
    this.expressionComponent.expressionArea.setTextContent(data.expression || "");

    this.state = new StateManager({
      business_name: data.business_name,
      technical_name: data.technical_name,
      length: data.length ||  "0",
      data_type: this.dropdown.selectedOptions[0].textContent,
      having: data.expression || ""
    });
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );
    this.expressionComponent.selectedRectangle = this.selectedRectangle;
  }
}

customElements.define("calculated-column-component", CalculatedColumnComponent);
