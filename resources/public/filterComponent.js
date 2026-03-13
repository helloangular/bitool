import "./source/modules/smart.listbox.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.button.js";

import EventHandler from "./library/eventHandler.js";
import { request } from "./library/utils.js";
import StateManager from "./library/state-manager.js";

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
  }
  .header {
    display: flex;
    justify-content: flex-end;
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
    <smart-button content="&#9747;" class="close-button"></smart-button>
  </div>
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;border-width:0px;"></smart-button>
  </div>
  <div class="filter-group">
    <div class="filter-name">
      <label>Name*</label>
      <smart-input id="filterName" name="alias" class="outlined" style="width:100%"></smart-input>
    </div>
  </div>
</div>`;

class FilterComponent extends HTMLElement {
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
    this.columnListComponent = document.querySelector("column-list-component");
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
      this.updateFields();
      this.appendExpression();
      this.setupEventListeners();
    } else {
      this.close();
      if (this.style.visibility === "hidden") {
        this.resetFields();
        EventHandler.removeGroup("Filter");
      }
    }
  }

  setupEventListeners() {
    EventHandler.on(this.saveButton, "click", this.save.bind(this), false, "Filter");
    EventHandler.on(this.closeButton, "click", this.close.bind(this), false, "Filter");
    this.trackInputs();
  }

  updateFields() {
    this.filterName.values = this.selectedRectangle.business_name || "";
    this.state = new StateManager(this.selectedRectangle);
  }

  resetFields() {
    this.filterName.value = "";
    const expressionComponent = this.shadowRoot.querySelector("expression-component");
    expressionComponent.expressionArea.setTextContent("");
    this.state.commit();
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
    // get values.
    const values = this.getValues();
    console.log(values);
    // make save request.
    request("/saveFilter", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      this.state.commit();
      this.reset();
      this.setAttribute("visibility", "close");
    }).catch((error) => console.error(error))
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("smart-input");
    const expressionComponent = this.shadowRoot.querySelector("expression-component");
    const textarea = expressionComponent.expressionArea.textarea;
    const buttons = expressionComponent.querySelectorAll("#otherContent smart-button");
    const functionsList = expressionComponent.shadowRoot.querySelector("#functionsList");
    [...inputs, textarea].forEach(el => {
      EventHandler.on(el, "input", (e) => {
        (e.target.tagName === "TEXTAREA") ? 
          this.state.updateField("having", e.target.value) :
          this.state.updateField(e.target.name, e.target.value);
        this.setDirty()
      }, false, "Filter");
    });
    buttons.forEach(el => {
      EventHandler.on(el, "click", () => {
        this.state.updateField("having", expressionComponent.expressionArea.getTextContent());
        this.setDirty()
      }, false, "Filter");
    });
    EventHandler.on(functionsList, "click", (e) => {
      if (e.target.closest(".function-item")) {
        this.state.updateField("having", expressionComponent.expressionArea.getTextContent());
        this.setDirty();
      }
    }, false, "Filter");
  }

  setDirty() {
    this.saveButton.disabled = !this.state.isDirty();
  }

  getValues() {
    return {
      id: this.selectedRectangle.id,
      ...this.state.updatedFields
    };
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );
  }

  appendExpression() {
    // Remove any stale expression-component before appending a fresh one
    this.shadowRoot.querySelectorAll("expression-component").forEach(el => el.remove());
    const component = document.createElement("expression-component");
    component.selectedRectangle = this.selectedRectangle;
    this.mainContainer.appendChild(component);
  }
}

customElements.define("filter-component", FilterComponent);
