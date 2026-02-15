import "./Library/ifElse.js";
import "./Library/ifElseIfElse.js";
import "./Library/multiIf.js";
import "./Library/case.js";
import "./Library/condition.js";
import "./Library/patternMatch.js";
import EventHandler from "../../eventHandler.js";

const template = document.createElement("template");
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="container padding">
      <div style="display:flex;justify-content:flex-end">
          <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
      </div>
      <div style="display:flex;justify-content:space-between;">
          <h3 style="margin-left:10px;" id="label"></h3>
          <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;"></smart-button>
      </div>
      <div id="content"></div>
    </div>
`;

class ControlFlowComponent extends HTMLElement {
  constructor() {
    super();
    const shadowRoot = this.attachShadow({ mode: "open" });
    shadowRoot.appendChild(template.content.cloneNode(true));

    this.cacheElements();
    this.setUpEventListeners();

    this.component = null;
  }

  static get observedAttributes() {
    return ["conditions", "visibility"];
  }

  connectedCallback() {
    this.updateVisibility();
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "conditions" && oldValue !== newValue) {
      this.renderUI({ condition: newValue, container: this.content });
    } else if (name === "visibility") {
      this.updateVisibility();
    }
  }

  cacheElements() {
    this.content = this.shadowRoot.querySelector("#content");
    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.label = this.shadowRoot.querySelector("#label");

    // Set label text based on the condition
    const condition = this.getAttribute("conditions");
    if (condition) {
      this.label.textContent = condition.charAt(0).toUpperCase() + condition.slice(1);
    }
  }

  setUpEventListeners() {
    EventHandler.on(this.saveButton, 'click', () => this.save());
    EventHandler.on(this.closeButton, 'click', () => this.closePanel());
  }

  closePanel() {
    this.setAttribute("visibility", "");
  }

  updateVisibility() {
    this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  renderUI({ condition, container }) {
    const componentMap = {
      "if-else": "if-else-component",
      "if-elif-else": "if-elseif-else",
      "multi-if": "multi-if",
      "case": "my-case",
      "cond": "my-condition",
      "pattern-match": "pattern-match-editor"
    };

    const componentTag = componentMap[condition];
    if (componentTag) {
      for (const child of container.children) {
        container.removeChild(child); // Clear previous components
      }
      this.component = document.createElement(componentTag);
      container.appendChild(this.component); // Append to light DOM, so it appears in <slot>
    }
  }

  save() {
    if (this.component && typeof this.component.save === 'function') {
      this.component.save();
    } else {
      console.warn("No component to save or save method not defined.");
    }
  }
}

customElements.define("control-flow-component", ControlFlowComponent);