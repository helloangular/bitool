import "./Library/ifElse.js";
import "./Library/ifElseIfElse.js";
import "./Library/multiIf.js";
import "./Library/case.js";
import "./Library/condition.js";
import "./Library/patternMatch.js";
import EventHandler from "../library/eventHandler.js";
import { request } from "../library/utils.js";

const template = document.createElement("template");
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <style>
      :host {
        display: block;
        height: 100%;
      }

      .container {
        box-sizing: border-box;
        height: 100%;
        display: flex;
        flex-direction: column;
        overflow: hidden;
        background: white;
      }

      #content {
        flex: 1 1 auto;
        min-height: 0;
        overflow: auto;
        padding: 0 10px 16px;
      }
    </style>
    <div class="container padding">
      <div style="display:flex;justify-content:flex-end">
          <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
      </div>
      <div style="display:flex;justify-content:space-between;">
          <h3 style="margin-left:10px;" id="label"></h3>
          <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;"></smart-button>
      </div>
      <select id="conditionals" name="conditionals">
        <option value="if-else">IF ELSE</option>
        <option value="if-elif-else">IF ELIF ELSE</option>
        <option value="multi-if">MULTI IF</option>
        <option value="case">CASE</option>
        <option value="cond">CONDITION</option>
        <option value="pattern-match">PATTERN MATCH</option>
      </select>
      <details id="columns-section" style="margin:8px 10px;">
        <summary style="cursor:pointer;font-size:12px;color:#555;">Available Columns</summary>
        <div id="columns-list" style="max-height:120px;overflow-y:auto;font-size:11px;padding:4px 8px;background:#f9f9f9;border:1px solid #ddd;border-radius:4px;margin-top:4px;"></div>
      </details>
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
    return ["visibility"];
  }

  connectedCallback() {
    this.updateVisibility();
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue === "open") {
        this.loadFromRect();
      } else {
        this.renderUI({ condition: "if-else", container: this.content });
      }
      this.updateVisibility();
    }
  }

  cacheElements() {
    this.content = this.shadowRoot.querySelector("#content");
    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.label = this.shadowRoot.querySelector("#label");
    this.select = this.shadowRoot.querySelector("#conditionals");
    this.columnsSection = this.shadowRoot.querySelector("#columns-section");
    this.columnsList = this.shadowRoot.querySelector("#columns-list");
  }

  setUpEventListeners() {
    EventHandler.on(this.saveButton, "click", () => this.save());
    EventHandler.on(this.closeButton, "click", () => this.closePanel());
    EventHandler.on(this.select, "change", () => this.panelChange());
  }

  /** Load saved data from the selected rectangle (populated by getItem) */
  loadFromRect() {
    const rect = (window.data?.rectangles || []).find(
      (r) => String(r.id) === String(window.data?.selectedRectangle)
    );

    const condType = rect?.cond_type || "if-else";
    this.select.value = condType;
    this.label.textContent = rect?.alias || rect?.business_name || "Conditional";

    // Show available columns from parent node
    const items = rect?.items || [];
    if (items.length > 0) {
      this.columnsList.innerHTML = items
        .map((col) => `<div style="padding:1px 0;"><code>${col.business_name || col.technical_name}</code> <span style="color:#999;">(${col.data_type || ""})</span></div>`)
        .join("");
      this.columnsSection.style.display = "block";
    } else {
      this.columnsSection.style.display = "none";
    }

    this.renderUI({ condition: condType, container: this.content });

    // If we have saved branches, populate the sub-component after it renders
    if (rect && this.component && typeof this.component.loadData === "function") {
      setTimeout(() => {
        this.component.loadData(rect);
      }, 50);
    }
  }

  panelChange() {
    this.renderUI({ condition: this.select.value, container: this.content });
  }

  closePanel() {
    this.setAttribute("visibility", "");
  }

  updateVisibility() {
    this.style.display =
      this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  renderUI({ condition, container }) {
    const componentMap = {
      "if-else": "if-else-component",
      "if-elif-else": "if-elseif-else",
      "multi-if": "multi-if",
      case: "my-case",
      cond: "my-condition",
      "pattern-match": "pattern-match-editor",
    };

    const componentTag = componentMap[condition];
    if (componentTag) {
      // Clear previous components
      while (container.firstChild) {
        container.removeChild(container.firstChild);
      }
      this.component = document.createElement(componentTag);
      container.appendChild(this.component);
    }
  }

  async save() {
    if (!this.component) {
      console.warn("No component to save.");
      return;
    }

    const id = window.data?.selectedRectangle;
    if (!id) {
      console.warn("No selected rectangle to save conditional.");
      return;
    }

    // Collect data from sub-component
    let payload = {};

    if (typeof this.component.collectData === "function") {
      payload = this.component.collectData() || {};
    } else if (typeof this.component.save === "function") {
      // Fallback: call legacy save (which calls storeData internally)
      this.component.save();
      return;
    }

    try {
      const data = await request("/saveConditional", {
        method: "POST",
        body: { id, cond_type: this.select.value, ...payload },
      });
      console.log("Conditional saved:", data);
    } catch (err) {
      console.error("Error saving conditional:", err);
    }
  }
}

customElements.define("control-flow-component", ControlFlowComponent);
