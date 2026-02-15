import "./source/modules/smart.listbox.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.button.js";

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
  }
  .header {
    display: flex;
    justify-content: flex-end;
    margin-bottom: 20px;
  }
  .smart-button {
    --smart-button-padding: 6px;
    --smart-border-width: 1px;
    --smart-font-size: 10px;
    --smart-ui-state-hover: #f0f0f0;
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
  .filter-expression {
    width: 100%;
    min-height: 60px;
    border: 1px solid #ccc;
    margin-bottom: 10px;
    padding: 8px;
  }
  .expression-buttons {
    display: flex;
    gap: 10px;
  }
  .tabs-container {
    margin-top: 20px;
    border: 1px solid #ccc;
  }
  .tab-header {
    display: flex;
    background-color: #f0f0f0;
    border-bottom: 1px solid #ccc;
  }
  .tab {
    padding: 8px 15px;
    cursor: pointer;
  }
  .tab.active {
    background-color: white;
    border-bottom: 2px solid #007bff;
  }
  .tab-content {
    padding: 15px;
    max-height: 400px;
    overflow-y: auto;
  }
  .function-item {
    padding: 8px;
    border-bottom: 1px solid #eee;
    cursor: pointer;
  }
  .function-item:hover {
    background-color: #f8f9fa;
  }
  .function-name {
    color: #007bff;
    font-weight: bold;
  }
  .function-desc {
    font-size: 12px;
    color: #666;
    margin-top: 4px;
  }
  .search-container {
    display: flex;
    gap: 10px;
    margin-bottom: 10px;
  }
  .function-type-select {
    width: 150px;
  }
  .search-box {
    flex-grow: 1;
    padding: 8px;
    border: 1px solid #ccc;
  }
  .error-message {
    background-color: #ffebee;
    color: #c62828;
    padding: 8px;
    margin-top: 5px;
    border-radius: 4px;
    font-size: 12px;
  }
  .button-group {
    margin-bottom: 20px;
  }
  .button-group-title {
    font-weight: bold;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .button-group-content {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
  }
  .info-icon {
    color: #666;
    cursor: help;
  }
</style>
<div>
  <div class="filter-group">
    <label>Expression</label>
    <div class="expression-buttons" style="margin-bottom: 10px;">
      <smart-button id="insertValuesBtn">Insert Values</smart-button>
      <smart-button id="validateBtn">Validate</smart-button>
    </div>
    <auto-complete-textarea id="expressionArea"></auto-complete-textarea>
    <!-- <textarea class="filter-expression" id="expressionArea"></textarea> -->
    <div class="error-message" id="errorMessage" style="display: none;">
      The condition of the filter is empty.
    </div>
  </div>
  
  <div class="tabs-container">
    <div class="tab-header">
      <div class="tab active" data-tab="functions">Functions (6)</div>
      <div class="tab" data-tab="columns">Columns (13)</div>
      <div class="tab" data-tab="parameters">Parameters</div>
      <div class="tab" data-tab="other">Other</div>
    </div>
    <div class="tab-content">
      <div class="search-container">
        <smart-input id="functionTypeSelect" class="function-type-select" dropDownButtonPosition="right" style="display: none;"></smart-input>
        <smart-input class="search-box" id="searchInput" placeholder="Search"></smart-input>
      </div>
      <div id="functionsList">
      </div>
      <div id="parametersList">
      </div>
      <div id="otherContent" style="display: none;overflow-y: scroll;">
        <div class="button-group">
          <div class="button-group-title">
            Operators <span class="info-icon">ⓘ</span>
          </div>
          <div class="button-group-content">
            <smart-button class="smart-button">+</smart-button>
            <smart-button class="smart-button">-</smart-button>
            <smart-button class="smart-button">/</smart-button>
            <smart-button class="smart-button">*</smart-button>
            <smart-button class="smart-button">(</smart-button>
            <smart-button class="smart-button">)</smart-button>
            <smart-button class="smart-button">=</smart-button>
            <smart-button class="smart-button">||</smart-button>
            <smart-button class="smart-button">AND</smart-button>
            <smart-button class="smart-button">OR</smart-button>
            <smart-button class="smart-button">NOT</smart-button>
            <smart-button class="smart-button">BETWEEN</smart-button>
            <smart-button class="smart-button"><</smart-button>
            <smart-button class="smart-button">></smart-button>
            <smart-button class="smart-button"><=</smart-button>
            <smart-button class="smart-button">>=</smart-button>
            <smart-button class="smart-button">=</smart-button>
            <smart-button class="smart-button">!=</smart-button>
            <smart-button class="smart-button">LIKE</smart-button>
            <smart-button class="smart-button">IS NULL</smart-button>
          </div>
        </div>
        <div class="button-group">
          <div class="button-group-title">
            Predicates <span class="info-icon">ⓘ</span>
          </div>
          <div class="button-group-content">
            <smart-button class="smart-button">ANY</smart-button>
            <smart-button class="smart-button">SOME</smart-button>
            <smart-button class="smart-button">ALL</smart-button>
            <smart-button class="smart-button">BETWEEN</smart-button>
            <smart-button class="smart-button">CONTAINS</smart-button>
            <smart-button class="smart-button">EXISTS</smart-button>
            <smart-button class="smart-button">IN</smart-button>
            <smart-button class="smart-button">LIKE</smart-button>
            <smart-button class="smart-button">MEMBER OF</smart-button>
            <smart-button class="smart-button">NULL</smart-button>
          </div>
        </div>
        <div class="button-group">
          <div class="button-group-title">
            Case Expressions <span class="info-icon">ⓘ</span>
          </div>
          <div class="button-group-content">
            <smart-button class="smart-button">CASE WHEN THEN ELSE END</smart-button>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>`;

class ExpressionComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.selectedRectangle = null;
    this.selectedAssociation = null;
    this.selectedValues = new Set();

    this.expressionArea = this.shadow.querySelector("#expressionArea");
    this.errorMessage = this.shadow.querySelector("#errorMessage");
    this.insertValuesBtn = this.shadow.querySelector("#insertValuesBtn");
    this.validateBtn = this.shadow.querySelector("#validateBtn");
    this.searchInput = this.shadow.querySelector("#searchInput");
    this.functionsList = this.shadow.querySelector("#functionsList");
    this.functionTypeSelect = this.shadow.querySelector("#functionTypeSelect");
    this.tabButtons = this.shadow.querySelectorAll(".tab");
    this.insertValuesDialog = document.querySelector(".insert-values-dialog");

    this.valuesList = [
      "800000111",
      "800000112",
      "800000113",
      "800000114",
      "800000115",
      "800000116",
      "800000117",
      "800000118",
      "800000119",
    ];

    if (this.insertValuesDialog) {
      this.initializeDialog();

      EventHandler.on(this.insertValuesDialog, "open", () => {
        this.resetDialogState();
      });

      EventHandler.on(this.insertValuesDialog, "close", () => {
        this.selectedValues.clear();
      });
    }

    this.functionTypes = [
      "All Functions",
      "String",
      "Numeric",
      "Datetime",
      "Miscellaneous",
      "Security",
      "Fulltext",
      "Data Type Conversion",
    ];

    this.functions = [
      {
        name: "ABAP_ALPHANUM",
        type: "String",
        description:
          "Converts a string to what would result if the string was transformed into a variable name of type and then converted back to a string.",
      },
      {
        name: "ABAP_LOWER",
        type: "String",
        description:
          "Converts all characters in a specified string to lowercase.",
      },
      {
        name: "ABAP_NUMC",
        type: "Data Type Conversion",
        description:
          "Converts an input string to a string of a specified length, that contains only digits.",
      },
      {
        name: "ABAP_UPPER",
        type: "String",
        description:
          "Converts all characters in the specified string to uppercase.",
      },
      {
        name: "ABS",
        type: "Numeric",
        description: "Returns the absolute value of a numeric argument.",
      },
      {
        name: "ACOS",
        type: "Numeric",
        description: "Returns the arc cosine of a number.",
      },
    ];

    this.setupEventListeners();
    this.initializeFunctionTypeSelect();
    this.populateFunctions();
  }

  saveFilter() {
    window.data.associations = window.data.associations.map((association) => {
      if (
        (association.target1 === this.selectedRectangle.alias &&
          association.target2 === this.selectedAssociation.alias) ||
        (association.target2 === this.selectedRectangle.alias &&
          association.target1 === this.selectedAssociation.alias)
      ) {
        return {
          ...association,
          filter: {
            // name: this.filterName.value,
            expression: this.expressionArea.getTextContent(),
          },
        };
      }

      return association;
    });
  }

  initializeFunctionTypeSelect() {
    this.functionTypeSelect.dataSource = this.functionTypes;
    this.functionTypeSelect.value = "All Functions";
  }

  setupEventListeners() {
    this.shadow.querySelectorAll(".tab").forEach((tab) => {
      EventHandler.on(tab, "click", () => {
        this.shadow
          .querySelectorAll(".tab")
          .forEach((t) => t.classList.remove("active"));
        tab.classList.add("active");

        const tabType = tab.getAttribute("data-tab");
        const functionTypeSelect = this.shadow.querySelector(
          "#functionTypeSelect"
        );
        const searchContainer = this.shadow.querySelector(".search-container");
        const functionsList = this.shadow.querySelector("#functionsList");
        const otherContent = this.shadow.querySelector("#otherContent");
        const parametersList = this.shadow.querySelector("#parametersList");

        console.log('tabType: ', tabType)
        if (tabType === "columns") {
          this.showColumns();
          functionTypeSelect.style.display = "none";
          searchContainer.style.display = "flex";
          functionsList.style.display = "block";
          otherContent.style.display = "none";
          parametersList.style.display = "none";
        } else if (tabType === "functions") {
          this.showFunctions();
          functionTypeSelect.style.display = "block";
          searchContainer.style.display = "flex";
          functionsList.style.display = "block";
          otherContent.style.display = "none";
          parametersList.style.display = "none";
        } else if (tabType === "other") {
          searchContainer.style.display = "none";
          functionsList.style.display = "none";
          otherContent.style.display = "block";
          parametersList.style.display = "none";
        } else if (tabType === "parameters") {
          searchContainer.style.display = "none";
          functionsList.style.display = "none";
          otherContent.style.display = "none";
          parametersList.style.display = "block";
        }
      });
    });

    EventHandler.on(this.insertValuesBtn, "click", () => {
      if (this.insertValuesDialog) {
        this.insertValuesDialog.opened = true;
      }
    });

    EventHandler.on(this.validateBtn, "click", () => {
      if (!this.expressionArea.getTextContent().trim()) {
        this.errorMessage.style.display = "block";
      } else {
        this.errorMessage.style.display = "none";
      }
    });

    EventHandler.on(this.searchInput, 
      "input",
      this.handleFunctionSearch.bind(this)
    );

    this.shadow
      .querySelectorAll("#otherContent .smart-button")
      .forEach((button) => {
        EventHandler.on(button, "click", () => {
          this.insertExpression(button.textContent);
        });
      });
  }

  handleColumnSearch(e) {
    const searchTerm = e.target.value.toLowerCase();
    const columnItems = this.shadow.querySelectorAll(".function-item");

    columnItems.forEach((item) => {
      const text = item.textContent.toLowerCase();
      item.style.display = text.includes(searchTerm) ? "block" : "none";
    });
  }

  handleFunctionSearch(e) {
    const searchTerm = e.target.value.toLowerCase();
    const selectedType = this.functionTypeSelect.value;
    const functionItems = this.shadow.querySelectorAll(".function-item");

    functionItems.forEach((item) => {
      const text = item.textContent.toLowerCase();
      const functionType = item.getAttribute("data-type");

      const matchesSearch = text.includes(searchTerm);
      const matchesType =
        selectedType === "All Functions" || functionType === selectedType;

      item.style.display = matchesSearch && matchesType ? "block" : "none";
    });
  }

  populateFunctions() {
    this.functionsList.innerHTML = "";

    this.functions.forEach((func) => {
      const functionItem = document.createElement("div");
      functionItem.className = "function-item";
      functionItem.setAttribute("data-type", func.type);
      functionItem.innerHTML = `
        <div class="function-name">${func.name} Function (${func.type})</div>
        <div class="function-desc">${func.description}</div>
      `;
      this.functionsList.appendChild(functionItem);

      EventHandler.on(functionItem, "click", () => {
        this.insertFunction(func.name);
      });
    });
  }

  showColumns() {
    this.functionsList.innerHTML = "";
    console.log(this.selectedRectangle);
    if (!this.selectedRectangle) return;

    console.log("showColumns");
    const columns = this.selectedRectangle.items.filter(
      (item) => item.column_type === "column" || item.column_type === "measure"
    );

    console.log(columns);

    columns.forEach((column) => {
      const columnItem = document.createElement("div");
      columnItem.className = "function-item";
      columnItem.innerHTML = `
        <div class="function-name">${column.business_name}</div>
        <div class="function-desc">${column.technical_name}</div>
      `;
      this.functionsList.appendChild(columnItem);

      EventHandler.on(columnItem, "click", () => {
        this.insertColumn(column.business_name);
      });
    });

    this.searchInput.removeEventListener("input", this.handleFunctionSearch);
    EventHandler.on(this.searchInput, 
      "input",
      this.handleColumnSearch.bind(this)
    );
  }

  showFunctions() {
    this.populateFunctions();
    this.searchInput.removeEventListener("input", this.handleColumnSearch);
    EventHandler.on(this.searchInput, 
      "input",
      this.handleFunctionSearch.bind(this)
    );
  }

  insertFunction(functionName) {
    const currentText = this.expressionArea.getTextContent()
    this.expressionArea.setTextContent(currentText + (currentText ? " " : "") + functionName + "()");
    this.errorMessage.style.display = "none";
  }

  insertColumn(columnName) {
    const currentText = this.expressionArea.getTextContent()
    this.expressionArea.setTextContent(currentText + (currentText ? " " : "") + columnName);
    this.errorMessage.style.display = "none";
  }

  insertExpression(expression) {
    const currentText = this.expressionArea.getTextContent()
    this.expressionArea.setTextContent(currentText + (currentText ? " " : "") + expression);
    this.errorMessage.style.display = "none";
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.alias === window.data.selectedRectangle
    );
    this.updateColumnCount();
  }

  updateSelectedAssociation() {
    let selectedAssociation = null;

    window.data.associations.forEach((association) => {
      if (association.target1 === this.selectedRectangle.alias)
        selectedAssociation = association.target2;
      if (association.target2 === this.selectedRectangle.alias)
        selectedAssociation = association.target1;
    });

    if (selectedAssociation)
      this.selectedAssociation = window.data.rectangles.find(
        (rectangle) => rectangle.alias === selectedAssociation
      );
  }

  updateColumnCount() {
    const columnsTab = this.shadow.querySelector('[data-tab="columns"]');
    if (this.selectedRectangle && columnsTab) {
      const columnCount = this.selectedRectangle.items.filter(
        (item) =>
          item.column_type === "column" || item.column_type === "measure"
      ).length;
      columnsTab.textContent = `Columns (${columnCount})`;
    }
  }

  updateInitialValues() {
    const association = window.data.associations.find((association) => {
      return (
        (association.target1 === this.selectedRectangle.alias &&
          association.target2 === this.selectedAssociation.alias) ||
        (association.target2 === this.selectedRectangle.alias &&
          association.target1 === this.selectedAssociation.alias)
      );
    });

    const { name, expression } = association.filter || {
      name: "",
      expression: "",
    };

    // this.filterName.value = name;
    this.expressionArea.setTextContent(expression);
  }

  initializeDialog() {
    const dialogContent = document.createElement("div");
    dialogContent.innerHTML = `
      <style>
        .values-list {
            margin-top: 10px;
            max-height: 300px;
            overflow-y: auto;
          }
          .values-list-item {
            padding: 8px;
            display: flex;
            align-items: center;
            cursor: pointer;
          }
          .values-list-item:hover {
            background-color: #f5f5f5;
          }
          .values-list-item input[type="checkbox"] {
            margin-right: 10px;
          }
          .dialog-footer {
            position: absolute;
            bottom: 0;
            right: 0;
            display: flex;
            justify-content: flex-end;
            gap: 10px;
            padding: 16px;
            background-color: #f5f5f5;
            width: 100%;
            box-sizing: border-box;
            border-top: 1px solid #ddd;
          }
          .dialog-content {
            padding: 20px;
            padding-bottom: 70px; /* Make room for footer */
            height: 100%;
            box-sizing: border-box;
            overflow-y: auto;
          }
          .selected-values {
            margin-top: 20px;
            margin-bottom: 20px;
            padding: 10px;
            background-color: #f5f5f5;
            border-radius: 4px;
          }
      </style>
      <div class="dialog-content">
        <smart-input id="searchValuesInput" placeholder="Search"></smart-input>
        <div class="values-list" id="valuesList">
          ${this.valuesList
            .map(
              (value) => `
            <div class="values-list-item">
              <input type="checkbox" value="${value}" id="${value}">
              <label for="${value}">${value}</label>
            </div>
          `
            )
            .join("")}
        </div>
        <div class="selected-values" id="selectedValues"></div>
      </div>
      <div class="dialog-footer">
        <smart-button id="cancelBtn">Cancel</smart-button>
        <smart-button id="insertBtn">Insert</smart-button>
      </div>
    `;

    this.insertValuesDialog.appendChild(dialogContent);

    const searchInput =
      this.insertValuesDialog.querySelector("#searchValuesInput");
    const valuesList = this.insertValuesDialog.querySelector("#valuesList");
    const insertBtn = this.insertValuesDialog.querySelector("#insertBtn");
    const cancelBtn = this.insertValuesDialog.querySelector("#cancelBtn");

   EventHandler.on(searchInput, "input", (e) => {
      const searchTerm = e.target.value.toLowerCase();
      const items = valuesList.querySelectorAll(".values-list-item");
      items.forEach((item) => {
        const text = item.textContent.toLowerCase();
        item.style.display = text.includes(searchTerm) ? "block" : "none";
      });
    });

    EventHandler.on(valuesList, "change", (e) => {
      if (e.target.type === "checkbox") {
        if (e.target.checked) {
          this.selectedValues.add(e.target.value);
        } else {
          this.selectedValues.delete(e.target.value);
        }
        this.updateSelectedValuesDisplay();
      }
    });

    EventHandler.on(insertBtn, "click", () => {
      this.insertSelectedValues();
      this.insertValuesDialog.opened = false;
    });

    EventHandler.on(cancelBtn, "click", () => {
      this.insertValuesDialog.opened = false;
    });
  }

  resetDialogState() {
    this.selectedValues.clear();

    const searchInput =
      this.insertValuesDialog.querySelector("#searchValuesInput");
    if (searchInput) {
      searchInput.value = "";
    }

    const checkboxes = this.insertValuesDialog.querySelectorAll(
      'input[type="checkbox"]'
    );
    checkboxes.forEach((checkbox) => {
      checkbox.checked = false;
    });

    const items = this.insertValuesDialog.querySelectorAll(".values-list-item");
    items.forEach((item) => {
      item.style.display = "block";
    });

    const selectedValuesDiv =
      this.insertValuesDialog.querySelector("#selectedValues");
    if (selectedValuesDiv) {
      selectedValuesDiv.textContent = "";
    }
  }

  updateSelectedValuesDisplay() {
    const selectedValuesDiv =
      this.insertValuesDialog.querySelector("#selectedValues");
    if (selectedValuesDiv) {
      selectedValuesDiv.textContent = Array.from(this.selectedValues).join(
        ", "
      );
    }
  }

  insertSelectedValues() {
    if (this.selectedValues.size > 0) {
      const values = Array.from(this.selectedValues);
      const currentText = this.expressionArea.getTextContent();
      this.expressionArea.setTextContent(currentText + (currentText ? " " : "") + values.join(", "));
    }
  }
}

customElements.define("expression-component", ExpressionComponent);
