import "./source/modules/smart.listbox.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.tooltip.js";
import "./source/modules/smart.table.js";

import EventHandler from "./library/eventHandler.js";
import { getRelativeBounds } from "./utils.js";
import { createElement, FollowUpLine } from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .container {
    display: flex;
    flex-direction: column;
    padding: 20px;
    height: 100%;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
    background-color: white;
  }
  .smart-button{
    --smart-button-padding: 4px;
    --smart-border-width: 0;
    --smart-font-size: 20px;
    --smart-ui-state-hover: white;
  }
  .flex {
    display:flex;
    flex-direction: row;
    justify-content:space-between;
  }
  .dropdown {
    width: 50%;
    padding: 12px 5px;
    border-radius: 5px;
    background: #fefefe;
    border: 1px solid #e0e0e0;
    margin: 5px 0px;
  }
</style>

<div class="container" style>
  <div class="flex">
    <h3>Union</h3>
    <smart-button content="&#9747;" class="smart-button" style="margin-right:10px;"></smart-button>
  </div>
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;padding:6px;"></smart-button>
  </div>
  <div style="padding:10px" id="generalForm">
    <div style="margin-bottom:10px;">
      <label>Name*:</label>
      <smart-input class="outlined" name="name" style="width:100%" required></smart-input>
    </div>
    <div style="margin-bottom:10px;">
      <label>Union Type:</label>
      <div class="flex">
        <select class="dropdown">
          <option value="v01">Inner</option>
          <option value="v02">Outer</option>
          <option value="v03">Other</option>
        </select>
        <input type="checkbox" name="distinct" />
        <label>Distinct Values</label>
      </div>
      <label>Default Table: </label>
      <select id="defaultTable" class="dropdown"></select>
    </div
  </div>
  <div style="width:100%;margin-top:10px;overflow:scroll;">
    <span style="font-size:16px;">Mappings</span>
    <div id="tableHeadings" style="display:flex;gap:40px;position:relative;width:100%;margin-top:5px"></div>
    <div id="tableZones" style="display:flex;gap:40px;position:relative;width:100%;margin-top:10px;user-select:none;">
      <svg style="position:absolute;width:inherit;"></svg>
    </div>
  </div>
</div>
`;

class UnionEditorComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.shadowRoot.append(template.content.cloneNode(true));
    this.cacheElements();
    this.joinItems = {};
    this.excludedColumns = [];
    this.renamedColumns = new Map();
    this.selectedRectangle = null;
    this.isDragging = false;
    this.startDiv = null;
    this.selectedTable = null;
    this.dirty = false;

    this.updateVisibility();
    this.setUpEventListeners();
    // this.updateSelectedRectangle();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue == "open") {
        this.reset();
        this.updateSelectedRectangle();
        setTimeout(() => {
          this.renderContent();
        }, 100);
      } else {
        this.removeLines();
      }
      this.updateVisibility();
    }
  }

  cacheElements() {
    this.generalForm = this.shadowRoot.querySelector("#generalForm");
    this.closeButton = this.shadowRoot.querySelector(".smart-button");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.tableHeadings = this.shadowRoot.querySelector("#tableHeadings");
    this.tableZones = this.shadowRoot.querySelector("#tableZones");
    this.svg = this.shadowRoot.querySelector("svg");
    this.followUp = new FollowUpLine(this.svg)
    this.renameDialog = document.querySelector("rename-dialog");
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", this.close.bind(this));

    EventHandler.on(this.renameDialog, "rename", (e) => {
      if (this.getAttribute("visibility") == "open") this.handleRename(e.detail);
      this.isSaved = false;
    });

    EventHandler.on(this.saveButton, "click", this.save.bind(this));

    let parentBounds = null;
    EventHandler.on(this.tableZones, "mousemove", (e) => {
      if (!parentBounds) parentBounds = this.tableZones.getBoundingClientRect();
      if (this.isDragging) {
        this.followUp.preview(e);
      }
    });

    EventHandler.on(this.tableZones, "mouseup", (e) => this.handleMouseUp(e));

    this.trackInputs();
  }

  reset() {
    this.dirty = false;
    this.saveButton.disabled = true;
  }

  close() {
    if (this.dirty) {
      alert("You must save your changes before closing.");
      return; // prevent closing
    }
    this.setAttribute("visibility", "");
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("input, select, smart-input");
    inputs.forEach(el => {
      el.addEventListener("input", () => this.setDirty());
      el.addEventListener("change", () => this.setDirty());
    });
  }

  setDirty() {
    this.dirty = true;
    this.saveButton.disabled = false;
  }

  handleMouseUp(e) {
    if (this.isDragging) {
      this.isDragging = false;

      const targetBox = this.shadowRoot.elementFromPoint(e.clientX, e.clientY);
      const parent = targetBox.parentElement;
      const tableId = parent.getAttribute("id");

      if (
        targetBox &&
        targetBox.tagName.toLowerCase() === "custom-web-component" &&
        targetBox !== this.startDiv
      ) {
        if (tableId) {
          this.joinItems[tableId].push(targetBox.getAttribute("technical_name"));

          // Maintain the same length of joinItems for all tables.
          // To avoid wrong joinings.
          Object.keys(this.joinItems).forEach((key) => {
            if (this.joinItems[key].length < this.joinItems[tableId].length) {
              this.joinItems[key].push("");
            }
          });
        }

        if (this.startDiv.getAttribute("data_type") !== targetBox.getAttribute("data_type")) {
          alert(`Type ${this.startDiv.getAttribute("data_type")} is not allowed to connect with ${targetBox.getAttribute("data_type")}`);
          this.followUp.end();
          return;
        }

        const newLine = document.createElementNS(
          "http://www.w3.org/2000/svg",
          "line"
        );

        const fromX = this.followUp.previewLine.getAttribute("x1");
        const fromY = this.followUp.previewLine.getAttribute("y1");
        const { x, y } = this.followUp.end(targetBox, e);

        newLine.setAttribute("x1", fromX);
        newLine.setAttribute("y1", fromY);
        newLine.setAttribute("x2", x);
        newLine.setAttribute("y2", y);
        newLine.setAttribute("stroke", "lightgray");
        newLine.setAttribute("stroke-width", 2);

        this.svg.appendChild(newLine);
        this.setDirty();
      }

      // If line is not drop on another table column it should not add that join data.
      if (targetBox == this.startDiv) {
        Object.keys(this.joinItems).forEach((key) => {
          if (this.joinItems[tableId].length > this.joinItems[key].length) {
            this.joinItems[tableId].pop();
          }
        });
      }
    }
  }

  isInputValidate() {
    if (this.generalForm.querySelector("smart-input").value == "") {
      alert("Name is required");
      return false
    }
    return true
  }

  save() {
    if (!this.isInputValidate()) return;
    if (!this.checkIfColumnCountIsSame(this.selectedRectangle.items)) {
      alert("All columns are must be of same count.");
      return;
    }

    const select = this.generalForm.querySelector("select");
    const dataObj = {
      id: this.selectedRectangle.id,
      alias: this.generalForm.querySelector("smart-input").value,
      business_name: this.generalForm.querySelector("smart-input").value,
      technical_name: this.generalForm.querySelector("smart-input").value,
      jtype: select.querySelector("option[value=" + select.value + "]").innerText,
      distinct: this.generalForm.querySelector("input[name=distinct]").checked,
      items: this.selectedRectangle.items,
      join_items: this.joinItems,
      excludedColumns: this.excludedColumns,
      renamedColumns: Array.from(this.renamedColumns.values()),
      default_table: this.shadowRoot.querySelector("#defaultTable").value
    }

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
        this.reset();
      }).catch((error) => {
        alert("Error: " + error);
        console.error("Error:", error);
      });
  }

  checkIfColumnCountIsSame(column) {
    let index = 0, count = 0, columnContents = 0;
    for (const key in column) {
      columnContents = column[key].length;
      for (const data of column[key]) {
        if (this.excludedColumns.some(excludedItem => excludedItem.business_name === data.business_name)) {
          columnContents--;
        }
      }
      if (index == 0) count = columnContents;
      if (count !== columnContents) return false;
      index++;
    }
    return true
  }

  updateVisibility() {
    this.style.display = this.getAttribute("visibility") == "open" ? "block" : "none";
  }

  removeLines() {
    const lines = this.svg.querySelectorAll("line");
    lines.forEach((line) => {
      if (line.id === "line") return;

      line.remove();
    });
  }

  renderContent() {
    this.tableHeadings.innerHTML = "";
    this.clearContent(this.tableZones, "div");

    this.selectedRectangle.tables.forEach((table) => {
      const option = createElement("option", {
        text: table,
        attrs: {value: table}
      });
      this.shadowRoot.querySelector("#defaultTable").appendChild(option);
    })
    Object.keys(this.selectedRectangle.items).forEach((table) => {
      const tableHeading = document.createElement("div");
      tableHeading.innerHTML = table;
      this.tableHeadings.appendChild(tableHeading);

      const tableZone = document.createElement("div");
      tableZone.style.display = "flex";
      tableZone.style.flexDirection = "column";
      tableZone.style.width = "100%";
      tableZone.setAttribute("id", table);
      this.tableZones.appendChild(tableZone);

      this.joinItems[table] = [];

      this.selectedRectangle.items[table].forEach((item) => {
        const component = this.createComponent(item, table, this.selectedRectangle.btype);
        if (table.toLowerCase() !== "final") {
          component.dropDowntooltip.querySelectorAll(".tooltip-item")[1].remove();
        }
        tableZone.appendChild(component);

        const dropdownTooltip = component.shadowRoot.querySelector("#dropDowntooltip");
        if (dropdownTooltip) {
          EventHandler.on(dropdownTooltip, "click", (e) => {
            const tooltipItem = e.target.closest(".tooltip-item");
            if (tooltipItem) {
              this.handleDropdownItemClick({
                table: table,
                itemText: tooltipItem.textContent,
                businessName: item.business_name
              })
            }
          });
        }
      });
    });

    this.makeContentDraggable(this.tableZones.querySelector("#Final"));

    const components = this.shadowRoot.querySelectorAll("custom-web-component:not([draggable=true])");
    components.forEach((box) => {
      const parent = box.parentElement;

      EventHandler.on(box, "mousedown", (e) => {
        this.isDragging = true;
        this.startDiv = box;

        const tableId = parent.getAttribute("id");
        if (tableId) {
          this.joinItems[tableId].push(this.startDiv.getAttribute("technical_name"));
        }

        this.followUp.start(this.startDiv, e);
      });
    });

    setTimeout(() => {
      this.drawExistingLines();
    }, 100);
  }

  clearContent(container, component) {
    const content = container.querySelectorAll(component);
    content.forEach((item) => {
      item.remove();
    });
  }

  createComponent(item, table, btype) {
    const component = document.createElement("custom-web-component");
    component.setAttribute("id", item.tid);
    component.setAttribute("data_type", item.data_type);
    component.setAttribute("business_name", item.business_name);
    component.setAttribute("technical_name", item.technical_name);
    component.setAttribute("key", item.key || "");
    component.setAttribute("semantic_type", item.semantic_type || "");
    component.setAttribute("parent_object", btype);
    // component.setAttribute("parent_object", "Projection");
    if (item.excluded) {
      component.setAttribute("excluded", item.excluded === "YES" ? true : false);
    } else {
      const isExcluded = this.excludedColumns.some(excludedItem => excludedItem.business_name === item.business_name);
      component.setAttribute("excluded", isExcluded);
    }
    component.setAttribute("info", "no");
    component.setAttribute("padding", "4px");
    if (table.toLowerCase() == "final") component.setAttribute("draggable", true);
    return component;
  }

  makeContentDraggable(parent) {
    parent.childNodes.forEach((col) => {
      // const dragBtn = col.shadow.getElementById("dragBtn");
      EventHandler.on(col, "dragstart", this.handleDragStart);
      EventHandler.on(col, "dragend", this.handleDragEnd);
    });

    EventHandler.on(parent, "dragover", this.handleDragOver.bind(this));
  }

  handleDragStart(event) {
    event.target.classList.add("dragging");
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", event.target.textContent);
  }

  handleDragEnd(event) {
    event.target.classList.remove("dragging");
  }

  handleDragOver(event) {
    event.preventDefault();
    const draggingEl = this.tableZones.querySelector("#Final").querySelector(".dragging");
    const list = event.currentTarget;
    const afterElement = this.getDragAfterElement(list, event.clientY);

    if (afterElement) {
      list.insertBefore(draggingEl, afterElement);
    } else {
      list.appendChild(draggingEl);
    }
  }

  getDragAfterElement(container, y) {
    const draggableElements = [...container.querySelectorAll("custom-web-component:not(.dragging)")];

    return draggableElements.reduce((closest, child) => {
      const box = child.getBoundingClientRect();
      const offset = y - box.top - box.height / 2;

      if (offset < 0 && offset > closest.offset) {
        return { offset, element: child };
      } else {
        return closest;
      }
    }, { offset: Number.NEGATIVE_INFINITY }).element;
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id == window.data.selectedRectangle
    );
    // fetch("getItem?id=8", {
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

  drawExistingLines() {
    if (!this.selectedRectangle) return;
    if (!this.selectedRectangle.join_items || this.selectedRectangle.join_items.length == 0) return;
    for (const key in this.selectedRectangle.join_items) {
      this.joinItems[key] = this.selectedRectangle.join_items[key];
    }

    const componentCouples = Array.from({ length: Object.keys(this.selectedRectangle.join_items).length }, () => []);

    Object.keys(this.selectedRectangle.join_items).forEach((key, index) => {
      const zone = this.tableZones.querySelector("#" + key);
      const components = Array.from(zone.querySelectorAll("custom-web-component"));
      componentCouples[index] = this.selectedRectangle.join_items[key].map((columnName) => {
        return components.find(
          (component) => component.getAttribute("business_name") == columnName
        )
      });
    });

    let columnLength = componentCouples.length - 1;
    let columnItemLength = componentCouples[0].length;
    for (let j = 0; j < columnLength; j++) {
      for (let i = 0; i < columnItemLength; i++) {
        // const fromRect = componentCouples[j][i].getBoundingClientRect();
        // const toRect = componentCouples[j + 1][i].getBoundingClientRect();
        const fromRect = getRelativeBounds(this.tableZones, componentCouples[j][i]);
        const toRect = getRelativeBounds(this.tableZones, componentCouples[j + 1][i]);
        const newLine = document.createElementNS(
          "http://www.w3.org/2000/svg",
          "line"
        );

        newLine.setAttribute("x1", fromRect.right);
        newLine.setAttribute("y1", fromRect.top + fromRect.height / 2);
        newLine.setAttribute("x2", toRect.left);
        newLine.setAttribute("y2", toRect.top + toRect.height / 2);
        newLine.setAttribute("stroke", "lightgray");
        newLine.setAttribute("stroke-width", 2);
        this.svg.appendChild(newLine);
      }
    }
  }

  handleRename({
    oldBusinessName,
    oldTechnicalName,
    newBusinessName,
    newTechnicalName
  }) {
    const item = this.selectedRectangle.items[this.selectedTable].find(
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
      this.renderContent();
    }
  }

  handleDropdownItemClick({ table, itemText, businessName }) {
    this.selectedTable = table;
    if (itemText === "Exclude Column") {
      this.handleExcludeColumn(businessName);
    } else if (itemText === "Include Column") {
      this.handleIncludeColumn(businessName);
    } else if (itemText === "Change Name") {
      this.handleChangeNameClick(businessName);
    }
  }

  handleChangeNameClick(businessName) {
    const item = this.selectedRectangle.items[this.selectedTable].find(
      (item) => item.business_name === businessName
    );
    if (item) {
      this.renameDialog.setAttribute("business-name", item.business_name);
      this.renameDialog.setAttribute("technical-name", item.technical_name);
      this.renameDialog.setAttribute("visibility", "open");
    }
  }

  handleExcludeColumn(businessName) {
    const item = this.selectedRectangle.items[this.selectedTable].find(item => item.business_name === businessName);
    if (item) {
      this.excludedColumns.push({ tid: item.tid, technical_name: item.technical_name, business_name: item.business_name });
      this.tableZones.querySelectorAll("custom-web-component").forEach((component) => {
        if (component.getAttribute("business_name") === businessName) {
          component.setAttribute("excluded", true);
        }
      });
    }
  }

  handleIncludeColumn(businessName) {
    this.excludedColumns = this.excludedColumns.filter(excludedItem => excludedItem.business_name !== businessName);
    this.tableZones.querySelectorAll("custom-web-component").forEach((component) => {
      if (component.getAttribute("business_name") === businessName) {
        component.setAttribute("excluded", false);
      }
    });
  }
}

customElements.define(
  "union-editor-component",
  UnionEditorComponent
);
