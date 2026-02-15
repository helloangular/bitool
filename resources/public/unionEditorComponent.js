import "./source/modules/smart.listbox.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.tooltip.js";
import "./source/modules/smart.table.js";

import EventHandler from "./library/eventHandler.js";
import { createElement, request } from "./library/utils.js";
import SortableList from "./library/sortable.js";
import Modal from "./library/modal.js";
import StateManager from "./library/state-manager.js";

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
  .mapping-container {
    width: 100%;
    padding: 10px 0;
    overflow-x: scroll;
    scrollbar-gutter: stable;
    scrollbar-width: thin;
  }
  #tableZones {
    display:flex;
    gap:40px;
    position:relative;
    width:100%;
    margin-top:10px;
    user-select:none;
  }
  .sortable-item {
    cursor: grab;
  }
  .dragging {
    opacity: 0.5;
  }
  .placeholder-line {
    height: 0;
    border-top: 4px solid #007bff;
    margin: 2px 0;
  }
</style>

<div class="container">
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
      <smart-input class="outlined" name="alias" style="width:100%" required></smart-input>
    </div>
    <div style="margin-bottom:10px;">
      <label>Union Type:</label>
      <div class="flex">
        <select class="dropdown" name="jtype">
          <option value="union">Union</option>
          <option value="all">Union All</option>
        </select>
      </div>
      <label>Default Table: </label>
      <select id="defaultTable" name="defaultTable" class="dropdown"></select>
    </div
  </div>
  <span style="font-size:16px;">Mappings:</span>
  <div class="mapping-container">
    <div id="tableZones">
      <!-- <svg style="position:absolute;width:inherit;overflow:visible;"></svg> -->
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
    this.modal = new Modal({
      title: "Add Alias",
      content: `
        <form style="gap: 10px;">
          <label for="business_name">Business Name:</label><br/>
          <input type="text" id="business_name" name="business_name" placeholder="Enter business name" style="padding: 5px 10px;margin-bottom:10px;" disabled /><br />
          <label for="alias">Alias:</label><br/>
          <input type="text" id="alias" name="alias" placeholder="Enter alias" style="padding: 5px 10px;margin-bottom:10px;" /><br/>
          <input type="submit" value="Add Alias" />
        </form>
      `
    })
    this.selectedRectangle = null;
    this.selectedTable = null;
    this.state = null;

    this.close();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue == "open") {
        this.open();
        this.updateSelectedRectangle();
        this.resetFields();
        setTimeout(() => {
          this.renderContent();
          this.updateFields();
        }, 100);
        this.setUpEventListeners();
      } else {
        this.close();
        if (this.style.visibility === "hidden") {
          EventHandler.removeGroup("Union");
        }
      }
    }
  }

  cacheElements() {
    this.generalForm = this.shadowRoot.querySelector("#generalForm");
    this.closeButton = this.shadowRoot.querySelector(".smart-button");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.tableZones = this.shadowRoot.querySelector("#tableZones");
    this.defaultTable = this.shadowRoot.querySelector("#defaultTable");
    this.transformEditor = document.querySelector("transform-editor");
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "Union");

    EventHandler.on(this.modal.overlay.querySelector('form'), "submit", (e) => {
      e.preventDefault();
      this.addAlias({
        table: this.selectedTable,
        businessName: e.target.business_name.value,
        alias: e.target.alias.value
      });
    }, false, "Union");

    EventHandler.on(this.saveButton, "click", this.save.bind(this), false, "Union");

    EventHandler.on(this.transformEditor, "transform-data", (e) => {
      console.log(e.detail)
      const index = this.selectedRectangle.items[this.selectedTable].findIndex(t => t.business_name === e.detail.businessName);
      if (index != -1) {
        this.selectedRectangle.items[this.selectedTable][index].transform = e.detail.transformations;
      }
      this.state.updateField("items", this.selectedRectangle.items);
      this.setDirty();
    }, false, "Union");

    EventHandler.on(this.shadowRoot, "click", (e) => {
      this.closeItemDropdowns(e.target.business_name);
    });

    EventHandler.on(window, "sortablelist:changed", () => {
      const sortItems = {};
      Object.keys(this.selectedRectangle.items).forEach((key) => sortItems[key] = []);
      Object.keys(sortItems).forEach((key) => {
        const cwc = this.tableZones.querySelector(`#${key}`).querySelectorAll("custom-web-component");
        cwc.forEach((c) => {
          sortItems[key].push(this.selectedRectangle.items[key].find((i) => i.business_name === c.getAttribute("business_name")));
        })
      });
      this.state.updateField("items", sortItems);
      this.setDirty();
    }, false, "Union");

    this.trackInputs();
  }

  updateFields() {
    this.shadowRoot.querySelector("smart-input[name='alias']").value = this.selectedRectangle.alias;
    this.shadowRoot.querySelector("select[name='jtype']").selectedIndex = this.selectedRectangle.jtype === "all" ? 1 : 0;
    this.state = new StateManager(this.selectedRectangle);
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
        // this.closeItemDropdowns();
      }
      return; // prevent closing
    }
    this.style.visibility = "hidden";
    // this.closeItemDropdowns();
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("input, select, smart-input");
    inputs.forEach(el => {
      if (el.tagName !== "SELECT") EventHandler.on(el, "input", (e) => {
        this.state.updateField(e.target.name, e.target.value);
        this.setDirty()
      }, false, "Union");
      if (el.tagName === "SELECT") EventHandler.on(el, "change", (e) => {
        this.state.updateField(e.target.name, e.target.value);
        this.setDirty()
      }, false, "Union");
    });
  }

  setDirty() {
    this.saveButton.disabled = !this.state.isDirty();
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

    const values = this.getValues();
    console.log(values);

    request("/saveUnion", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      this.state.commit();
      this.reset();
      this.setAttribute("visibility", "close");
    }).catch((error) => console.error("Error: ", error));
  }

  getValues() {
    return {
      id: this.selectedRectangle.id,
      ...this.state.updatedFields,
      // alias: this.generalForm.querySelector("smart-input").value,
      // business_name: this.generalForm.querySelector("smart-input").value,
      // technical_name: this.generalForm.querySelector("smart-input").value,
      // jtype: select.querySelector("option[value=" + select.value + "]").innerText,
      // items: this.selectedRectangle.items,
      // join_items: sortItems,
      // columnAlias: this.columnAlias,
      // default_table: this.shadowRoot.querySelector("#defaultTable").value
    }
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

  removeLines() {
    const lines = this.svg.querySelectorAll("line");
    lines.forEach((line) => {
      if (line.id === "line") return;

      line.remove();
    });
  }

  renderContent() {
    this.clearContent(this.tableZones, "div");
    this.clearContent(this.defaultTable, "option");
    this.selectedRectangle.tables.forEach((table) => {
      const option = createElement("option", {
        text: table,
        attrs: { value: table }
      });
      this.defaultTable.appendChild(option);
    })
    Object.keys(this.selectedRectangle.items).forEach((table) => {
      const tableHeading = document.createElement("div");
      tableHeading.innerHTML = `<h4 style="margin: 0 0 10px 0;">${table}</h4>`;

      const tableZone = document.createElement("div");
      tableZone.style.display = "flex";
      tableZone.style.flexDirection = "column";
      tableZone.style.width = "100%";
      tableZone.setAttribute("id", table);
      tableZone.appendChild(tableHeading);
      this.tableZones.appendChild(tableZone);

      this.selectedRectangle.items[table].forEach((item) => {
        const component = this.createComponent(item, this.selectedRectangle.btype);
        component.classList.add("sortable-item");
        component.shadowRoot.querySelector(".tooltip-item").remove();
        tableZone.appendChild(component);

        const dropdownTooltip = component.shadowRoot;
        EventHandler.on(dropdownTooltip, "click", (e) => {
          if (e.target.closest('.tooltip-item')) {
            this.handleDropdownItemClick({
              table: table,
              itemText: e.target.textContent,
              item
            })
          }
        });
      });
      new SortableList(tableZone, "Union");
    });
  }

  clearContent(container, component) {
    const content = container.querySelectorAll(component);
    content.forEach((item) => {
      item.remove();
    });
  }

  closeItemDropdowns(business_name) {
    this.tableZones.querySelectorAll("custom-web-component").forEach((component) => {
      if (component.business_name !== business_name) {
        const dropdown = component.shadowRoot.querySelector("#dropDowntooltip");
        dropdown.close();
      }
    });
  }

  resetFields() {
    this.shadowRoot.querySelector("smart-input").value = "";
    this.shadowRoot.querySelector("select").selectedIndex = 0;
    this.clearContent(this.tableZones, "div");
    this.clearContent(this.defaultTable, "option");
  }

  createComponent(item, btype) {
    const component = document.createElement("custom-web-component");
    component.setAttribute("id", item.tid);
    component.setAttribute("alias", item.alias || "");
    component.setAttribute("data_type", item.data_type || "");
    component.setAttribute("business_name", item.business_name || "");
    component.setAttribute("technical_name", item.technical_name || "");
    component.setAttribute("key", item.key || "");
    component.setAttribute("semantic_type", item.semantic_type || "");
    component.setAttribute("parent_object", btype || "");
    component.setAttribute("info", "no");
    component.setAttribute("padding", "8px");

    return component;
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id == window.data.selectedRectangle
    );
  }

  addAlias({ table, businessName, alias }) {
    const item = this.selectedRectangle.items[table].find(
      (item) => item.business_name === businessName
    );

    if (!item) return;

    // item.business_name = businessName;
    item["alias"] = alias;

    const index = this.selectedRectangle.items[table].findIndex(c => c.business_name === businessName);
    if (index == -1) {
      this.selectedRectangle.items[table][index].alias = alias;
    }
    this.state.updateField("items", this.selectedRectangle.items);
    this.setDirty();
    this.modal.close();
    this.renderContent();
  }

  handleDropdownItemClick({ table, itemText, item }) {
    this.selectedTable = table;
    if (itemText === "Add Alias") {
      this.modal.open();
      this.modal.overlay.querySelector('input[name="business_name"]').value = item.business_name;
      this.modal.overlay.querySelector('input[name="alias"]').value = item.alias || "";
    } else if (itemText === "Transform") {
      this.transformEditor.setAttribute("visibility", "open");
      this.transformEditor.setAttribute("item", JSON.stringify(item));
    }
  }
}

customElements.define(
  "union-editor-component",
  UnionEditorComponent
);
