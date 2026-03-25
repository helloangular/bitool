import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";
import "./projectionComponent.js";
import { renderContentOfEditDialog } from "./renderContentOfEditDialog.js";
import { setEventListenersToDropdownItems } from "./setEventListenersToDropdownItems.js";
import "./source/modules/smart.dropdownlist.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.listbox.js";
import "./source/modules/smart.table.js";
import "./source/modules/smart.tooltip.js";
import { updateDropdownTooltips } from "./updateDropdownTooltips.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  :host {
    visibility: hidden;
    background: white;
    overflow: auto;
  }
  .columnlist-container {
    display: flex;
    flex-direction: column;
    align-items: stretch;
    gap: 10px;
    height: 100%;
    box-sizing: border-box;
    padding: 24px 10px 10px;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
  }
  .smart-button{
    --smart-button-padding: 4px;
    --smart-border-width: 0;
    --smart-font-size: 20px;
    --smart-ui-state-hover: white;
  }
  ul {
    margin: 0;
    padding: 0;
  }
  li {
    list-style: none;
    width: 100%;
  }
  .cl-actions {
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    position: sticky;
    top: 0;
    z-index: 2;
    padding: 0;
    background: white;
  }
</style>

<div class="columnlist-container">
  <div class="cl-actions">
    <div style="display:flex;">
      <smart-button content="Save" id="saveButton" class="smart-button" style="font-size:10px;" disabled></smart-button>
      <smart-button content="filter" id="filterButton" class="smart-button" style="font-size:10px;"></smart-button>
      <smart-button content="projection" id="projectionButton" class="smart-button" style="font-size:10px;"></smart-button>
    </div>
    <smart-button id="closeX" content="&#9747;" class="smart-button" aria-label="Close"></smart-button>
  </div>
  <div id="generalForm">
    <label> 
      Business name: 
    </label>
    <smart-input class="outlined" name="business_name" style="width:100%"></smart-input>
    <label>
      Technical name: 
    </label>
    <smart-input class="outlined" name="technical_name" style="width:100%"></smart-input>
    <label>
      Alias:
    </label>
    <smart-input class="outlined" name="alias" style="width:100%"></smart-input>
  </div>
  <div style="display:flex;align-items:center;width:100%;">
    <smart-input id="searchInput" style="width:100%;height:40px" placeholder="Search"></smart-input>
    <smart-button content="&#8645;" class="smart-button" style="margin-left:10px;margin-right:10px;display:none;" id="sortButton"></smart-button>
  </div>
  <div id="settingsMenu" style="height:100%;width:100%;overflow-y:auto;"></div>
  <!-- <smart-list-box id="settingsMenu" selection-mode="none" style="height: 100%; width: 100%" ></smart-list-box> -->
</div>
`;

class ColumnListComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
  }

  connectedCallback() {
    this.shadowRoot.append(template.content.cloneNode(true));

    this.listBox = this.shadowRoot.querySelector("#settingsMenu");
    this.closeButton = this.shadowRoot.querySelector("#closeX");
    this.searchInput = this.shadowRoot.querySelector("#searchInput");
    this.sortButton = this.shadowRoot.querySelector("#sortButton");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.textAssociationDialog = document.querySelector(".association-dialog");
    this.columnEditDialog = document.querySelector(".column-edit-dialog");
    this.filterButton = this.shadowRoot.querySelector("#filterButton");
    this.projectionButton = this.shadowRoot.querySelector("#projectionButton");
    this.projectionComponent = document.querySelector("projection-component");
    this.transformEditor = document.querySelector("transform-editor");
    this.renameDialog = document.querySelector("rename-dialog");
    this.selectedRectangle = null;
    this.state = null;
    this.close();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue === "open") {
        this.open();
        this.setUpEventListeners();
        this.updateSelectedRectangle();
        if (!this.selectedRectangle) {
          console.warn("ColumnListComponent: no rectangle selected, closing.");
          this.close();
          return;
        }
        this.updateFields();
        this.renderContent();
      } else {
        this.close();
        if (this.style.visibility === "hidden") {
          this.resetFields();
          EventHandler.removeGroup("ColumnListComponent");
        }
      }

      if (this.selectedRectangle?.parent_object === "Table_view")
        this.sortButton.style.display = "block";
      else this.sortButton.style.display = "none";
    }
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "ColumnListComponent");
    EventHandler.on(this.saveButton, "click", this.save.bind(this), false, "ColumnListComponent");
    EventHandler.on(this.searchInput, "changing", (e) => this.handleSearchInputChange(e), false, "ColumnListComponent");
    EventHandler.on(this.filterButton, "click", this.handleFilterButtonClick.bind(this), false, "ColumnListComponent");
    EventHandler.on(this.projectionButton, "click", this.handleProjectionButtonClick.bind(this), false, "ColumnListComponent");
    EventHandler.on(this.sortButton, "click", this.handleSortButtonClick.bind(this), false, "ColumnListComponent");
    EventHandler.on(this.shadowRoot, "click", (e) => this.closeItemDropdowns(e.target.business_name), false, "ColumnListComponent");
    EventHandler.on(this.transformEditor, "transform-data", (e) => {
      this.selectedRectangle.items.map(item => {
        if (item.business_name === e.detail.businessName) item.transform = e.detail.transformations;
      });
      this.state.updateField("items", this.selectedRectangle.items);
      this.setDirty();

      // Setting column is transformed or transform removed.
      this.listBox.querySelectorAll("custom-web-component").forEach((component) => {
        if (component.getAttribute("business_name") === e.detail.businessName) {
          if (e.detail.transformations.length > 0) {
            component.setAttribute("transform", "YES");
          } else {
            component.setAttribute("transform", "NO");
          }
        }
      });
    }, false, "ColumnListComponent");
    EventHandler.on(this.renameDialog, "rename", (e) => {
      if (this.getAttribute("visibility") == "open") this.addAlias(e.detail);
    }, false, "ColumnListComponent");
    this.trackInputs();
  }

  updateFields() {
    this.shadowRoot.querySelector("smart-input[name='business_name']").value = this.selectedRectangle.business_name;
    this.shadowRoot.querySelector("smart-input[name='technical_name']").value = this.selectedRectangle.technical_name;
    this.shadowRoot.querySelector("smart-input[name='alias']").value = this.selectedRectangle.alias;
    this.searchInput.value = "";
    this.state = new StateManager(this.selectedRectangle);
  }

  resetFields() {
    this.shadowRoot.querySelector("smart-input[name='business_name']").value = "";
    this.shadowRoot.querySelector("smart-input[name='technical_name']").value = "";
    this.shadowRoot.querySelector("smart-input[name='alias']").value = "";
    this.searchInput.value = "";
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
    request("/saveColumnList", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      this.state.commit();
      this.reset();
    })
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("smart-input");
    Array.from(inputs).pop(); // remove search input
    inputs.forEach(el => {
      EventHandler.on(el, "input", (e) => {
        this.state.updateField(e.target.name, e.target.value);
        this.setDirty()
      }, false, "ColumnListComponent");
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

  handleFilterButtonClick() {
    const filterComponent = document.querySelector("filter-component");
    if (filterComponent) {
      this.setAttribute("visibility", "");
      filterComponent.setAttribute("visibility", "open");
    }
  }

  handleProjectionButtonClick() {
    if (this.projectionComponent) {
      this.setAttribute("visibility", "");
      this.projectionComponent.setAttribute("visibility", "open");
    }
  }

  handleSearchInputChange(e) {
    const searchQuery = e.detail.value;
    const items = this.listBox.querySelectorAll("li");
    Array.from(items).forEach((item) => {
      if (item.textContent.toLowerCase().includes(searchQuery.toLowerCase())) {
        item.style.display = "block";
        return;
      }

      item.style.display = "none";
    });
  }

  handleSortButtonClick() {
    window.data.rectangles = window.data.rectangles.map((rect) => {
      if (rect.alias === this.selectedRectangle.alias) {
        if (rect.sorted_by && rect.sorted_by === "desc")
          return { ...rect, sorted_by: "asc" };

        return { ...rect, sorted_by: "desc" };
      }
      return rect;
    });

    this.updateSelectedRectangle();
    this.renderContent();
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );
  }

  renderContent() {
    this.clearContent(this.listBox); 

    const groups = new Map();
    let hasAnyAssociation = false;

    const sortedBy = this.selectedRectangle.sorted_by || "asc";

    const columnItems = Array.from(this.selectedRectangle.items).filter(
      (item) => item.column_type === "column"
    );
    const nonColumnItems = Array.from(this.selectedRectangle.items).filter(
      (item) => item.column_type != "column"
    );

    const sortedColumnItems = columnItems.sort((a, b) => {
      if (sortedBy === "desc")
        return a.business_name < b.business_name ? 1 : -1;

      return a.business_name > b.business_name ? 1 : -1;
    });

    [...sortedColumnItems, ...nonColumnItems].forEach((item) => {
      const columnType = item.column_type || "Uncategorized";
      const businessName = item.business_name || "";
      const technicalName = item.technical_name || "";
      const semanticType = item.semantic_type || "";
      const dataType = item.data_type || "";
      const key = item.key || "";
      const aggregation = item.aggregation || "SUM";
      const excluded = item.excluded || "NO";

      const hasAssociation = (window.data.associations || []).some(assoc => {
        const items = assoc.items || [];
        if (assoc.target1 === this.selectedRectangle.alias) {
          return items.some(it => it.target1 === businessName);
        }
        if (assoc.target2 === this.selectedRectangle.alias) {
          return items.some(it => it.target2 === businessName);
        }
        return false;
      });

      if (hasAssociation) {
        hasAnyAssociation = true;
      }

      if (!groups.has(columnType)) {
        const group = document.createElement("ul");
        group.setAttribute("aria-label", columnType);
        groups.set(columnType, group);

        if (columnType == "attribute" || columnType == "measure") {
          const div = document.createElement("div");
          div.style.display = "flex";
          div.style.justifyContent = "space-between";

          const p = document.createElement("p");
          p.textContent = columnType;

          const pencilButton = document.createElement("button");
          pencilButton.textContent = "✎";
          pencilButton.style.border = "none";
          pencilButton.style.backgroundColor = "unset";
          pencilButton.style.fontSize = "18px";

          div.append(p, pencilButton);

          EventHandler.on(pencilButton, "click", () => {
            renderContentOfEditDialog(this, columnType);
          }, false, "ColumnListComponent");

          group.appendChild(div);
        }
      }

      const listItem = document.createElement("li");

      const component = document.createElement("custom-web-component");
      component.setAttribute("alias", item.alias || "");
      component.setAttribute("data_type", dataType);
      component.setAttribute("column_type", columnType);
      component.setAttribute("business_name", businessName);
      component.setAttribute("technical_name", technicalName);
      component.setAttribute("semantic_type", semanticType);
      component.setAttribute("key", key);
      component.setAttribute("aggregation", aggregation);
      component.setAttribute("association", hasAssociation ? "yes" : "no");
      component.setAttribute("parent_object", this.selectedRectangle.btype);
      component.setAttribute("excluded", excluded);
      component.setAttribute("transform", item.transform && item.transform.length > 0 ? "YES" : "NO");
      listItem.appendChild(component);

      EventHandler.on(component, "excluded", (e) => {
        this.selectedRectangle.items.map(item => {
          if (item.business_name === businessName) item.excluded = e.detail.excluded;
        });
        this.state.updateField("items", this.selectedRectangle.items);
        this.setDirty();
      });
      groups.get(columnType).appendChild(listItem);
    });

    groups.forEach((group) => {
      this.listBox.appendChild(group);
    });

    this.filterButton.style.display = hasAnyAssociation ? "block" : "none";
    this.projectionButton.style.display = hasAnyAssociation ? "block" : "none";

    updateDropdownTooltips(this);
    setEventListenersToDropdownItems(this);
  }

  clearContent(container) {
    const content = container.querySelectorAll("ul");
    content.forEach((item) => {
      item.remove();
    });
  }

  closeItemDropdowns(business_name) {
    this.listBox.querySelectorAll("custom-web-component").forEach((c) => {
      if (c.business_name !== business_name) {
        const dropdown = c.shadowRoot.querySelector("#dropDowntooltip");
        dropdown.close();
      }
    })
  }

  addAlias({ businessName, alias }) {
    const item = this.selectedRectangle.items.find(
      (item) => item.business_name === businessName
    );

    if (!item) return;

    // item.business_name = businessName;
    item["alias"] = alias;

    const index = this.selectedRectangle.items.findIndex(c => c.business_name === businessName);
    // if alias is not empty, that means user want to add/update alias.
    // if alias is empty, that means user want to remove alias.
    if (index == -1) {
      this.selectedRectangle.items[index].alias = alias;
    }
    this.state.updateField("items", this.selectedRectangle.items);
    this.setDirty();
    this.renderContent();
  }
}

customElements.define("column-list-component", ColumnListComponent);
