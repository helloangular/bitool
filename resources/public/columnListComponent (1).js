import "./source/modules/smart.listbox.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.tooltip.js";
import "./source/modules/smart.table.js";
import "./source/modules/smart.dropdownlist.js";
import "./projectionComponent.js";
import { setEventListenersToDropdownItems } from "./setEventListenersToDropdownItems.js";
import { updateDropdownTooltips } from "./updateDropdownTooltips.js";
import { renderContentOfEditDialog } from "./renderContentOfEditDialog.js";

import EventHandler from "./library/eventHandler.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .columnlist-container {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
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
</style>

<div class="columnlist-container">
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="&#9747;" class="smart-button" style="margin-right:10px;"></smart-button>
  </div>
  <div style="display:flex;">
    <smart-button content="filter" id="filterButton" class="smart-button" style="font-size:10px;"></smart-button>
    <smart-button content="projection" id="projectionButton" class="smart-button" style="font-size:10px;"></smart-button>
  </div>
  <div style="padding:10px" id="generalForm">
    <label> 
      Business name: 
    </label>
    <smart-input class="outlined" name="businessName" style="width:100%" disabled></smart-input>
    <label>
      Technical name: 
    </label>
    <smart-input class="outlined" name="technicalName" style="width:100%" disabled></smart-input>
    <label>
      Alias:
    </label>
    <smart-input class="outlined" name="alias" style="width:100%" disabled></smart-input>
  </div>
  <div style="display:flex;align-items:center;width:100%;">
    <smart-input id="searchInput" style="width:100%;height:40px" placeholder="Search"></smart-input>
    <smart-button content="&#8645;" class="smart-button" style="margin-left:10px;margin-right:10px;display:none;" id="sortButton"></smart-button>
  </div>
  <smart-list-box id="settingsMenu" selection-mode="none" style="height: 100%; width: 100%" ></smart-list-box>
</div>
`;

class ColumnListComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.selectedRectangle = null;
    this.cacheElements();
    this.filterButton.style.display = "none";
    this.projectionButton.style.display = "none";
    this.setUpEventListeners();
    this.updateVisibility();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue == "open") {
        this.updateSelectedRectangle();
        this.renderContent();
      }
      this.updateVisibility();

      if (this.selectedRectangle?.parent_object === "Table_view")
        this.sortButton.style.display = "block";
      else this.sortButton.style.display = "none";
    }
  }

  cacheElements() {
    this.listBox = this.shadow.querySelector("smart-list-box");
    this.closeButton = this.shadow.querySelector("smart-button");
    this.searchInput = this.shadow.querySelector("#searchInput");
    this.sortButton = this.shadow.querySelector("#sortButton");
    this.textAssociationDialog = document.querySelector(".association-dialog");
    this.columnEditDialog = document.querySelector(".column-edit-dialog");
    this.filterButton = this.shadow.querySelector("#filterButton");
    this.projectionButton = this.shadow.querySelector("#projectionButton");
    this.projectionComponent = document.querySelector("projection-component");
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", this.handleCloseButtonClick.bind(this));
    EventHandler.on(this.searchInput, "changing", this.handleSearchInputChange.bind(this));
    EventHandler.on(this.filterButton, "click", this.handleFilterButtonClick.bind(this));
    EventHandler.on(this.projectionButton, "click", this.handleProjectionButtonClick.bind(this));
    EventHandler.on(this.sortButton, "click", this.handleSortButtonClick.bind(this));
  }

  handleCloseButtonClick() {
    this.setAttribute("visibility", "");
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

  handleSearchInputChange() {
    const searchQuery = e.detail.value;
    const items = this.listBox.querySelectorAll("smart-list-item");
    Array.from(items).forEach((item) => {
      if (item.value.toLowerCase().includes(searchQuery.toLowerCase())) {
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

  updateVisibility() {
    this.style.display =
      this.getAttribute("visibility") == "open" ? "block" : "none";
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id == window.data.selectedRectangle
    );
  }

  renderContent() {
    this.clearContent(this.listBox);

    this.searchInput.value = "";

    const businessNameInput = this.shadow.querySelector(
      "smart-input[name='businessName']"
    );
    const technicalNameInput = this.shadow.querySelector(
      "smart-input[name='technicalName']"
    );
    const aliasInput = this.shadow.querySelector("smart-input[name='alias']");

    businessNameInput.value = this.selectedRectangle.business_name;
    technicalNameInput.value = this.selectedRectangle.technical_name;
    aliasInput.value = this.selectedRectangle.alias;

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

      const hasAssociation = window.data.associations.find((association) => {
        if (association.target1 === this.selectedRectangle.alias)
          return association.items.find(
            (item) => item.target1 === businessName
          );
        if (association.target2 === this.selectedRectangle.alias)
          return association.items.find(
            (item) => item.target2 === businessName
          );
      });

      if (hasAssociation) {
        hasAnyAssociation = true;
      }

      if (!groups.has(columnType)) {
        const group = document.createElement("smart-list-items-group");
        group.setAttribute("label", columnType);
        groups.set(columnType, group);

        if (columnType == "attribute" || columnType == "measure") {
          const div = document.createElement("div");
          div.style.display = "flex";
          div.style.justifyContent = "flex-end";

          const pencilButton = document.createElement("button");
          pencilButton.innerHTML = "✎";
          pencilButton.style.border = "none";
          pencilButton.style.backgroundColor = "unset";
          pencilButton.style.fontSize = "18px";
          pencilButton.style.marginRight = "10%";

          div.appendChild(pencilButton);

          EventHandler.on(pencilButton, "click", () => {
            renderContentOfEditDialog(this, columnType);
          });

          group.appendChild(div);
        }
      }

      const listItem = document.createElement("smart-list-item");
      listItem.setAttribute("value", businessName);

      const component = document.createElement("custom-web-component");
      component.setAttribute("data_type", dataType);
      component.setAttribute("column_type", columnType);
      component.setAttribute("business_name", businessName);
      component.setAttribute("technical_name", technicalName);
      component.setAttribute("semantic_type", semanticType);
      component.setAttribute("key", key);
      component.setAttribute("aggregation", aggregation);
      component.setAttribute("association", hasAssociation ? "yes" : "no");
      component.setAttribute(
        "parent_object",
        this.selectedRectangle.parent_object
      );
      listItem.appendChild(component);

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
    const content = container.querySelectorAll("smart-list-items-group");
    content.forEach((item) => {
      item.remove();
    });
  }
}

customElements.define("column-list-component", ColumnListComponent);
