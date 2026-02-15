import "./source/modules/smart.button.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.listbox.js";
import "./customWebComponent.js";
import "./renameDialog.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .projection-container {
    display: flex;
    flex-direction: column;
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
  .content-area {
    padding: 10px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
</style>

<div class="projection-container">
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
  </div>
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;"></smart-button>
  </div>
  <div class="content-area">
    <smart-input id="projectionName" class="outlined" style="width:100%" value="Projection 1"></smart-input>
    <smart-input id="searchInput" class="outlined" style="width:100%" placeholder="Search"></smart-input>
  </div>
  <smart-list-box id="itemsList" selection-mode="none" style="width:100%;height:100%"></smart-list-box>
</div>`;

class ProjectionComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.selectedRectangle = null;
    this.selectedAssociation = null;
    this.excludedColumns = new Set();
    this.renamedColumns = new Map();

    this.closeButton = this.shadow.querySelector("#closeButton");
    this.saveButton = this.shadow.querySelector("#saveButton");
    this.searchInput = this.shadow.querySelector("#searchInput");
    this.itemsList = this.shadow.querySelector("#itemsList");
    this.projectionName = this.shadow.querySelector("#projectionName");
    this.columnListComponent = document.querySelector("column-list-component");

    this.renameDialog = document.createElement("rename-dialog");

    this.closeButton.addEventListener("click", () => {
      this.setAttribute("visibility", "");
    });

    this.saveButton.addEventListener("click", () => {
      this.save();
      this.setAttribute("visibility", "");
      this.columnListComponent.setAttribute("visibility", "open");
    });

    this.searchInput.addEventListener("changing", (e) => {
      const searchQuery = e.detail.value.toLowerCase();
      const items = this.itemsList.querySelectorAll("smart-list-item");

      Array.from(items).forEach((item) => {
        const component = item.querySelector("custom-web-component");
        const businessName = component
          .getAttribute("business_name")
          .toLowerCase();
        item.style.display = businessName.includes(searchQuery)
          ? "block"
          : "none";
      });
    });

    this.addEventListener("dropdown-item-click", (e) => {
      const { itemText, businessName } = e.detail;
      if (itemText === "Exclude Column") {
        this.handleExcludeColumn(businessName);
      } else if (itemText === "Include Column") {
        this.handleIncludeColumn(businessName);
      } else if (itemText === "Change Name") {
        this.handleChangeNameClick(businessName);
      }
    });

    this.renameDialog.addEventListener("rename", (e) => {
      const {
        oldBusinessName,
        oldTechnicalName,
        newBusinessName,
        newTechnicalName,
      } = e.detail;
      this.handleRename(
        oldBusinessName,
        oldTechnicalName,
        newBusinessName,
        newTechnicalName
      );
    });

    this.updateVisibility();
  }

  handleChangeNameClick(businessName) {
    const item = this.selectedAssociation.items.find(
      (item) => item.business_name === businessName
    );
    if (item) {
      this.renameDialog.setAttribute("business-name", item.business_name);
      this.renameDialog.setAttribute("technical-name", item.technical_name);
      this.renameDialog.setAttribute("visibility", "open");
    }
  }

  save() {
    window.data.associations = window.data.associations.map((association) => {
      if (
        (association.target1 === this.selectedRectangle.alias &&
          association.target2 === this.selectedAssociation.alias) ||
        (association.target2 === this.selectedRectangle.alias &&
          association.target1 === this.selectedAssociation.alias)
      ) {
        return {
          ...association,
          projection: {
            name: this.projectionName.value,
            excludedColumns: Array.from(this.excludedColumns),
            renamedColumns: Array.from(this.renamedColumns.values()),
          },
        };
      }

      return association;
    });
  }

  loadProjection(projection) {
    if (!projection) return;

    this.projectionName.value = projection.name || "Projection 1";
    this.excludedColumns = new Set(projection.excludedColumns || []);

    this.renamedColumns = new Map();
    if (projection.renamedColumns) {
      projection.renamedColumns.forEach((rename) => {
        this.renamedColumns.set(rename.oldBusinessName, rename);
      });
    }
  }

  handleRename(
    oldBusinessName,
    oldTechnicalName,
    newBusinessName,
    newTechnicalName
  ) {
    const item = this.selectedAssociation.items.find(
      (item) => item.business_name === oldBusinessName
    );
    if (item) {
      item.business_name = newBusinessName;
      item.technical_name = newTechnicalName;

      this.renamedColumns.set(oldBusinessName, {
        oldBusinessName,
        oldTechnicalName,
        newBusinessName,
        newTechnicalName,
      });
      this.renderItems();
    }
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue === "open") {
        this.updateSelectedRectangle();
        this.updateSelectedAssociation();
        this.renderItems();
      }
      this.updateVisibility();
    }
  }

  updateVisibility() {
    this.style.display =
      this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.alias === window.data.selectedRectangle
    );
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

    this.loadProjection(this.selectedAssociation.projection);
  }

  renderItems() {
    this.itemsList
      .querySelectorAll("smart-list-item")
      .forEach((item) => item.remove());

    if (!this.selectedAssociation || !this.selectedAssociation.items) return;

    this.selectedAssociation.items.forEach((item) => {
      const listItem = document.createElement("smart-list-item");

      const component = document.createElement("custom-web-component");
      component.setAttribute("data_type", item.data_type || "");
      component.setAttribute("column_type", item.column_type || "");
      component.setAttribute("business_name", item.business_name || "");
      component.setAttribute("technical_name", item.technical_name || "");
      component.setAttribute("semantic_type", item.semantic_type || "");
      component.setAttribute("key", item.key || "");
      component.setAttribute("aggregation", item.aggregation || "");
      component.setAttribute("parent_object", "Projection");
      component.setAttribute(
        "excluded",
        this.excludedColumns.has(item.business_name)
      );

      listItem.appendChild(component);
      this.itemsList.appendChild(listItem);

      const dropdownTooltip =
        component.shadow.querySelector("#dropDowntooltip");
      if (dropdownTooltip) {
        dropdownTooltip.addEventListener("click", (e) => {
          const tooltipItem = e.target.closest(".tooltip-item");
          if (tooltipItem) {
            this.dispatchEvent(
              new CustomEvent("dropdown-item-click", {
                detail: {
                  itemText: tooltipItem.textContent,
                  businessName: item.business_name,
                },
              })
            );
          }
        });
      }
    });
  }

  handleExcludeColumn(businessName) {
    this.excludedColumns.add(businessName);
    this.renderItems();
  }

  handleIncludeColumn(businessName) {
    this.excludedColumns.delete(businessName);
    this.renderItems();
  }
}

customElements.define("projection-component", ProjectionComponent);
