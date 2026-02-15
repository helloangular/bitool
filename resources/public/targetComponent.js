import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import {request} from "./library/utils.js";
import "./projectionComponent.js";
import {renderContentOfEditDialog} from "./renderContentOfEditDialog.js";
import {setEventListenersToDropdownItems} from "./setEventListenersToDropdownItems.js";
import "./source/modules/smart.dropdownlist.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.listbox.js";
import "./source/modules/smart.table.js";
import "./source/modules/smart.tooltip.js";
import {updateDropdownTooltips} from "./updateDropdownTooltips.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .columnlist-container {
    display: flex;
    padding: 0 10px;
    flex-direction: column;
    align-items: flex-end;
    gap: 10px;
    height: 100%;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
  }
  select, input[type="text"] {
    padding: 6px 12px;
    margin-bottom: 10px;
  }
  ul {
    margin: 0;
    padding: 0;
  }
  li {
    list-style: none;
    width: 100%;
  }
</style>

<div class="columnlist-container">
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="&#9747;" class="smart-button"></smart-button>
  </div>
  <div style="display:flex;">
    <smart-button content="Save" id="saveButton" class="smart-button" style="font-size:10px;" disabled></smart-button>
  </div>
  <div id="generalForm" style="display:flex; flex-direction:column; width:100%;">
    <label>Connection</label>
    <select name="connection">
      <option value="">Select connection</option>
      <option value="MySQL1">MySQL1</option>
      <option value="MySQL2">MySQL2</option>
      <option value="Postgres">Postgres</option>
      <option value="MSSQL">MSSQL</option>
      <option value="Oracle">Oracle</option>
    </select>
    <label>Table Name</label>
    <input type="text" name="table_name" placeholder="Enter table name" />
    <div>
      <label>
        <input type="checkbox" name="truncate" />
        Truncate for each run
      </label>

      <label>
        <input type="checkbox" name="create_table" />
        Create table if not exist
      </label>
    </div>
  </div>
  <div style="display:flex;align-items:center;width:100%;">
    <smart-input id="searchInput" style="width:100%;height:40px" placeholder="Search"></smart-input>
    <smart-button content="&#8645;" class="smart-button" style="margin-left:10px;margin-right:10px;display:none;" id="sortButton"></smart-button>
  </div>
  <div id="settingsMenu" style="height:100%;width:100%;overflow-y:auto;"></div>
</div>
`;

class TargetComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({mode: "open"});
  }

  connectedCallback() {
    this.shadowRoot.append(template.content.cloneNode(true));

    this.listBox = this.shadowRoot.querySelector("#settingsMenu");
    this.closeButton = this.shadowRoot.querySelector("smart-button");
    this.searchInput = this.shadowRoot.querySelector("#searchInput");
    this.sortButton = this.shadowRoot.querySelector("#sortButton");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.columnEditDialog = document.querySelector(".column-edit-dialog");
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
        this.updateSelectedRectangle();
        this.updateFields();
        this.renderContent();
        this.setUpEventListeners();
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
    EventHandler.on(this.sortButton, "click", this.handleSortButtonClick.bind(this), false, "ColumnListComponent");
    EventHandler.on(this.shadowRoot, "click", (e) => {
      const bn = e.target?.business_name || (e.target && e.target.getAttribute && e.target.getAttribute('business_name')) || null;
      this.closeItemDropdowns(bn);
    }, false, "ColumnListComponent");
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
    const fields = this.shadowRoot.querySelectorAll("select, input");
    Array.from(fields).forEach(f => {
      (f.type === "checkbox") ? f.checked = this.selectedRectangle[f.name] : f.value = this.selectedRectangle[f.name] || ""
    })
    this.state = new StateManager({
      connection: this.selectedRectangle["connection"] || "",
      table_name: this.selectedRectangle["table_name"] || "",
      truncate: this.selectedRectangle["truncate"] || false,
      create_table: this.selectedRectangle["create_table"] || false,
      items: (this.selectedRectangle && Array.isArray(this.selectedRectangle.items)) ? this.selectedRectangle.items : []
    });
  }

  resetFields() {
    const fields = this.shadowRoot.querySelectorAll("select, input");
    Array.from(fields).forEach(f => {
      (f.type === "checkbox") ? f.checked = this.selectedRectangle[f.name] : f.value = this.selectedRectangle[f.name]
    })
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
    const inputs = this.shadowRoot.querySelectorAll("input, select");
    inputs.forEach(el => {
      if (el.tagName === "SELECT") {
        EventHandler.on(el, 'change', (e) => {
          const val = e.target.value;
          this.state.updateField(e.target.name, val);
          this.setDirty();
        }, false, "ColumnListComponent");
      } else {
        EventHandler.on(el, "input", (e) => {
          const val = e.target.type === "checkbox" ? e.target.checked : e.target.value;
          this.state.updateField(e.target.name, val);
          this.setDirty()
        }, false, "ColumnListComponent");
      }
    });
  }

  setDirty() {
    this.saveButton.disabled = !this.state.isDirty();
  }

  getValues() {
    return {
      id: this.selectedRectangle.id,
      ...this.state.getUpdatedFieldsWithRequiredFields(['truncate', 'create_table', 'table_name', 'connection'])
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
          return {...rect, sorted_by: "asc"};

        return {...rect, sorted_by: "desc"};
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
    if (!this.selectedRectangle) return;

    this.clearContent(this.listBox);

    // If there are no items to show, render a friendly empty message
    if (!this.selectedRectangle.items || this.selectedRectangle.items.length === 0) {
      const emptyMessage = document.createElement('div');
      emptyMessage.textContent = 'No columns available for this table.';
      emptyMessage.style.padding = '12px';
      emptyMessage.style.color = '#666';
      emptyMessage.style.fontStyle = 'italic';
      this.listBox.appendChild(emptyMessage);
      return;
    }

    if (this.listBox.children.length > 0) this.listBox.children[0].remove();

    const groups = new Map();

    const sortedBy = this.selectedRectangle.sorted_by || "asc";

    const columnItems = Array.from(this.selectedRectangle.items || []).filter(
      (item) => item.column_type === "column"
    );
    const nonColumnItems = Array.from(this.selectedRectangle.items || []).filter(
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

      let hasAnyAssociation = false;

      const hasAssociation = (window.data && window.data.associations || []).some(assoc => {
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
      }, false, "ColumnListComponent");
      groups.get(columnType).appendChild(listItem);
    });

    groups.forEach((group) => {
      this.listBox.appendChild(group);
    });

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
      const bn = c.getAttribute ? c.getAttribute('business_name') : c.business_name;
      if (bn !== business_name) {
        const dropdown = c.shadowRoot && c.shadowRoot.querySelector && c.shadowRoot.querySelector("#dropDowntooltip");
        if (dropdown && typeof dropdown.close === 'function') dropdown.close();
      }
    })
  }

  addAlias({businessName, alias}) {
    const item = this.selectedRectangle.items.find(
      (item) => item.business_name === businessName
    );

    if (!item) return;

    // item.business_name = businessName;
    item["alias"] = alias;

    const index = this.selectedRectangle.items.findIndex(c => c.business_name === businessName);
    if (index !== -1) {
      this.selectedRectangle.items[index].alias = alias;
    }
    this.state.updateField("items", this.selectedRectangle.items);
    this.setDirty();
    this.renderContent();
  }
}

customElements.define("target-component", TargetComponent);
