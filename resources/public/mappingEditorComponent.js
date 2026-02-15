import EventHandler from "./library/eventHandler.js";
import {request} from "./library/utils.js";
import StateManager from "./library/state-manager.js";

const template = document.createElement("template");
template.innerHTML = `
  <link rel="stylesheet" href="./app.css" />
  <link rel="stylesheet" href="./source/styles/smart.default.css" />
  <style>
    .container { display:flex; flex-direction:column; gap:10px; height:100%; padding:10px; }
    table { width:100%; border-collapse:collapse; }
    th,td { border:1px solid #ccc; padding:8px; }
    th { background:#f4f4f4; }
    .smart-button {
      --smart-button-padding: 4px;
      --smart-border-width: 0;
      --smart-font-size: 20px;
      --smart-ui-state-hover: white;
    }
    .btn { cursor:pointer; padding:4px 8px; }
    .btn-add-transformation { background:#4caf50; color:#fff; border:0; }
    .btn-remove { background:#f44336; color:#fff; border:0; }
    ul { list-style:none; padding:0; margin:0; }
    li { display:flex; justify-content:space-between; margin-bottom:4px; }
  </style>

  <div class="container">
    <div style="display:flex; justify-content:flex-end;">
      <smart-button content="&#9747;" id="closeButton" class="smart-button"></smart-button>
    </div>

    <div style="display:flex; justify-content:space-between;">
      <h3>Mapping Editor</h3>
      <smart-button content="Save" id="saveButton" style="margin-right:10px; font-size:10px;"></smart-button>
    </div>

    <table id="transformTable">
      <thead>
        <tr>
          <th>Source Column</th>
          <th>Transformations</th>
          <th>Target Column</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
`;

class MappingEditor extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({mode: "open"});
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this.saveButton = this.shadowRoot.getElementById("saveButton");
    this.closeButton = this.shadowRoot.getElementById("closeButton");
    this.table = this.shadowRoot.getElementById("transformTable");
    this.selectedRectangle = null;
    this.state = null;

    this.close();
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue === "open") {
        this.open();
        this.updateSelectedRectangle();
        this.initializeState();
        this.populateRows();
        this.setupListeners();
      } else {
        this.close();
        EventHandler.removeGroup("Mapping");
      }
    }
  }

  setupListeners() {
    EventHandler.on(this.table, "change", (e) => {
      if (e.target.name === "source-column" || e.target.name === "target-column") {
        this.onColumnSelect(e.target);
      }
    }, false, "Mapping");

    EventHandler.on(this.table, "click", (e) => {
      if (e.target.classList.contains("btn-add-transformation")) {
        this.addTransformation(e.target);
      }
      else if (e.target.classList.contains("btn-remove")) {
        this.removeItem(e.target);
      }
    }, false, "Mapping");

    EventHandler.on(this.saveButton, "click", () => this.save(), false, "Mapping");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "Mapping");
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(r => r.id === window.data.selectedRectangle);
    if (!this.selectedRectangle.mapping) this.selectedRectangle.mapping = [];
  }

  initializeState() {
    this.state = new StateManager({mapping: this.selectedRectangle.mapping});
    this.saveButton.disabled = true;
  }

  open() {this.style.visibility = "visible";}

  close() {
    if (this.state && this.state.isDirty()) {
      const ask = confirm("Unsaved changes detected. Save before closing?");
      if (!ask) this.style.visibility = "hidden";
      return;
    }
    this.style.visibility = "hidden";
  }

  generateOptions(list) {
    return list.map(x => `<option value="${x.technical_name}">${x.technical_name}</option>`).join("");
  }

  populateRows() {
    const tbody = this.table.querySelector("tbody");
    tbody.innerHTML = "";

    const max = this.selectedRectangle.target.length;

    for (let i = 0; i < max; i++) {
      const row = document.createElement("tr");
      row.dataset.row = i;

      const src = this.generateOptions(this.selectedRectangle.source);
      const tgt = this.generateOptions(this.selectedRectangle.target);

      const m = this.selectedRectangle.mapping[i] || {source: {}, target: {}, transform: []};

      row.innerHTML = `
        <td>
          <select name="source-column" data-row="${i}">
            <option value=""></option>
            ${src}
          </select>
        </td>

        <td>
          <div class="transformations">
            <select class="transformation-type">
              <option value="">Select Transformation</option>
              <option value="TRIM">TRIM</option>
              <option value="TO_DATE">TO_DATE</option>
              <option value="UPPERCASE">UPPERCASE</option>
              <option value="LOWERCASE">LOWERCASE</option>
              <option value="SUBSTRING">SUBSTRING</option>
            </select>
            <input type="text" class="parameters" placeholder="Parameters" />
            <button class="btn btn-add-transformation" data-row="${i}">Add</button>
          </div>
          <ul class="transform-list" data-row="${i}"></ul>
        </td>

        <td>
          <select name="target-column" data-row="${i}" disabled>
            <option value=""></option>
            ${tgt}
          </select>
        </td>

        <td>
          <button class="btn btn-remove" data-row="${i}">Remove Row</button>
        </td>
      `;

      tbody.appendChild(row);

      console.log(m.source.technical_name);
      // set selected source/target
      if (m.source?.technical_name) row.querySelector("[name='source-column']").selectedIndex = i + 1; //  m.source.technical_name;
      if (m.target?.technical_name) row.querySelector("[name='target-column']").selectedIndex = i + 1; //  m.target.technical_name;

      // load transformations
      const list = row.querySelector(".transform-list");
      if (Array.isArray(m.transform)) {
        m.transform.forEach((t, ti) => {
          const li = document.createElement("li");
          li.dataset.row = i;
          li.dataset.index = ti;
          li.innerHTML = `${t} <button class="btn btn-remove">Remove</button>`;
          list.appendChild(li);
        });
      }
    }
  }

  ensureRow(index) {
    while (this.selectedRectangle.mapping.length <= index) {
      this.selectedRectangle.mapping.push({source: {}, transform: [], target: {}});
    }
  }

  onColumnSelect(select) {
    const row = Number(select.dataset.row);
    const value = select.value;

    this.ensureRow(row);

    if (select.name === "source-column") {
      this.selectedRectangle.mapping[row].source =
        this.selectedRectangle.source.find(x => x.technical_name === value) || {};
    } else {
      this.selectedRectangle.mapping[row].target =
        this.selectedRectangle.target.find(x => x.technical_name === value) || {};
    }

    this.state.updateField("mapping", this.selectedRectangle.mapping);
    this.saveButton.disabled = !this.state.isDirty();
  }

  addTransformation(btn) {
    const row = Number(btn.dataset.row);
    this.ensureRow(row);

    const wrap = btn.closest(".transformations");
    const type = wrap.querySelector(".transformation-type").value;
    const params = wrap.querySelector(".parameters").value;

    if (!type) return alert("Select transformation type");

    const expr = `${type}(${params})`;
    this.selectedRectangle.mapping[row].transform.push(expr);

    this.state.updateField("mapping", this.selectedRectangle.mapping);
    this.saveButton.disabled = !this.state.isDirty();

    this.populateRows();
  }

  removeItem(btn) {
    const row = Number(btn.dataset.row);

    // remove entire row
    if (btn.closest("tr")) {
      this.selectedRectangle.mapping.splice(row, 1);
      this.state.updateField("mapping", this.selectedRectangle.mapping);
      this.saveButton.disabled = !this.state.isDirty();
      this.populateRows();
      return;
    }

    // remove transformation
    const li = btn.closest("li");
    const tindex = Number(li.dataset.index);
    this.selectedRectangle.mapping[row].transform.splice(tindex, 1);

    this.state.updateField("mapping", this.selectedRectangle.mapping);
    this.saveButton.disabled = !this.state.isDirty();

    this.populateRows();
  }

  save() {
    request("/saveMapping", {
      method: "POST",
      body: {
        id: this.selectedRectangle ? this.selectedRectangle.id : "",
        ...this.state.updatedFields
      }
    }).then(data => {
      this.selectedRectangle = data;
      this.state.commit();
      this.saveButton.disabled = true;
      this.setAttribute("visibility", "close");
    });
  }
}

customElements.define("mapping-editor", MappingEditor);
