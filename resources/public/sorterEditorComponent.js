import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="container padding">
        <div style="display:flex;justify-content:flex-end">
            <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
        </div>
        <div style="display:flex;justify-content:space-between;">
            <h3 style="margin-left:10px;">Sorter</h3>
            <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;"></smart-button>
        </div>
        <div id="content">
            <div id="list" class="section"></div>
            <div class="section">
                <label>Name</label>
                <input type="text" class="margin-left" name="name">
            </div>
            <div class="section">
                <select name="option"></select>
                <select class="margin-left" name="order">
                    <option value="ASC">ASC</option>
                    <option value="DESC">DESC</option>
                </select>
            </div>
            <button type="button" class="button" id="add-sorter-btn">Add Sorter</button>
        </div>
    </div>
`;

class SorterEditor extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
  }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));

    this.addSorterBtn = this.shadowRoot.querySelector("#add-sorter-btn");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.closeBtn = this.shadowRoot.querySelector("#closeButton");
    this.list = this.shadowRoot.querySelector("#list");
    this.content = this.shadowRoot.querySelector("#content");
    this.optionSelect = this.shadowRoot.querySelector('select[name="option"]');
    this.orderSelect = this.shadowRoot.querySelector('select[name="order"]');
    this.selectedRectangle = null;
    this.state = null;

    this.close();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    console.log(`Attribute: ${name} changed from ${oldValue} to ${newValue}`);
    if (name === "visibility") {
      if (newValue === "open") {
        this.open();
        this.updateSelectedRectangle();
        this.populateDropDown();
        this.setUpEventListeners();
      } else {
        this.close();
        if (this.style.visibility === "hidden") {
          this.resetFields();
          EventHandler.removeGroup("Sorter");
        }
      }
    }
  }

  setUpEventListeners() {
    EventHandler.on(this.addSorterBtn, 'click', () => this.addSorter(), false, "Sorter");
    EventHandler.on(this.saveButton, 'click', () => this.save(), false, "Sorter");
    EventHandler.on(this.closeBtn, 'click', () => this.setAttribute("visibility", "close"), false, "Sorter");
    this.trackInputs();
  }

  open() {
    this.style.visibility = "visible";
    this.reset();
  }

  reset() {
    this.saveButton.disabled = true;
  }

  resetFields() {
    this.shadowRoot.querySelector("input[name='name']").value = "";
    this.optionSelect.selectedIndex = 0;
    this.orderSelect.selectedIndex = 0;
    this.list.innerHTML = '';
    this.state.commit();
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
    request("/saveSorter", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      // this.populateDropDown();
      this.state.commit();
      this.reset();
      this.setAttribute("visibility", "close");
    })
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("input, select");
    inputs.forEach(el => {
      EventHandler.on(el, "input", (e) => {
        // Update field in state
        if (e.target.name === "name") {
          this.state.updateField("name", e.target.value);
        } else if (e.target.name === "option" || e.target.name === "order") {
          // rebuild sorter list
          this.state.updateField("sorter", this.collectSorters());
        }
        this.setDirty()
      }, false, "Sorter");
    });
  }

  setDirty() {
    this.saveButton.disabled = !this.state.isDirty();
  }

  getValues() {
    const values = {
      id: this.selectedRectangle.id,
      name: this.content.querySelector('input[name="name"]').value,
      sorters: this.collectSorters()
    };

    return values;
  }

  collectSorters() {
    const sorters = [];
    const selects = this.shadowRoot.querySelectorAll("select[name='option']");
    const orderSel = this.shadowRoot.querySelectorAll("select[name='order']");
    selects.forEach((sel, i) => {
      const item = this.selectedRectangle.items.find(item => item.technical_name === sel.value);
      if (item) {
        sorters.push({
          tid: item.tid,
          option: sel.value,
          order: orderSel[i].value
        });
      }
    });
    return sorters;
  }

  updateSelectedRectangle() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id === window.data.selectedRectangle
    );

    // Initialize state with current rectangle
    this.state = new StateManager({
      name: "",
      sorter: []
    });
  }

  populateDropDown() {
    this.optionSelect.innerHTML = ''; // Clear existing options

    if (this.selectedRectangle && this.selectedRectangle.items) {
      this.selectedRectangle.items.forEach(item => {
        const option = document.createElement('option');
        option.value = item.technical_name;
        option.textContent = item.technical_name;
        this.optionSelect.appendChild(option);
      });
    }
  }

  addSorter() {
    const secondSection = this.content.querySelector(".section:has(>select)").cloneNode(true); 
    
    secondSection.querySelector('select[name="option"]').selectedIndex = this.optionSelect.selectedIndex;
    secondSection.querySelector('select[name="order"]').selectedIndex = this.orderSelect.selectedIndex;

    secondSection.appendChild(this.createDeleteButton(secondSection));

    this.list.appendChild(secondSection);

    // Resret the selects.
    this.optionSelect.selectedIndex = 0;
    this.orderSelect.selectedIndex = 0;
    
    this.state.updateField("sorter", this.collectSorters());
    this.setDirty();
  }

  createDeleteButton(container) {
    const btn = document.createElement("button");
    btn.textContent = "Delete";
    btn.classList.add("button", "margin-left");
    EventHandler.on(btn, "click", () => {
      container.remove()
      this.state.updateField("sorter", this.collectSorters());
      this.setDirty();
    });
    return btn;
  }
}

customElements.define('sorter-editor', SorterEditor);
