import EventHandler from "./library/eventHandler.js";

const template = document.createElement('template');
template.innerHTML = `
  <link rel="stylesheet" href="./app.css" />
  <link rel="stylesheet" href="./source/styles/smart.default.css" />
  <style>
    :host {
      display: none;
      z-index: 140;
      box-sizing: border-box;
    }
    .container {
      display: flex;
      flex-direction: column;
      gap: 10px;
      height: 100%;
      box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
    }
    .smart-button {
      --smart-button-padding: 4px;
      --smart-border-width: 0;
      --smart-font-size: 20px;
      --smart-ui-state-hover: white;
    }
    .section {
      margin-top: 10px;
      margin-bottom: 10px;
    }
    .padding {
      padding: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 20px;
    }
    th, td {
      border: 1px solid #ccc;
      padding: 8px;
      text-align: left;
    }
    th {
      background-color: #f4f4f4;
    }
    .btn {
      padding: 5px 10px;
      cursor: pointer;
    }
    .btn-add {
      background-color: #4CAF50;
      color: white;
      border: none;
    }
    .btn-remove {
      background-color: #f44336;
      color: white;
      border: none;
    }
  </style>
  <div class="container padding">
    <div style="display:flex;justify-content:flex-end;">
      <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
    </div>
    <div style="display:flex;justify-content:space-between;">
      <h3>Transform Editor</h3>
      <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;"></smart-button>
    </div>
    <div class="transformations">
      <select class="transformation-type">
        <option value="">Select Transformation</option>
        <option value="TRIM">TRIM</option>
        <option value="TO_DATE">TO_DATE</option>
        <option value="TO_VARCHAR">TO_VARCHAR</option>
        <option value="UPPERCASE">UPPERCASE</option>
        <option value="LOWERCASE">LOWERCASE</option>
        <option value="SUBSTRING">SUBSTRING</option>
      </select>
      <input type="text" class="parameters" placeholder="Parameters">
      <button class="btn btn-add">Add</button>
    </div>
    <ul class="transform-list"></ul>
  </div>
`;

class TransformEditor extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' }).appendChild(template.content.cloneNode(true));
    this.saveButton = this.shadowRoot.getElementById("saveButton");
    this.closeButton = this.shadowRoot.getElementById("closeButton");

    this.item = null;
  }

  static get observedAttributes() {
    return ["visibility", "item"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      this.updateVisibility();
    }
    if (name === "item") {
      const newItem = JSON.parse(newValue);
      this.shadowRoot.querySelector(".transform-list").innerHTML = "";
      if (newItem.transform) {
        newItem.transform.forEach((text) => {
          this.addListItem(text);
        });
      }
      this.item = newItem;
    }
  }

  connectedCallback() {
    this.updateVisibility();
    EventHandler.on(this.shadowRoot, 'click', (event) => {
      if (event.target.classList.contains('btn-remove') && event.target.getAttribute("onclick") === undefined) {
        this.removeRow(event.target);
      } else if (event.target.classList.contains('btn-add')) {
        this.addTransformation(event.target);
      }
    });

    EventHandler.on(this.saveButton, 'click', () => {
      this.save();
    });

    EventHandler.on(this.closeButton, 'click', () => {
      this.setAttribute("visibility", "close");
    });
  }

  updateVisibility() {
    this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  removeRow(button) {
    const row = button.closest('li');
    row.remove();
  }

  addTransformation(button) {
    const parentDiv = button.closest('.transformations');
    const transformType = parentDiv.querySelector(".transformation-type").value;
    const parameters = parentDiv.querySelector(".parameters").value;

    if (!transformType) {
      alert("Please select a transformation type.");
      return;
    }

    this.addListItem(`${transformType} (${parameters || "No parameters"}) `);

    // Reset the inputs
    parentDiv.querySelector(".transformation-type").value = "";
    parentDiv.querySelector(".parameters").value = "";
  }

  addListItem(text) {
    const listItem = document.createElement("li");
    listItem.textContent = text;
    const removeButton = document.createElement("button");
    removeButton.textContent = "Remove";
    removeButton.className = "btn btn-remove";
    removeButton.onclick = function () {
      listItem.remove();
    };

    listItem.classList.add("section");

    listItem.appendChild(removeButton);
    this.shadowRoot.querySelector(".transform-list").appendChild(listItem);
  }

  save() {
    const dataObj = {
      businessName: this.item.business_name,
      technicalName: this.item.technical_name,
      transformations: Array.from(this.shadowRoot.querySelectorAll(".transform-list li")).map(li => {
        return li.textContent.replace("Remove", "").trim();
      })
    }

    this.dispatchEvent(new CustomEvent("transform-data", { detail: dataObj }));
    this.setAttribute("visibility", "close");
  }
}

customElements.define('transform-editor', TransformEditor);
