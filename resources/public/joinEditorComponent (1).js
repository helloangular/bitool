import "./source/modules/smart.input.js";
import "./source/modules/smart.listbox.js";
import "./source/modules/smart.table.js";
import "./source/modules/smart.tooltip.js";

import EventHandler from "./library/eventHandler.js"
import { SvgConnector, FollowUpLine } from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `<link rel="stylesheet" href="./app.css" />
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .container {
    padding: 20px;
    height: 100%;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
    background-color: white;
    overflow: scroll;
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
  svg {
    width: 100%;
    height: 500px;
    border: 1px solid #ccc;
    user-select: none;
    pointer-events: fill;
  }
  rect {
    fill: #4a90e2;
    stroke: #2c3e50;
    stroke-width: 1.5;
    rx: 6;
    ry: 6;
  }
  text {
    fill: white;
    font-size: 12px;
    font-family: Arial, sans-serif;
  }
  path {
    cursor: pointer;
    pointer-events: stroke;
  }
  path:hover {
    stroke: red;
  }
  .graph-container {
    width: 100%;
    height: 80%;
    border: 1px solid #ccc;
    background: #fff;
    position: relative;
  }
  /* Side panel */
  .side-panel {
    position: fixed;
    top: 0;
    right: -350px;
    width: 350px;
    height: 100%;
    background: #f9f9f9;
    border-left: 1px solid #ccc;
    box-shadow: -2px 0 5px rgba(0,0,0,0.1);
    transition: right 0.3s ease;
    padding: 20px;
    overflow-y: auto;
  }
  .side-panel.open {
    right: 0;
    z-index: 5;
  }
  .side-panel header {
    font-size: 18px;
    font-weight: bold;
    margin-bottom: 15px;
  }
  .form-group {
    margin-bottom: 15px;
  }
  .form-group label {
    display: block;
    font-size: 14px;
    margin-bottom: 5px;
  }
  .side-panel svg {
    pointer-event: none;
  }
  .side-panel path {
    cursor: normal;
    pointer-events: none;
  }
  select {
    width: 100%;
    padding: 6px;
    border: 1px solid #ccc;
    border-radius: 4px;
  }
  button {
    padding: 8px 14px;
    border: none;
    border-radius: 4px;
    background: #4a90e2;
    color: #fff;
    cursor: pointer;
    margin-top: 10px;
  }
  button.close {
    background: #999;
    float: right;
  }
</style>

<div class="container">
  <div class="flex">
    <h3>Join</h3>
    <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
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
        <input type="checkbox" name="distinct" />
        <label>Distinct Values</label>
    </div>
  </div>
  <span style="font-size:16px;">Mappings</span>
  <div class="graph-container"></div>
  <!-- Side panel -->
  <div id="sidePanel" class="side-panel">
    <div class="flex">
      <h3>Join Table</h3>
      <smart-button content="&#9747;" id="closePanelBtn" class="smart-button" style="margin-right:10px;"></smart-button>
    </div>
    <div style="display:flex;justify-content:flex-end">
      <smart-button content="Save" id="savePanelBtn" style="margin-right:10px;font-size:10px;padding:6px;"></smart-button>
    </div>
    <div style="padding:10px">
      <div style="margin-bottom:10px;">
        <label>Join Type:</label>
        <div class="flex">
          <select class="dropdown" id="jTypeDropDown">
            <option value="v01">Inner</option>
            <option value="v02">Outer</option>
            <option value="v03">Other</option>
          </select>
        </div>
      </div
    </div>
    <div style="width:100%;margin-top:10px;">
      <span style="font-size:16px;">Mappings</span>
      <div class="graph-container"></div>
    </div>
  </div>
</div>
`;

class JoinEditorComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' });
    this.shadowRoot.append(template.content.cloneNode(true));
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue == "open") {
        this._reset();
        this._updateSelectedRectangle();
        this._renderContent();
      }
      this._updateVisibility();
    }
  }

  connectedCallback() {
    this.generalForm = this.shadowRoot.querySelector("#generalForm");
    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.container = this.shadowRoot.querySelector(".graph-container");
    this.connector = new SvgConnector(this.container);
    this.followUp = new FollowUpLine(this.connector.svg);
    this.sidePanel = this.shadowRoot.querySelector("#sidePanel");
    this.savePanelBtn = this.shadowRoot.querySelector("#savePanelBtn")
    this.closePanelBtn = this.shadowRoot.querySelector("#closePanelBtn")
    this.subPanelContainer = this.sidePanel.querySelector(".graph-container");
    this.subPanelConnector = new SvgConnector(this.subPanelContainer);
    this.subPanelFollowUp = new FollowUpLine(this.subPanelConnector.svg);
    this.jTypeDropDown = this.shadowRoot.querySelector("#jTypeDropDown");
    this.joinItems = [];
    this.tempJoinItems = [];
    // this.positions = {};
    this.activeTables = [];
    this.selectedRectangle = null;
    this.dirty = false;

    this.pointerState = {
      start: null,
      end: null,
      reset: () => {
        this.start = null;
        this.end = null;
      }
    }

    this._updateVisibility();
    this._setUpEventListeners();
    // this._updateSelectedRectangle();
    // this._renderContent();
  }

  disconnectedCallback() {
    EventHandler.removeGroup("Join");
  }

  // Called when adopted into a new document
  adoptedCallback() {
    console.log('MyElement moved to a new page.');
  }

  _setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", this._close.bind(this), {}, "Join");

    EventHandler.on(this.saveButton, "click", () => {
      if (this._isInputValidate()) {
        this._save();
        this._close();
      }
    }, {}, "Join");

    EventHandler.on(this.closePanelBtn, "click", this._closePanel.bind(this), {}, "Join");
    EventHandler.on(this.savePanelBtn, "click", this._savePanel.bind(this), {}, "Join");
    EventHandler.on(this.jTypeDropDown, "change", (e) => {
      this.tempJoinItems.forEach((items) => {
        const text = this.jTypeDropDown.options[this.jTypeDropDown.selectedIndex].text;
        items.jtype = text
      });
    })
    EventHandler.on(this.connector.svg, "pointerdown", this._onPointerDown.bind(this), {}, "Join");
    EventHandler.on(this.connector.svg, "pointermove", this._onPointerMove.bind(this), {}, "Join");
    EventHandler.on(this.connector.svg, "pointerup", this._onPointerUp.bind(this), {}, "Join");
    EventHandler.on(this.subPanelConnector.svg, "pointerdown", this._onPointerDownOnSubPanel.bind(this), {}, "Join");
    EventHandler.on(this.subPanelConnector.svg, "pointermove", this._onPointerMoveOnSubPanel.bind(this), {}, "Join");
    EventHandler.on(this.subPanelConnector.svg, "pointerup", this._onPointerUpOnSubPanel.bind(this), {}, "Join");
  }

  _trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("input, select, smart-input");
    inputs.forEach(el => {
      el.addEventListener("change", () => this.setDirty());
    });
  }

  _reset() {
    this.dirty = false;
    this.saveButton.disabled = true;
  }

  _setDirty() {
    this.dirty = true;
    this.saveButton.disabled = false;
  }

  _onPointerDown(ev) {
    if (ev.target.tagName === "rect") {
      this.pointerState.start = ev.target.getAttribute("id");
      this.followUp.start(ev.target, ev);
    }
  }

  _onPointerMove(ev) {
    if (this.followUp.previewLine) this.followUp.preview(ev);
  }

  _onPointerUp(ev) {
    if (ev.target.tagName === "rect") {
      this.followUp.end();
      this.pointerState.end = ev.target.getAttribute("id");
      const lineId = this.connector.connect(this.pointerState.start, this.pointerState.end, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });
      const startRect = this.connector.svg.querySelector(`#${this.pointerState.start}`);
      const endRect = ev.target;
      this.connector.connections.get(lineId).pathEl.onclick = () => this._openPanel(startRect.getAttribute("data-title"), endRect.getAttribute("data-title"));
      this.pointerState.reset();
      this.setDirty();
    }
  }

  _onPointerDownOnSubPanel(ev) {
    if (ev.target.tagName === "rect") {
      this.pointerState.start = ev.target.getAttribute("id");
      this.subPanelFollowUp.start(ev.target, ev);
    }
  }

  _onPointerMoveOnSubPanel(ev) {
    if (this.subPanelFollowUp.previewLine) this.subPanelFollowUp.preview(ev);
  }

  _onPointerUpOnSubPanel(ev) {
    if (ev.target.tagName === "rect") {
      this.subPanelFollowUp.end();
      this.pointerState.end = ev.target.getAttribute("id");
      this.subPanelConnector.connect(this.pointerState.start, this.pointerState.end, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });
      const select = this.shadowRoot.querySelector(".side-panel select");
      this.tempJoinItems.push({
        jtype: select.querySelector("option[value=" + select.value + "]").innerText,
        from: [
          this.subPanelConnector.svg.querySelector(`#${this.pointerState.start}`).getAttribute("data-table"),
          this.subPanelConnector.svg.querySelector(`#${this.pointerState.start}`).getAttribute("data-title")
        ],
        to: [
          this.subPanelConnector.svg.querySelector(`#${this.pointerState.end}`).getAttribute("data-table"),
          this.subPanelConnector.svg.querySelector(`#${this.pointerState.end}`).getAttribute("data-title")
        ]
      })
      this.pointerState.reset();
    }
  }

  _close() {
    if (this.dirty) {
      alert("You must save your changes before closing.");
      return; // prevent closing
    }
    this.setAttribute("visibility", "close");
  }

  _isInputValidate() {
    if (this.generalForm.querySelector("smart-input").value == "") {
      alert("Name is required");
      return false
    }
    return true
  }

  _save() {
    const select = this.shadowRoot.querySelector(".side-panel select");
    const dataObj = {
      id: this.selectedRectangle.id,
      alias: this.generalForm.querySelector("smart-input").value,
      business_name: this.generalForm.querySelector("smart-input").value,
      technical_name: this.generalForm.querySelector("smart-input").value,
      distinct: this.generalForm.querySelector("input[name=distinct]").checked,
      join_items: this.joinItems
    }

    console.log(dataObj);
    this._handleAjaxCall(dataObj);
  }

  _handleAjaxCall(dataObj) {
    fetch("/saveFunction", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(dataObj)
    }).then((response) => response.json())
      .then((data) => {
        this.selectedRectangle = data;
        this._reset();
      }).catch((error) => {
        alert("Error: " + error);
        console.error("Error:", error);
      });
  }

  _updateVisibility() {
    this.style.visibility = this.getAttribute("visibility") == "open" ? "visible" : "hidden";
  }

  _renderContent() {
    if (!this.selectedRectangle) return;
    this.connector.clear();
    const parent = this.connector.svg.parentElement;
    const width = parent.clientWidth;
    const height = parent.clientHeight;

    this.connector.svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

    const centerX = width / 2;
    const centerY = height / 2;
    const radius = Math.min(width, height) / 2.5;
    const rectWidth = 120, rectHeight = 40;

    const keys = Object.keys(this.selectedRectangle.items);
    const angleStep = (2 * Math.PI) / keys.length;

    keys.forEach((table, i) => {
      const angle = i * angleStep;
      const x = centerX + radius * Math.cos(angle) - rectWidth / 2;
      const y = centerY + radius * Math.sin(angle) - rectHeight / 2;

      // this.positions[table] = { x: x + rectWidth / 2, y: y + rectHeight / 2 };

      this._createRectangle({ title: table, x, y, rectWidth, rectHeight });
    });

    this._drawExistingLines();
  }

  _createRectangle({ table = null, title, x, y, rectWidth, rectHeight, isForPanel = false }) {
    // rectangle
    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", x);
    rect.setAttribute("y", y);
    rect.setAttribute("width", rectWidth);
    rect.setAttribute("height", rectHeight);
    rect.setAttribute("data-title", title);
    if (table) rect.setAttribute("data-table", table);
    isForPanel ? this.subPanelConnector.svg.appendChild(rect) : this.connector.svg.appendChild(rect);

    // title
    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", x + rectWidth / 2);
    text.setAttribute("y", y + rectHeight / 2 + 4);
    text.setAttribute("text-anchor", "middle");
    text.textContent = title;
    isForPanel ? this.subPanelConnector.svg.appendChild(text) : this.connector.svg.appendChild(text);

    /**
     * Problem in using custom-web-component.
     * custom-web-component need one data raw of table.
     * here we do field to field connection.
     * hence not possible and no need.
     */

    const id = isForPanel ? this.subPanelConnector.addNode(rect) : this.connector.addNode(rect);
    rect.setAttribute("id", id);
  }

  _openPanel(table1, table2) {
    this.sidePanel.classList.add("open");
    if (this.activeTables.length > 0 && this.activeTables[0] == table1 && this.activeTables[1] == table2) return;
    this._resetPanel();
    this.activeTables = [table1, table2];

    const rectWidth = 120, rectHeight = 40;
    let x = 10, y = 10;

    Object.keys(this.selectedRectangle.items[table1][0]).forEach((key) => {
      this._createRectangle({ table: table1, title: key, x, y, rectWidth, rectHeight, isForPanel: true });
      y += rectHeight + 10;
    });

    x += rectWidth + 30;
    y = 10;

    Object.keys(this.selectedRectangle.items[table2][0]).forEach((key) => {
      this._createRectangle({ table: table2, title: key, x, y, rectWidth, rectHeight, isForPanel: true });
      y += rectHeight + 10;
    });

    if (this.selectedRectangle?.join_items.length > 0) {
      this._drawExistingLines(true);
    }
  }

  _savePanel() {
    if (this.tempJoinItems.length > 0) {
      this.joinItems.push(...this.tempJoinItems);
      this.tempJoinItems = [];
    }
  }

  _closePanel() {
    if (this.tempJoinItems.length > 0) {
      alert("You must save your changes before closing.");
      return;
    }
    this.sidePanel.classList.remove("open");
  }

  _resetPanel() {
    this.subPanelConnector.clear();
    this.shadowRoot.querySelector(".side-panel select").selectedIndex = 0;
  }

  _updateSelectedRectangle() {
    // console.log(window.data.selectedRectangle);
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id == window.data.selectedRectangle
    );
    // fetch("getItem?id=4", {
    //   method: "GET",
    //   headers: {
    //     "Content-Type": "application/json",
    //   },
    // }).then((res) => res.json())
    //   .then((data) => {
    //     this.selectedRectangle = data;
    //     this._renderContent();
    //   })
  }

  _drawExistingLines(isForPanel = false) {
    if (!this.selectedRectangle) return;

    this.selectedRectangle.join_items.forEach((joins) => {
      if (isForPanel && this.activeTables[0] == joins.from[0] && this.activeTables[1] == joins.to[0]) {
        const from = this.subPanelContainer.querySelector(`rect[data-title=${joins.from[1]}][data-table=${joins.from[0]}]`);
        const to = this.subPanelContainer.querySelector(`rect[data-title=${joins.to[1]}][data-table=${joins.to[0]}]`);
        const fromId = from.getAttribute("id");
        const toId = to.getAttribute("id");
        this.subPanelConnector.connect(fromId, toId, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 })
      } else {
        const from = this.container.querySelector(`rect[data-title=${joins.from[0]}]`);
        const to = this.container.querySelector(`rect[data-title=${joins.to[0]}]`);
        const fromId = from.getAttribute("id");
        const toId = to.getAttribute("id");
        const lineId = this.connector.connect(fromId, toId, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });
        this.connector.connections.get(lineId).pathEl.onclick = () => this._openPanel(joins.from[0], joins.to[0]);  
      }
    })
  }
}

customElements.define("join-editor-component", JoinEditorComponent);