import "./source/modules/smart.input.js";
import "./source/modules/smart.listbox.js";
import "./source/modules/smart.table.js";
import "./source/modules/smart.tooltip.js";

import EventHandler from "./library/eventHandler.js"
import { request } from "./library/utils.js";
import StateManager from "./library/state-manager.js";
import SvgConnector from "./library/svg-connector.js";
import FollowUpLine from "./library/follow-up-line.js";

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
  rect.danger {
    fill: #f44336;
    stroke: red;
    stroke-width: 1.0;
    rx: 3;
    ry: 3;
  }
  text {
    fill: white;
    font-size: 12px;
    font-family: Arial, sans-serif;
    pointer-events: none;
  }
  path {
    cursor: pointer;
    pointer-events: stroke;
  }
  path:hover {
    stroke: red;
  }
  .graph-container {
    position: relative;
    width: 100%;
    height: 80%;
    min-height: fit-content;
    border: 1px solid #ccc;
    background: #fff;
  }
  /* Side panel */
  .side-panel {
    position: fixed;
    top: 0;
    right: -360px;
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
  .side-panel path {
    cursor: pointer;
    pointer-events: stroke;
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
      <smart-input class="outlined" name="alias" style="width:100%" required></smart-input>
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
          <button id="deleteBtn" style="background: #f44336;">Delete Connection</button>
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
    this.attachShadow({ mode: "open" });
  }

  connectedCallback() {
    this.shadowRoot.append(template.content.cloneNode(true));

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
    this.state = null;
    this.panelState = null;
    this.joinItems = [];
    this.activeTables = [];
    this.selectedRectangle = null;

    this.pointerState = {
      start: null,
      end: null,
      reset: () => {
        this.start = null;
        this.end = null;
      }
    }

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
        this.updateFields();
        this.renderContent();
        this.setUpEventListeners();
      } else {
        this.close();
        if (this.style.visibility === "hidden") {
          this.resetFields();
          EventHandler.removeGroup("Join");
        }
      }
    }
  }

  updateFields() {
    if (this.selectedRectangle == null) {
      setTimeout(() => {
        this.updateFields();
      }, 100);
      return;
    }
    this.shadowRoot.querySelector("smart-input").value = this.selectedRectangle.alias;
    this.shadowRoot.querySelector('input[type="checkbox"]').ariaChecked = this.selectedRectangle.distinct;
    this.state = new StateManager({
      id: this.selectedRectangle.id,
      alias: this.selectedRectangle.alias,
      join_items: [...this.selectedRectangle.join_items],
      distinct: this.selectedRectangle.distinct
    })
  }

  resetFields() {
    this.shadowRoot.querySelector("smart-input").value = "";
    this.shadowRoot.querySelector('input[type="checkbox"]').ariaChecked = false;
    this.connector.clear();
    this.subPanelConnector.clear();
    this.activeTables = [];
    this.joinItems = [];
    this.state.commit();
    this.panelState.commit();
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), {}, "Join");

    EventHandler.on(this.saveButton, "click", () => {
      if (this.isInputValidate()) {
        this.save();
      }
    }, {}, "Join");

    EventHandler.on(this.closePanelBtn, "click", this.closePanel.bind(this), {}, "Join");
    EventHandler.on(this.savePanelBtn, "click", this.savePanel.bind(this), {}, "Join");
    EventHandler.on(this.jTypeDropDown, "change", (e) => {
      this.joinItems.forEach((join) => {
        if (join.from[0] === this.activeTables[0] && join.to[0] === this.activeTables[1]) {
          join.jtype = e.target.selectedOptions[0].innerText;
          this.panelState.updateField("jtype", e.target.selectedOptions[0].innerText);
          this.setDirty(true);
          return;
        }
      });
    });
    EventHandler.on(this.connector.svg, "pointerdown", this.onPointerDown.bind(this), {}, "Join");
    EventHandler.on(this.connector.svg, "pointermove", this.onPointerMove.bind(this), {}, "Join");
    EventHandler.on(this.connector.svg, "pointerup", this.onPointerUp.bind(this), {}, "Join");
    EventHandler.on(this.subPanelConnector.svg, "pointerdown", this.onPointerDownOnSubPanel.bind(this), {}, "Join");
    EventHandler.on(this.subPanelConnector.svg, "pointermove", this.onPointerMoveOnSubPanel.bind(this), {}, "Join");
    EventHandler.on(this.subPanelConnector.svg, "pointerup", this.onPointerUpOnSubPanel.bind(this), {}, "Join");
    EventHandler.on(window, "resize", (e) => {
      this.renderContent();
    });
    EventHandler.on(this.shadowRoot.getElementById("deleteBtn"), "click", (e) => {
      const deleteLine = confirm("Do you want to delete this table connection?");
      if (deleteLine) {
        this.connector.disconnect(e.target.dataset.lineid);
        const startRect = this.connector.svg.getElementById(e.target.dataset.start);
        const endRect = this.connector.svg.getElementById(e.target.dataset.end);
        const index = this.joinItems.findIndex(j => j.from[0] === startRect.getAttribute("data-title") && j.to[0] === endRect.getAttribute("data-title"));
        this.joinItems.splice(index, 1);
        this.state.updateField("join_items", this.joinItems);
        this.setDirty();
        this.reset(true);
        this.closePanel();
      }
    }, false, "Join");
    this.trackInputs();
  }

  open() {
    this.style.visibility = "visible";
    this.saveButton.disabled = true;
  }

  reset(panel = false) {
    if (!panel) {
      this.saveButton.disabled = true;
    } else {
      this.savePanelBtn.disabled = true;
    }
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
    request("/saveJoin", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      this.state.commit();
      this.reset();
      this.setAttribute("visibility", "close");
    }).catch((error) => console.error(error));
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("input, smart-input");
    inputs.forEach(el => {
      EventHandler.on(el, "input", (e) => {
        const el = e.target;
        this.state.updateField(el.name, el.type == "checkbox" ? el.checked : el.value);
        this.setDirty()
      }, false, "Join");
    });
  }

  setDirty(panel = false) {
    if (!panel) {
      this.saveButton.disabled = !this.state.isDirty();
    } else {
      this.savePanelBtn.disabled = !this.panelState.isDirty();
    }
  }

  getValues() {
    return {
      id: this.selectedRectangle.id,
      alias: this.generalForm.querySelector("smart-input").value,
      business_name: this.generalForm.querySelector("smart-input").value,
      technical_name: this.generalForm.querySelector("smart-input").value,
      distinct: this.generalForm.querySelector("input[name=distinct]").checked,
      join_items: this.joinItems
    }
  }

  onPointerDown(ev) {
    if (ev.target.tagName === "rect") {
      this.pointerState.start = ev.target.getAttribute("id");
      this.followUp.start(ev.target, ev);
    }
  }

  onPointerMove(ev) {
    if (this.followUp.previewLine) this.followUp.preview(ev);
  }

  onPointerUp(ev) {
    if (this.followUp.previewLine) this.followUp.end();
    if (ev.target.tagName === "rect") {
      // connect if not connected already.
      this.pointerState.end = ev.target.getAttribute("id");
      if (!this.connector.hasConnection(this.pointerState.start, this.pointerState.end)) {
        // connect rect with line.
        const lineId = this.connector.connect(this.pointerState.start, this.pointerState.end, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });

        // update the join_items in selectedRectangle.
        const startRect = this.connector.svg.querySelector(`#${this.pointerState.start}`);
        const endRect = ev.target;
        const path = this.connector.connections.get(lineId).pathEl;
        this.joinItems.push({
          jtype: "Inner",
          from: [startRect.getAttribute("data-title")],
          to: [endRect.getAttribute("data-title")]
        });
        this.state.updateField("join_items", this.joinItems);

        // add listener to path element onclick side panel opens,
        // and on right click ask for confirmation to delete line.
        EventHandler.on(path, "click", () => {
          this.openPanel(startRect.getAttribute("data-title"), endRect.getAttribute("data-title"));
          this.shadowRoot.getElementById("deleteBtn").setAttribute("data-lineid", lineId);
          this.shadowRoot.getElementById("deleteBtn").setAttribute("data-start", this.pointerState.start);
          this.shadowRoot.getElementById("deleteBtn").setAttribute("data-end", this.pointerState.end);
        }, false, "Join");
        // reset and setDirty.
        this.setDirty();
      }
    }
    this.pointerState.reset();
  }

  onPointerDownOnSubPanel(ev) {
    if (ev.target.tagName === "rect") {
      this.pointerState.start = ev.target.getAttribute("id");
      this.subPanelFollowUp.start(ev.target, ev);
    }
  }

  onPointerMoveOnSubPanel(ev) {
    if (this.subPanelFollowUp.previewLine) this.subPanelFollowUp.preview(ev);
  }

  onPointerUpOnSubPanel(ev) {
    if (this.subPanelFollowUp.previewLine) this.subPanelFollowUp.end();
    if (ev.target.tagName === "rect") {
      this.pointerState.end = ev.target.getAttribute("id");
      if (!this.subPanelConnector.isOneOfThemConnected(this.pointerState.start, this.pointerState.end)) {
        const startRect = this.subPanelConnector.svg.querySelector(`#${this.pointerState.start}`);
        const endRect = this.subPanelConnector.svg.querySelector(`#${this.pointerState.end}`);

        if (startRect.dataset.table == endRect.dataset.table) return;

        const lineId = this.subPanelConnector.connect(this.pointerState.start, this.pointerState.end, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });

        for (const join of this.joinItems) {
          if (join.from[0] === startRect.getAttribute("data-table") && join.to[0] === endRect.getAttribute("data-table")) {
            join.from.push(startRect.getAttribute("data-title"));
            join.to.push(endRect.getAttribute("data-title"));
            this.panelState.updateField("from", join.from);
            this.panelState.updateField("to", join.to);
            break;
          }
        }

        const rectWidth = 15, rectHeight = 15, space = 10;
        const rect = this.createRectangle({
          title: "x",
          x: +endRect.getAttribute("x") + +endRect.getAttribute("width") + space,
          y: +endRect.getAttribute("y") + +endRect.getAttribute("height") / 2 - rectHeight / 2,
          rectWidth, rectHeight,
          isForPanel: true,
          danger: true
        });

        EventHandler.on(rect, "click", (e) => {
          e.preventDefault();
          const deleteLine = confirm("Do you want to delete this line?");
          if (deleteLine) {
            this.subPanelConnector.disconnect(lineId);
            for (const join of this.joinItems) {
              // find join data by table.
              if (join.from[0] === startRect.getAttribute("data-table") &&
                join.to[0] === endRect.getAttribute("data-table")) {
                // update join data (from and to).
                const fromIndex = join.from.indexOf(startRect.getAttribute("data-title"));
                const toIndex = join.to.indexOf(endRect.getAttribute("data-title"));
                join.from.splice(fromIndex, 1);
                join.to.splice(toIndex, 1);
                rect.remove();
                this.panelState.updateField("from", join.from);
                this.panelState.updateField("to", join.to);
                break;
              }
            }
            // set dirty on delete line.
            this.setDirty(true);
          }
        }, false, "Join");

        // set dirty on line draw.
        this.setDirty(true);
      } else {
        alert("One of the column already connected. if want to connect remove existing connection first.");
      }
    }
    this.pointerState.reset();
  }

  isInputValidate() {
    if (this.generalForm.querySelector("smart-input").value == "") {
      alert("Name is required");
      return false
    }
    return true
  }

  renderContent() {
    if (!this.selectedRectangle) return;
    this.connector.clear();
    if (this.connector.svg.children.length > 2) {
      this.renderContent();
      return;
    }
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

      this.createRectangle({ title: table, x, y, rectWidth, rectHeight });
    });

    this.drawExistingLines();
  }

  createRectangle({ table = null, title, x, y, rectWidth, rectHeight, isForPanel = false, danger = false }) {
    /**
     * Problem in using custom-web-component.
     * custom-web-component need one data raw of table.
     * here we do field to field connection.
     * hence not possible and no need.
     */
    // rectangle
    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", x);
    rect.setAttribute("y", y);
    rect.setAttribute("width", rectWidth);
    rect.setAttribute("height", rectHeight);
    rect.setAttribute("data-title", title);
    if (danger) rect.classList.add("danger");
    if (table) rect.setAttribute("data-table", table);
    isForPanel ? this.subPanelConnector.svg.appendChild(rect) : this.connector.svg.appendChild(rect);

    // title
    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", x + rectWidth / 2);
    text.setAttribute("y", y + rectHeight / 2 + 4);
    text.setAttribute("text-anchor", "middle");
    text.textContent = title;
    isForPanel ? this.subPanelConnector.svg.appendChild(text) : this.connector.svg.appendChild(text);

    const id = isForPanel ? this.subPanelConnector.addNode(rect) : this.connector.addNode(rect);
    rect.setAttribute("id", id);
    return rect;
  }

  openPanel(table1, table2) {
    this.sidePanel.classList.add("open");
    if (this.activeTables.length > 0 && this.activeTables[0] == table1 && this.activeTables[1] == table2) return;
    this.resetPanel();
    this.activeTables = [table1, table2];

    const rectWidth = 120, rectHeight = 40;
    let x = 10, y = 10;

    this.selectedRectangle.items[table1].forEach((item) => {
      this.createRectangle({ table: table1, title: item['business_name'], x, y, rectWidth, rectHeight, isForPanel: true });
      y += rectHeight + 10;
    });

    x += rectWidth + 30;
    y = 10;

    this.selectedRectangle.items[table2].forEach((item) => {
      this.createRectangle({ table: table2, title: item['business_name'], x, y, rectWidth, rectHeight, isForPanel: true });
      y += rectHeight + 10;
    });

    if (this.selectedRectangle?.join_items.length > 0) {
      this.jTypeDropDown.selectedIndex = Array.from(this.jTypeDropDown.children).findIndex(c => c.innerText === this.selectedRectangle.join_items[0].jtype);
      this.panelState = new StateManager({
        jtype: this.jTypeDropDown.selectedOptions[0].innerText,
        from: [...this.joinItems.find(f => f.from[0] === table1 && f.to[0] === table2).from],
        to: [...this.joinItems.find(f => f.from[0] === table1 && f.to[0] === table2).to]
      })
      this.drawExistingLines(true);
    }
  }

  savePanel() {
    this.panelState.commit();
    this.state.updateField("join_items", this.joinItems);
    this.save();
    this.reset(true);
  }

  closePanel() {
    if (this.panelState.isDirty()) {
      const wantToSave = confirm("Looks like you’ve made changes. Would you like to save them before leaving this panel?");
      if (!wantToSave) this.sidePanel.classList.remove("open");
      return;
    }
    this.sidePanel.classList.remove("open");
  }

  resetPanel() {
    this.subPanelConnector.clear();
    if (this.subPanelConnector.svg.children.length > 2) {
      this.resetPanel();
      return;
    }
    this.shadowRoot.querySelector(".side-panel select").selectedIndex = 0;
    this.reset(true);
  }

  updateSelectedRectangle() {
    // console.log(window.data.selectedRectangle);
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id == window.data.selectedRectangle
    );
    this.joinItems = this.selectedRectangle.join_items;
  }

  drawExistingLines(isForPanel = false) {
    if (!this.selectedRectangle) return;

    this.joinItems.forEach((joins, index) => {
      // check if join data is for active tables.
      if (isForPanel && this.activeTables[0] == joins.from[0] && this.activeTables[1] == joins.to[0]) {
        // connect columns.
        for (let i = 1; i < joins.from.length; i++) {
          const from = this.subPanelContainer.querySelector(`rect[data-title="${joins.from[i]}"][data-table=${joins.from[0]}]`);
          const to = this.subPanelContainer.querySelector(`rect[data-title="${joins.to[i]}"][data-table=${joins.to[0]}]`);
          const fromId = from.getAttribute("id");
          const toId = to.getAttribute("id");
          if (!this.subPanelConnector.hasConnection(fromId, toId)) {
            const lineId = this.subPanelConnector.connect(fromId, toId, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 })

            const rectWidth = 15, rectHeight = 15, space = 10;
            const rect = this.createRectangle({
              title: "x",
              x: +to.getAttribute("x") + +to.getAttribute("width") + space,
              y: +to.getAttribute("y") + +to.getAttribute("height") / 2 - rectHeight / 2,
              rectWidth, rectHeight,
              isForPanel: true,
              danger: true
            });

            EventHandler.on(rect, "click", (e) => {
              e.preventDefault();
              const deleteLine = confirm("Do you want to delete this line?");
              if (deleteLine) {
                this.subPanelConnector.disconnect(lineId);
                for (const join of this.joinItems) {
                  // find join data by table.
                  if (join.from[0] === from.getAttribute("data-table") &&
                    join.to[0] === to.getAttribute("data-table")) {
                    // update join data (from and to).
                    const fromIndex = join.from.indexOf(from.getAttribute("data-title"));
                    const toIndex = join.to.indexOf(to.getAttribute("data-title"));
                    join.from.splice(fromIndex, 1);
                    join.to.splice(toIndex, 1);
                    this.panelState.updateField("from", join.from);
                    this.panelState.updateField("to", join.to);
                    rect.remove();
                    break;
                  }
                }
                // set dirty on delete line.
                this.setDirty(true);
              }
            }, false, "Join");
          }
        }
      } else {
        // connect tables.
        const from = this.container.querySelector(`rect[data-title="${joins.from[0]}"]`);
        const to = this.container.querySelector(`rect[data-title="${joins.to[0]}"]`);
        const fromId = from.getAttribute("id");
        const toId = to.getAttribute("id");
        if (!this.connector.hasConnection(fromId, toId)) {
          const lineId = this.connector.connect(fromId, toId, { arrow: false, stroke: '#2b6cb0', strokeWidth: 2 });
          const path = this.connector.connections.get(lineId).pathEl;
          EventHandler.on(path, "click", () => {
            this.openPanel(from.getAttribute("data-title"), to.getAttribute("data-title"));
            this.shadowRoot.getElementById("deleteBtn").setAttribute("data-lineid", lineId);
            this.shadowRoot.getElementById("deleteBtn").setAttribute("data-start", fromId);
            this.shadowRoot.getElementById("deleteBtn").setAttribute("data-end", toId);
          }, false, "Join");
        }
      }
    })
  }
}

customElements.define("join-editor-component", JoinEditorComponent);
