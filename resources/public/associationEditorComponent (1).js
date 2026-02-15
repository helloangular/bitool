import "./source/modules/smart.listbox.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.tooltip.js";
import "./source/modules/smart.table.js";
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
  <div style="display:flex;justify-content:flex-end">
    <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;padding:6px;"></smart-button>
  </div>
  <div style="padding:10px" id="generalForm">
    <label> 
      Business name: 
    </label>
    <smart-input class="outlined" name="businessName" style="width:100%"></smart-input>
    <label>
      Technical name: 
    </label>
    <smart-input class="outlined" name="technicalName" style="width:100%"></smart-input>
    <label>
      From:
    </label>
    <smart-input class="outlined" name="from" style="width:100%" disabled></smart-input>
    <label>
      To:
    </label>
    <smart-input class="outlined" name="to" style="width:100%" disabled></smart-input>
  </div>
  <div style="width:100%;margin-top:10px;"><span style="margin-left:10px;font-size:16px;">Mappings</span></div>
  <div style="display:flex;gap:40px;position:relative;width:100%;">
    <div style="display:flex;flex-direction:column;width:50%" id="fromZone"></div>
    <div style="display:flex;flex-direction:column;width:50%" id="toZone"></div>
  </div>
</div>
`;

class AssociationEditorComponent extends HTMLElement {
  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));
    this.cacheElements();
    this.setUpEventListeners();
    this.tempAssociations = [...window.data.associations];
    this.selectedRectangle;
    this.selectedAssociation;

    this.updateVisibility();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue == "open") {
        this.updateSelectedRectangleAndAssociation();
        this.renderContent();
      } else {
        this.removeLines();
      }
      this.updateVisibility();
    }
  }

  cacheElements() {
    this.closeButton = this.shadow.querySelector(".smart-button");
    this.fromList = this.shadow.querySelector("#fromZone");
    this.toList = this.shadow.querySelector("#toZone");
    this.line = document.getElementById("line");
    this.svg = document.querySelector("svg");
    this.saveButton = this.shadow.querySelector("#saveButton");
    this.columnListComponent = document.querySelector("column-list-component");
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", this.handleCloseButtonClick.bind(this));
    EventHandler.on(this.saveButton, "click", this.handleSaveButtonClick.bind(this));
    EventHandler.on(document, "mousemove", this.handleMouseMove.bind(this));
    EventHandler.on(document, "mouseup", this.handleMouseUp.bind(this));
  }

  handleCloseButtonClick() {
    this.setAttribute("visibility", "");
  }

  handleSaveButtonClick() {
    window.data.associations = this.tempAssociations;
    this.setAttribute("visibility", "");
    // this.columnListComponent.setAttribute("visibility", "open");
  }

  handleMouseMove(e) {
    if (this.isDragging) {
      this.line.setAttribute("x2", e.clientX);
      this.line.setAttribute("y2", e.clientY);
    }
  }

  handleMouseUp(e) {
    if (this.isDragging) {
      this.isDragging = false;

      const targetBox = this.shadow.elementFromPoint(e.clientX, e.clientY);
      const parent = targetBox.parentElement;
      if (
        targetBox &&
        targetBox.tagName.toLowerCase() === "custom-web-component" &&
        targetBox !== this.startDiv
      ) {
        const endRect = targetBox.getBoundingClientRect();
        let endX = 0;
        let endY = 0;
        if (parent.id === "fromZone") {
          endX = endRect.right;
        }

        if (parent.id === "toZone") {
          endX = endRect.left;
        }

        endY = endRect.top + endRect.height / 2;

        const associated = this.addAssociation(this.startDiv, targetBox);

        if (associated) {
          const newLine = document.createElementNS(
            "http://www.w3.org/2000/svg",
            "line"
          );

          newLine.setAttribute("x1", this.line.getAttribute("x1"));
          newLine.setAttribute("y1", this.line.getAttribute("y1"));
          newLine.setAttribute("x2", endX);
          newLine.setAttribute("y2", endY);
          newLine.setAttribute("stroke", "lightgray");
          newLine.setAttribute("stroke-width", 2);

          this.svg.appendChild(newLine);
        }
      }

      this.line.style.visibility = "hidden";
    }
  }

  updateVisibility() {
    this.style.display =
      this.getAttribute("visibility") == "open" ? "block" : "none";
  }

  removeLines() {
    const lines = this.svg.querySelectorAll("line");
    lines.forEach((line) => {
      if (line.id === "line") return;

      line.remove();
    });
  }

  renderContent() {
    this.clearContent(this.fromList);
    this.clearContent(this.toList);

    this.tempAssociations = [...window.data.associations];

    const businessNameInput = this.shadow.querySelector(
      "smart-input[name='businessName']"
    );
    const technicalNameInput = this.shadow.querySelector(
      "smart-input[name='technicalName']"
    );
    const fromInput = this.shadow.querySelector("smart-input[name='from']");
    const toInput = this.shadow.querySelector("smart-input[name='to']");
    businessNameInput.value = `${this.selectedRectangle.alias} to ${this.selectedAssociation.alias}`;
    technicalNameInput.value = `${this.selectedRectangle.alias} to ${this.selectedAssociation.alias}`;
    fromInput.value = this.selectedRectangle.alias;
    toInput.value = this.selectedAssociation.alias;

    this.populateList(this.selectedRectangle.items, this.fromList, "fromZone");
    this.populateList(this.selectedAssociation.items, this.toList, "toZone");

    this.setUpDragEvents();

    setTimeout(() => {
      this.drawExistingLines();
    }, 100);
  }

  clearContent(container) {
    const content = container.querySelectorAll("custom-web-component");
    content.forEach((item) => {
      item.remove();
    });
  }

  populateList(items, container, zoneId) {
    items.forEach((item) => {
      if (item.column_type !== "attribute" && zoneId === "fromZone") return;

      const component = document.createElement("custom-web-component");
      component.setAttribute("data_type", item.data_type);
      component.setAttribute("business_name", item.business_name);
      component.setAttribute("technical_name", item.technical_name);
      component.setAttribute("info", "no");
      component.style.margin = zoneId === "fromZone" ? "0 0 0 5px" : "0 5px 0 0";
      component.setAttribute("padding", "4px");
      container.appendChild(component);
    });
  }

  setUpDragEvents() {
    const components = this.shadow.querySelectorAll("custom-web-component");

    components.forEach((box) => {
      const parent = box.parentElement;

     EventHandler.on(box, "mousedown", (e) => {
        this.isDragging = true;
        this.startDiv = box;

        const rect = box.getBoundingClientRect();
        let startX = 0;
        let startY = 0;
        if (parent.id === "fromZone") {
          startX = rect.right;
        }

        if (parent.id === "toZone") {
          startX = rect.left;
        }

        startY = rect.top + rect.height / 2;

        this.line.setAttribute("x1", startX);
        this.line.setAttribute("y1", startY);
        this.line.setAttribute("x2", startX);
        this.line.setAttribute("y2", startY);
        this.line.style.visibility = "visible";
      });
    });
  }

  updateSelectedRectangleAndAssociation() {
    this.selectedRectangle = window.data.rectangles.find(
      (rectangle) => rectangle.id == window.data.selectedRectangle
    );
    this.selectedAssociation = window.data.rectangles.find(
      (rectangle) => rectangle.alias == window.data.selectedAssociation
    );
    // Assigning default value if association is null or empty for debuging purpose.
    if(this.selectedAssociation || this.selectedAssociation?.length > 0) this.selectedAssociation = window.data.rectangles[2]
  }

  addAssociation(startItem, endItem) {
    const association = this.tempAssociations.find(
      (association) =>
        (association.target1 == this.selectedRectangle.alias &&
          association.target2 == this.selectedAssociation.alias) ||
        (association.target2 == this.selectedRectangle.alias &&
          association.target1 == this.selectedAssociation.alias)
    );

    const added = association ? false : true;

    const newAssociation = association
      ? JSON.parse(JSON.stringify(association))
      : {
        target1: this.selectedRectangle.alias,
        target2: this.selectedAssociation.alias,
        items: [],
      };

    if (
      newAssociation.items.find((item) => {
        if (newAssociation.target1 == this.selectedRectangle.alias) {
          return (
            item.target1 === startItem.getAttribute("business_name") &&
            item.target2 === endItem.getAttribute("business_name")
          );
        }

        return (
          item.target2 === startItem.getAttribute("business_name") &&
          item.target1 === endItem.getAttribute("business_name")
        );
      })
    ) {
      return false;
    }

    newAssociation.items.push({
      target1:
        newAssociation.target1 == this.selectedRectangle.alias
          ? startItem.getAttribute("business_name")
          : endItem.getAttribute("business_name"),
      target2:
        newAssociation.target1 == this.selectedRectangle.alias
          ? endItem.getAttribute("business_name")
          : startItem.getAttribute("business_name"),
    });

    if (added) {
      this.tempAssociations = [...this.tempAssociations, newAssociation];
    } else {
      this.tempAssociations = this.tempAssociations.map((association) => {
        if (
          (association.target1 == this.selectedRectangle.alias &&
            association.target2 == this.selectedAssociation.alias) ||
          (association.target2 == this.selectedRectangle.alias &&
            association.target1 == this.selectedAssociation.alias)
        )
          return newAssociation;

        return association;
      });
    }

    return true;
  }

  drawExistingLines() {
    const association = window.data.associations.find((association) => {
      return (
        (association.target1 == this.selectedRectangle.alias &&
          association.target2 == this.selectedAssociation.alias) ||
        (association.target1 == this.selectedAssociation.alias &&
          association.target2 == this.selectedRectangle.alias)
      );
    });

    if (!association) return;

    const components = Array.from(
      this.shadow.querySelectorAll("custom-web-component")
    );

    const componentCouples = association.items.map((item) => {
      if (association.target1 == this.selectedRectangle.alias)
        return [
          components.find(
            (component) =>
              component.parentElement.id === "fromZone" &&
              component.getAttribute("business_name") == item.target1
          ),
          components.find(
            (component) =>
              component.parentElement.id === "toZone" &&
              component.getAttribute("business_name") == item.target2
          ),
        ];
      if (association.target2 == this.selectedRectangle.alias)
        return [
          components.find(
            (component) =>
              component.parentElement.id === "fromZone" &&
              component.getAttribute("business_name") == item.target2
          ),
          components.find(
            (component) =>
              component.parentElement.id === "toZone" &&
              component.getAttribute("business_name") == item.target1
          ),
        ];
    });

    componentCouples.forEach(([fromComponent, toComponent]) => {
      const fromRect = fromComponent.getBoundingClientRect();
      const toRect = toComponent.getBoundingClientRect();

      const newLine = document.createElementNS(
        "http://www.w3.org/2000/svg",
        "line"
      );

      newLine.setAttribute("x1", fromRect.right);
      newLine.setAttribute("y1", fromRect.top + fromRect.height / 2);
      newLine.setAttribute("x2", toRect.left);
      newLine.setAttribute("y2", toRect.top + toRect.height / 2);
      newLine.setAttribute("stroke", "lightgray");
      newLine.setAttribute("stroke-width", 2);

      this.svg.appendChild(newLine);
    });
  }
}

customElements.define(
  "association-editor-component",
  AssociationEditorComponent
);
