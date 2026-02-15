import "./source/modules/smart.listbox.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.tooltip.js";
import "./source/modules/smart.table.js";
import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";

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
    <smart-input class="outlined" name="from" style="width:100%"></smart-input>
    <label>
      To:
    </label>
    <smart-input class="outlined" name="to" style="width:100%"></smart-input>
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

    // Initialize StateManager with the relevant data fields for the editor
    this.stateManager = new StateManager({
      // We will initialize this fully in attributeChangedCallback when 'open'
      associations: []
    });

    this.selectedRectangle = null;
    this.selectedAssociation = null;

    this.updateVisibility();
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue == "open") {
        // 1. Initialize/Reset StateManager with the global data when opening
        this.stateManager = new StateManager({
          associations: [...window.data.associations]
        });

        this.updateSelectedRectangleAndAssociation();
        this.renderContent();
        // 2. Set save button state based on clean state
        this.saveButton.disabled = !this.stateManager.isDirty();
        this.trackInputChanges();

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

    // Cache inputs to manage their change events and state
    this.businessNameInput = this.shadow.querySelector("smart-input[name='businessName']");
    this.technicalNameInput = this.shadow.querySelector("smart-input[name='technicalName']");
    this.fromInput = this.shadow.querySelector("smart-input[name='from']");
    this.toInput = this.shadow.querySelector("smart-input[name='to']");
  }

  setUpEventListeners() {
    EventHandler.on(this.closeButton, "click", this.handleCloseButtonClick.bind(this), false, "Association");
    EventHandler.on(this.saveButton, "click", this.handleSaveButtonClick.bind(this), false, "Association");
    EventHandler.on(document, "mousemove", this.handleMouseMove.bind(this), false, "Association");
    EventHandler.on(document, "mouseup", this.handleMouseUp.bind(this), false, "Association");
  }

  trackInputChanges() {
    // Also track the simple text inputs for dirty state management
    // In a full implementation, these values would be added to the StateManager's object
    // For this demonstration, we'll just check if the form values have changed from the initial calculated values

    const inputs = [this.businessNameInput, this.technicalNameInput];
    const initialBusinessName = `${this.selectedRectangle.alias} to ${this.selectedAssociation.alias}`;
    const initialTechnicalName = `${this.selectedRectangle.alias} to ${this.selectedAssociation.alias}`;

    // Helper function to check dirty state across inputs AND associations
    const checkDirtyState = () => {
      const inputsDirty =
        this.businessNameInput.value !== initialBusinessName ||
        this.technicalNameInput.value !== initialTechnicalName;

      const associationsDirty = this.stateManager.isDirty();

      this.saveButton.disabled = !(inputsDirty || associationsDirty);
    };

    inputs.forEach(el => {
      // Check to prevent adding multiple listeners
      if (!el.__hasChangeListener) {
        // Use 'change' event for smart-input
        EventHandler.on(el, "change", checkDirtyState, false, "Association");
        el.__hasChangeListener = true;
      }
    });

    // We can also call this now to set the initial button state
    checkDirtyState();
  }

  handleCloseButtonClick() {
    if (!this.saveButton.disabled) { // If the save button is enabled, state is dirty
      const wantToSave = confirm("Looks like you’ve made changes. Would you like to save them before leaving this panel?");
      if (!wantToSave) {
        // If the user chooses NOT to save, we simply close and the stateManager's current state is discarded 
        // because it will be reset on the next 'open'.
        this.setAttribute("visibility", "close");
      }
      return; // prevent closing until confirmed
    }
    this.setAttribute("visibility", "close");
  }

  handleSaveButtonClick() {
    // 1. Update the global data with the associations from the StateManager's current state
    window.data.associations = this.stateManager.current.associations;

    // 2. Commit the current state to be the new initial state (making it clean)
    this.stateManager.commit();

    this.setAttribute("visibility", "");
    this.saveButton.disabled = true; // Button is disabled after commit
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
      this.line.style.visibility = "hidden"; // Hide the line immediately

      const targetBox = this.shadow.elementFromPoint(e.clientX, e.clientY);
      const parent = targetBox ? targetBox.parentElement : null;

      if (
        targetBox &&
        targetBox.tagName.toLowerCase() === "custom-web-component" &&
        targetBox !== this.startDiv
      ) {
        const endRect = targetBox.getBoundingClientRect();
        let endX = 0;
        let endY = 0;

        if (parent && parent.id === "fromZone") {
          endX = endRect.right;
        } else if (parent && parent.id === "toZone") {
          endX = endRect.left;
        } else {
          return; // Target is not in a valid zone
        }

        endY = endRect.top + endRect.height / 2;

        const associated = this.addAssociation(this.startDiv, targetBox);

        if (associated) {
          // Draw the permanent line
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

          // Update save button state via StateManager
          this.saveButton.disabled = !this.stateManager.isDirty();
        }
      }
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

    // Populate general form inputs
    this.businessNameInput.value = `${this.selectedRectangle.alias} to ${this.selectedAssociation.alias}`;
    this.technicalNameInput.value = `${this.selectedRectangle.alias} to ${this.selectedAssociation.alias}`;
    this.fromInput.value = this.selectedRectangle.alias;
    this.toInput.value = this.selectedAssociation.alias;

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
      // Logic for filtering columns in the 'from' zone
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
    if (!this.selectedAssociation || this.selectedAssociation?.length === 0) this.selectedAssociation = window.data.rectangles[2]
  }

  addAssociation(startItem, endItem) {
    // Get the current associations from the StateManager
    const currentAssociations = this.stateManager.current.associations;

    // Find the association in the current state
    const association = currentAssociations.find(
      (association) =>
        (association.target1 == this.selectedRectangle.alias &&
          association.target2 == this.selectedAssociation.alias) ||
        (association.target2 == this.selectedRectangle.alias &&
          association.target1 == this.selectedAssociation.alias)
    );

    let newAssociation;
    let associationIndex = -1;
    let added = false;

    if (association) {
      // Deep copy the existing association
      newAssociation = JSON.parse(JSON.stringify(association));
      associationIndex = currentAssociations.indexOf(association);
    } else {
      // Create a brand new association object
      newAssociation = {
        target1: this.selectedRectangle.alias,
        target2: this.selectedAssociation.alias,
        items: [],
      };
      added = true;
    }

    // Check if the specific item mapping already exists
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
      return false; // Mapping already exists, don't add
    }

    // Add the new item mapping
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

    // Create the updated associations array
    let updatedAssociations;
    if (added) {
      updatedAssociations = [...currentAssociations, newAssociation];
    } else {
      updatedAssociations = currentAssociations.map((assoc, index) => {
        if (index === associationIndex) {
          return newAssociation;
        }
        return assoc;
      });
    }

    // Update the state manager
    // We use the 'assign' type, explicitly passing the full updated array
    this.stateManager.updateField('associations', updatedAssociations, { type: 'assign' });

    return true; // Association successfully added/updated
  }

  drawExistingLines() {
    // We draw lines based on the CURRENT state (which is a fresh copy of global data when opened)
    const association = this.stateManager.current.associations.find((association) => {
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
    }).filter(couple => couple && couple[0] && couple[1]);

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