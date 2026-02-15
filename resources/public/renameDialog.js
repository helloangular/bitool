import "./source/modules/smart.window.js";
import "./source/modules/smart.button.js";
import "./source/modules/smart.input.js";
import EventHandler from "./library/eventHandler.js";

class RenameDialog extends HTMLElement {
  constructor() {
    super();
    // console.log("RenameDialog Initialize");
    this.dialog = document.getElementById("renameDialog");
    this.businessNameInput = document.getElementById("businessNameInput");
    this.technicalNameInput = document.getElementById("technicalNameInput");
    this.renameButton = document.getElementById("renameButton");
    this.cancelButton = document.getElementById("cancelButton");

    EventHandler.on(this.renameButton, "click", () => {
      // const detail = {
      //  oldBusinessName: this.getAttribute("business-name"),
      //  oldTechnicalName: this.getAttribute("technical-name"),
      //  newBusinessName: this.businessNameInput.value,
      //  newTechnicalName: this.technicalNameInput.value,
      // };
      
      const detail = {
        alias: this.technicalNameInput.value,
        businessName: this.getAttribute("business-name"),
      }

      this.dispatchEvent(new CustomEvent("rename", { detail }));
      this.setAttribute("visibility", "");
    });

    EventHandler.on(this.cancelButton, "click", () => {
      this.setAttribute("visibility", "");
    });
  }

  static get observedAttributes() {
    return ["visibility", "business-name", "technical-name"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      if (newValue === "open") {
        this.dialog.open();
        this.businessNameInput.value = this.getAttribute("business-name") || "";
        this.technicalNameInput.value =
          this.getAttribute("technical-name") || "";
      } else {
        this.dialog.close();
      }
    }
  }
}

customElements.define("rename-dialog", RenameDialog);
