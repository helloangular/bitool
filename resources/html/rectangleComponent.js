const template = document.createElement("template");
template.innerHTML = `
<style>
  .rectangle-container {
    padding: 10px;
    border: 3px solid #004466;
    border-radius: 6px;
    cursor: pointer;
  }
</style>

<div class="rectangle-container">
  <div id="alias-info"></div>
</div>
`;

class RectangleComponent extends HTMLElement {
  constructor() {
    super();

    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.shadow.addEventListener("click", () => {
      this.handleClick();
    });

    this.updateAlias(this.getAttribute("alias") || "");
    this.container = this.shadow.querySelector(".rectangle-container");
  }

  static get observedAttributes() {
    return ["alias", "style"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "alias") {
      this.updateAlias(newValue);
    }
  }

  handleClick() {
    window.data.selectedRectangle = this.getAttribute("alias") || "";
    const columnListComponent = document.querySelector("column-list-component");
    if (columnListComponent) {
      columnListComponent.setAttribute("visibility", "open");
    }
  }

  updateAlias(alias) {
    this.shadow.querySelector("#alias-info").textContent = alias;
  }
}

customElements.define("rectangle-component", RectangleComponent);
