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

  async handleClick() {
    window.data.selectedRectangle = this.getAttribute("alias") || "";

    const existingRectangle = window.data.rectangles?.find(
      (rect) => rect.alias === this.getAttribute("alias")
    );

    if (!existingRectangle) {
      try {
        this.style.cursor = "wait";

         const response = await fetch(
           `/getItem2?id=${this.getAttribute("alias")}`,
           {
             method: "POST",
             headers: {
               "Content-Type": "application/json",
             },
           }
         );

         if (!response.ok) {
           throw new Error(`HTTP error! status: ${response.status}`);
         }

         const rectangleDetails = await response.json();
/*
        const rectangleDetails = {
          alias: "accounts",
          parent_object: "Table_view",
          business_name: "Table 1 business name",
          technical_name: "Table 1 technical name",
          items: [
            {
              business_name: "Item ID",
              technical_name: "Item",
              semantic_type: "String",
              data_type: "long",
              key: "yes",
              column_type: "column",
            },
            {
              business_name: "Product ID",
              technical_name: "Product",
              semantic_type: "String",
              data_type: "long",
              key: "no",
              column_type: "column",
            },
            {
              business_name: "Unit",
              technical_name: "Unit",
              semantic_type: "String",
              data_type: "string",
              key: "no",
              column_type: "column",
            },
            {
              business_name: "Quantity",
              technical_name: "Quantity",
              semantic_type: "Number",
              data_type: "float",
              key: "no",
              column_type: "column",
            },
          ],
        };
*/
        if (!window.data.rectangles) {
          window.data.rectangles = [];
        }

        window.data.rectangles.push(rectangleDetails);
      } catch (error) {
        console.error(error);
      } finally {
        this.style.cursor = "pointer";
      }
    }

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
