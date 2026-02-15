import "./source/modules/smart.tree.js";
import "./source/modules/smart.window.js";
import "./source/modules/smart.input.js";
import "./source/modules/smart.button.js";
import "./source/modules/smart.dropdownlist.js";

class TreeComponent extends HTMLElement {
  constructor() {
    super();

    this.attachShadow({ mode: "open" });
    this.tree = null;
    this.dialog = null;
  }

  connectedCallback() {
    this.loadTemplate();
  }

  async loadTemplate() {
    try {
      const response = await fetch("TreeComponent.html");
      const templateHTML = await response.text();

      const template = document.createElement("template");
      template.innerHTML = templateHTML;

      this.render(template);
    } catch (error) {
      console.error("Error loading template:", error);
    }
  }

  render(template) {
    try {
      const content = template.content.cloneNode(true);

      const form = content.getElementById("profileForm");

      this.dialog = content.querySelector(".dialog");
      this.tree = content.querySelector("smart-tree");
      this.renderItems();

      form.addEventListener("submit", (event) => {
        event.preventDefault();

        this.createConnection(this.item);
      });

      const droppableArea = content.querySelector(".dropable-area");

      const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
          if (
            mutation.type === "attributes" &&
            mutation.attributeName === "visibility"
          ) {
            const columnList = document.querySelector("column-list-component");
            if (columnList.getAttribute("visibility") === "open") {
              droppableArea.classList.add("column-list-open");
            } else {
              droppableArea.classList.remove("column-list-open");
            }
          }
        });
      });

      const columnList = document.querySelector("column-list-component");
      if (columnList) {
        observer.observe(columnList, { attributes: true });
      }

      droppableArea.addEventListener("dragover", (event) => {
        event.preventDefault();
        droppableArea.style.borderColor = "#666";
      });

      droppableArea.addEventListener("dragleave", () => {
        droppableArea.style.borderColor = "#ccc";
      });

      droppableArea.addEventListener("drop", (event) => {
        event.preventDefault();

        if (event.target.tagName === "RECTANGLE-COMPONENT") {
          this.shadowRoot.getElementById("table1").value =
            event.dataTransfer.getData("Text");
          this.shadowRoot.getElementById("table2").value =
            event.target.getAttribute("alias");
          this.shadowRoot.getElementById("addtable").submit();
        }

        droppableArea.style.borderColor = "#ccc";

        const rect = droppableArea.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;

        const scrollX = droppableArea.scrollLeft;
        const scrollY = droppableArea.scrollTop;

        const rectangle = document.createElement("rectangle-component");
        rectangle.style.position = "absolute";
        rectangle.style.left = `${x + scrollX}px`;
        rectangle.style.top = `${y + scrollY}px`;

        droppableArea.appendChild(rectangle);
        rectangle.setAttribute(
          "alias",
          event.dataTransfer.getData("text/plain")
        );

        if (!droppableArea.classList.contains("has-items")) {
          droppableArea.classList.add("has-items");
        }
      });

      this.shadowRoot.appendChild(content);
    } catch (error) {
      console.error("Error rendering template:", error);
    }
  }

  handleDragStart(event) {
    const draggedItem = event.target;
    event.dataTransfer.setData("text/plain", draggedItem.textContent);
  }

  showDialog(item, dialog) {
    dialog.open();
    this.item = item;
  }

  renderItems() {
    if (!this.tree) return;

    this.tree.dataSource = window.data.treeItems;

    setTimeout(() => {
      const items = this.tree.querySelectorAll("smart-tree-item");

      items.forEach((element) => {
        const level = element.path.split(".").length - 1;

        if (level === 0 || level === 1) {
          element.addEventListener("contextmenu", (event) => {
            event.preventDefault();
            event.stopPropagation();

            if (this.dialog) {
              this.showDialog(element, this.dialog);
            }
          });
        } else {
          element.setAttribute("draggable", "true");
          element.addEventListener(
            "dragstart",
            this.handleDragStart.bind(this)
          );
        }
      });
    }, 100);
  }

  mapItems(item, treeItems, data) {
    return treeItems.map((treeItem) => {
      if (treeItem.label === item.label) {
        if (!treeItem.items) treeItem.items = [];

        return {
          ...treeItem,
          items: [...treeItem.items, data],
        };
      } else if (treeItem.items && treeItem.items.length > 0) {
        return {
          ...treeItem,
          items: this.mapItems(item, treeItem.items, data),
        };
      }

      return treeItem;
    });
  }

  async createConnection(item) {
    const formData = new FormData(this.dialog.querySelector("form"));
    const connectionName = formData.get("connectionName");
    const username = formData.get("username");
    const password = formData.get("password");
    const host = formData.get("host");
    const sid = formData.get("sid");
    const serviceName = formData.get("serviceName");

    try {
      const response = await fetch("/api/connection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          connectionName,
          username,
          password,
          host,
          sid,
          serviceName,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();

      window.data.treeItems = this.mapItems(item, window.data.treeItems, data);

      this.renderItems();

      this.dialog.close();
    } catch (error) {
      console.error("Error creating connection:", error);
    }
  }
}
customElements.define("tree-component", TreeComponent);
