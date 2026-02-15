import EventHandler from "./eventHandler.js";
export default class SortableList {
  constructor(container, eventGroup) {
    this.container = container;
    this.eventGroup = eventGroup;
    this.draggedItem = null;
    this.placeholder = document.createElement("div");
    this.placeholder.classList.add("placeholder-line");

    this.init();
  }

  init() {
    this.container.querySelectorAll(".sortable-item").forEach(item => {
      item.setAttribute("draggable", true);

      EventHandler.on(item, "dragstart", () => {
        this.draggedItem = item;
        item.classList.add("dragging");
        setTimeout(() => item.style.display = "none", 0);
      }, false, this.eventGroup);

      EventHandler.on(item, "dragend", () => {
        item.classList.remove("dragging");
        item.style.display = "block";
        this.placeholder.remove();
      }, false, this.eventGroup);
    });

    EventHandler.on(this.container, "dragover", (e) => {
      e.preventDefault();
      const afterElement = this.getDragAfterElement(e.clientY);
      if (afterElement == null && this.draggedItem) {
        this.container.appendChild(this.placeholder);
      } else if (this.draggedItem) {
        this.container.insertBefore(this.placeholder, afterElement);
      }
    }, false, this.eventGroup);

    EventHandler.on(this.container, "drop", () => {
      if (!this.draggedItem) return;
      this.container.insertBefore(this.draggedItem, this.placeholder);
      this.placeholder.remove();
      dispatchEvent(new Event("sortablelist:changed"));
    }, false, this.eventGroup);
  }

  getDragAfterElement(y) {
    const elements = [...this.container.querySelectorAll(".sortable-item:not(.dragging)")];
    return elements.reduce((closest, child) => {
      const box = child.getBoundingClientRect();
      const offset = y - box.top - box.height / 2;
      if (offset < 0 && offset > closest.offset) {
        return { offset: offset, element: child };
      } else {
        return closest;
      }
    }, { offset: Number.NEGATIVE_INFINITY }).element;
  }
}