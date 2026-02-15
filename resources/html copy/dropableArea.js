class DroppableArea extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });

    this.shadowRoot.innerHTML = `
      <div class="drop-area">
        Drop tables or views here
      </div>
    `;

    // Add drag and drop functionality (similar to previous implementation)
  }
}

customElements.define("droppable-area", DroppableArea);
