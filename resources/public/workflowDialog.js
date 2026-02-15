class WorkflowDialog extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });

    this.shadowRoot.innerHTML = `
      <style>
        :host {
          display: none;
          position: fixed;
          inset: 0;
          background: rgba(0,0,0,0.45);
          justify-content: center;
          align-items: center;
          font-family: Arial, sans-serif;
        }

        .dialog {
          width: 320px;
          padding: 20px;
          border: 2px solid #000;
          background: #fff;
        }

        .dialog-header {
          font-weight: bold;
          margin-bottom: 20px;
          border-bottom: 2px solid #000;
          padding-bottom: 4px;
        }

        .form-group {
          display: flex;
          flex-direction: column;
          margin-bottom: 18px;
          gap: 6px;
        }

        .form-group label {
          font-size: 14px;
          font-weight: 600;
        }

        input,
        select {
          padding: 6px;
          border: 1px solid #000;
          border-radius: 3px;
          font-size: 14px;
        }

        .actions {
          display: flex;
          gap: 10px;
          margin-top: 10px;
        }

        button {
          padding: 6px 16px;
          border: 2px solid #000;
          background: #f5f5f5;
          cursor: pointer;
          font-weight: 600;
        }

        button:hover {
          background: #e0e0e0;
        }

        .save {
          background: #d9ffd9;
        }

        .cancel {
          background: #ffd9d9;
        }
      </style>

      <div class="dialog">
        <div class="dialog-header">Workflow</div>

        <div class="form-group">
          <label>Name</label>
          <input type="text" id="nameInput" />
        </div>

        <div class="form-group">
          <label>Default Connection</label>
          <select id="connectionSelect"></select>
        </div>

        <div class="actions">
          <button class="save" id="saveBtn">Save</button>
          <button class="cancel" id="cancelBtn">Cancel</button>
        </div>
      </div>
    `;
  }

  connectedCallback() {
    this.nameInput = this.shadowRoot.getElementById("nameInput");
    this.connectionSelect = this.shadowRoot.getElementById("connectionSelect");

    this.shadowRoot.getElementById("saveBtn").addEventListener("click", () => {
      this.dispatchEvent(
        new CustomEvent("save", {
          detail: {
            name: this.nameInput.value,
            connection: this.connectionSelect.value
          }
        })
      );
      this.close();
    });

    this.shadowRoot.getElementById("cancelBtn").addEventListener("click", () => {
      this.dispatchEvent(new CustomEvent("cancel"));
      this.close();
    });
  }

  /** Populate dropdown options */
  setConnections(list) {
    this.connectionSelect.innerHTML = "";
    list.forEach(name => {
      const opt = document.createElement("option");
      opt.textContent = name;
      opt.value = name;
      this.connectionSelect.appendChild(opt);
    });
  }

  /** Optionally preload existing values */
  setValue({ name = "", connection = "" } = {}) {
    this.nameInput.value = name;
    if (connection) this.connectionSelect.value = connection;
  }

  /** Show dialog */
  open() {
    this.style.display = "flex";
  }

  /** Hide dialog */
  close() {
    this.style.display = "none";
  }
}

customElements.define("workflow-dialog", WorkflowDialog);