// fetch("getItem?id=7", {
//   method: "GET",
//   headers: {
//     "Content-Type": "application/json",
//   },
// }).then((res) => res.json())
//   .then((data) => {
//     this.selectedRectangle = data;
//     this.renderItems();
//   })
class SidePanel extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.shadowRoot.innerHTML = `
      <header>
        <slot name="title">Panel</slot>
      </header>
      <main>
        <slot></slot>
      </main>
      <footer>
        <button id="saveBtn" disabled>Save</button>
        <button id="closeBtn">Close</button>
      </footer>
    `;
    this.dirty = false;
  }

  connectedCallback() {
    this.saveButton = this.shadowRoot.getElementById("saveBtn");
    this.closeButton = this.shadowRoot.getElementById("closeBtn");

    this.closeButton.addEventListener("click", () => this.close());
    this.saveButton.addEventListener("click", () => this.save());

    // Watch slotted inputs
    // this.observer = new MutationObserver(() => this.trackInputs());
    // this.observer.observe(this, { childList: true, subtree: true });
    this.trackInputs();
  }

  disconnectedCallback() {
    this.observer.disconnect();
  }

  open() {
    this.classList.add("open");
    this.dirty = false;
    this.saveButton.disabled = true;
  }

  close() {
    if (this.dirty) {
      const wantToSave = confirm("Looks like you’ve made changes. Would you like to save them before leaving this panel?");
      if (!wantToSave) this.setAttribute("visibility", "close");
      return; // prevent closing
    }
    this.classList.remove("open");
  }

  save() {
    this.dispatchEvent(new CustomEvent("save", { detail: this.getValues() }));
    this.dirty = false;
    this.saveButton.disabled = true;
  }

  trackInputs() {
    const inputs = this.querySelectorAll("input, select, textarea");
    inputs.forEach(el => {
      el.addEventListener("input", () => this.setDirty());
      el.addEventListener("change", () => this.setDirty());
    });
  }

  setDirty() {
    this.dirty = true;
    this.saveButton.disabled = false;
  }

  getValues() {
    const values = {};
    this.querySelectorAll("input, select, textarea").forEach(el => {
      values[el.name || el.id || el.type] = el.value;
    });
    return values;
  }
}

customElements.define("side-panel", SidePanel);

// Example Usage.
// <side-panel id="panel">
//   <span slot="title">Edit Settings</span>
//   <label>
//     Name:
//     <input type="text" name="username">
//   </label><br><br>
//   <label>
//     Type:
//     <select name="usertype">
//       <option value="admin">Admin</option>
//       <option value="user">User</option>
//     </select>
//   </label>
// </side-panel>

// <button onclick="document.getElementById('panel').open()">Open Panel</button>


class CustomElement extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
  }

  connectedCallback() {
    this.dirty = false;
  }

  disconnectedCallback() { }

  static get observedAttributes() {
    return ["visibility"];
  }

  attributeChangedCallback(name, oldValue, newValue) { 
    if (name === "visibility") {
      if (newValue === "open") {
        this.open();
      } else {
        this.close();
        if (this.style.hidden === "hidden") EventHandler.removeGroup("Group");
      }
    }
  }

  open() {
    this.style.visibility = "visible";
    this.reset();
  }

  reset() {
    this.dirty = false;
    this.saveButton.disabled = true;
  }

  close() {
    if (this.dirty) {
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
    request("/saveFunction", {
      method: "POST",
      body: values
    }).then((data) => {
      this.selectedRectangle = data;
      this.reset();
    })
  }

  trackInputs() {
    const inputs = this.shadowRoot.querySelectorAll("input, select, textarea");
    inputs.forEach(el => {
      el.addEventListener("change", () => this.setDirty());
    });
  }

  setDirty() {
    this.dirty = true;
    this.saveButton.disabled = false;
  }

  getValues() {
    const values = {};
    this.querySelectorAll("input, select, textarea").forEach(el => {
      values[el.name || el.id || el.type] = el.value;
    });
    return values;
  }
}

customElements.define("custom-element", CustomElement);
