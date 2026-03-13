import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

const DATA_TYPES = ["varchar", "integer", "boolean", "numeric"];

class AuthComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.selectedRectangle = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() { this.loadTemplate(); }


  async loadTemplate() {
    try {
      const resp = await fetch("authComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("AuthComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.nodeNameInput      = sr.querySelector("#nodeName");
    this.authType      = sr.querySelector("#authType");
    this.tokenHeader   = sr.querySelector("#tokenHeader");
    this.secret        = sr.querySelector("#secret");
    this.claimsBody    = sr.querySelector("#claimsBody");
    this.addClaimBtn   = sr.querySelector("#addClaimBtn");
    this.saveBtn       = sr.querySelector("#saveBtn");
    this.closeBtn      = sr.querySelector("#closeBtn");

    EventHandler.on(this.saveBtn,  "click", () => this.save(),  false, "Auth");
    EventHandler.on(this.closeBtn, "click", () => this.close(), false, "Auth");
    this.closeBtn.addEventListener("click", () => this.close());
    EventHandler.on(this.addClaimBtn, "click", () => this.addClaimRow(), false, "Auth");
  }

  attributeChangedCallback(name, _old, newValue) {
    if (name === "visibility") {
      newValue === "open" ? this.open() : this.close();
    }
  }

  open() {
    this.style.display = "block";
    this.selectedRectangle = window.data?.rectangles?.find(r => r.id === window.data?.selectedRectangle);
    if (!this.selectedRectangle) return;
    const rect = this.selectedRectangle;
    this.nodeNameInput.value    = rect.name || "";
    this.authType.value    = rect.auth_type || "jwt";
    this.tokenHeader.value = rect.token_header || "Authorization";
    // Never pre-fill secret — server never echoes it back
    this.secret.value       = "";
    this.secret.placeholder = rect.secret_set ? "Secret is set — leave blank to keep" : "Enter secret / public key";
    this.renderClaims(rect.claims_to_cols || []);
    this.state = new StateManager({
      name: rect.name || "",
      auth_type: rect.auth_type || "jwt",
      token_header: rect.token_header || "Authorization",
      claims_to_cols: rect.claims_to_cols || []
    });
  }

  close() {
    this.style.display = "none";
  }

  renderClaims(claims) {
    this.claimsBody.innerHTML = "";
    claims.forEach(c => this.addClaimRow(c));
  }

  addClaimRow(data = {}) {
    const tr = document.createElement("tr");

    const claimTd = document.createElement("td");
    const claimInput = document.createElement("input");
    claimInput.value = data.claim || "";
    claimInput.placeholder = "sub";
    claimTd.appendChild(claimInput);

    const colTd = document.createElement("td");
    const colInput = document.createElement("input");
    colInput.value = data.column || "";
    colInput.placeholder = "user_id";
    colTd.appendChild(colInput);

    const typeTd = document.createElement("td");
    const typeSelect = document.createElement("select");
    DATA_TYPES.forEach(dt => {
      const opt = document.createElement("option");
      opt.value = opt.textContent = dt;
      if (dt === (data.data_type || "varchar")) opt.selected = true;
      typeSelect.appendChild(opt);
    });
    typeTd.appendChild(typeSelect);

    const removeTd = document.createElement("td");
    const removeBtn = document.createElement("button");
    removeBtn.textContent = "✕";
    removeBtn.className = "remove-btn";
    removeBtn.style.cssText = "background:none;border:none;color:#c00;cursor:pointer;";
    EventHandler.on(removeBtn, "click", () => tr.remove(), false, "Auth");
    removeTd.appendChild(removeBtn);

    tr.append(claimTd, colTd, typeTd, removeTd);
    this.claimsBody.appendChild(tr);
  }

  getClaims() {
    return Array.from(this.claimsBody.querySelectorAll("tr")).map(tr => {
      const inputs  = tr.querySelectorAll("input");
      const select  = tr.querySelector("select");
      return { claim: inputs[0].value, column: inputs[1].value, data_type: select.value };
    });
  }

  save() {
    const rect = this.selectedRectangle;
    if (!rect) return;
    request("/saveAuth", {
      method: "POST",
      body: {
        id:             rect.id,
        name:           this.nodeNameInput.value,
        auth_type:      this.authType.value,
        token_header:   this.tokenHeader.value,
        secret:         this.secret.value,
        claims_to_cols: this.getClaims()
      }
    }).then(data => {
      Object.assign(rect, data);
      this.close();
    });
  }
}

customElements.define("auth-component", AuthComponent);
