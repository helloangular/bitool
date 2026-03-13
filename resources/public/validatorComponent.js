import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

const RULE_TYPES = ["required", "min", "max", "min-length", "max-length", "regex", "one-of", "type"];

class ValidatorComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.selectedRectangle = null;
  }

  static get observedAttributes() {
    return ["visibility"];
  }

  connectedCallback() {
    this.loadTemplate();
  }

  disconnectedCallback() {
  }

  async loadTemplate() {
    try {
      const resp = await fetch("validatorComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("ValidatorComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.saveButton = sr.querySelector("#saveButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.addRuleBtn = sr.querySelector("#addRuleBtn");
    this.rulesBody = sr.querySelector("#rulesBody");
  }

  attributeChangedCallback(name, oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") this.open();
    else this.close();
  }

  open() {
    this.style.display = "block";
    this.updateSelectedRectangle();
    this.initializeState();
    this.populateFields();
    this.setupEventListeners();
  }

  close() {
    this.style.display = "none";
  }

  updateSelectedRectangle() {
    const id = window.data?.selectedRectangle;
    this.selectedRectangle = (window.data?.rectangles || []).find(
      (r) => String(r.id) === String(id)
    );
  }

  initializeState() {
    const rect = this.selectedRectangle || {};
    this.state = new StateManager({ rules: rect.rules || [] });
  }

  populateFields() {
    if (!this.rulesBody) return;
    const rect = this.selectedRectangle || {};
    this.renderRules(rect.rules || []);
    this.saveButton.disabled = true;
  }

  setupEventListeners() {
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "Validator");
    EventHandler.on(this.closeButton, "click", () => {
      this.setAttribute("visibility", "close");
    }, false, "Validator");
    EventHandler.on(this.addRuleBtn, "click", () => this.addRule(), false, "Validator");
  }

  addRule() {
    const current = [...(this.state.current.rules || [])];
    current.push({ field: "", rule: "required", value: "", message: "" });
    this.state.updateField("rules", current);
    this.renderRules(current);
    this.saveButton.disabled = !this.state.isDirty();
  }

  renderRules(rules) {
    if (!this.rulesBody) return;
    this.rulesBody.innerHTML = "";
    (rules || []).forEach((r, i) => {
      const tr = document.createElement("tr");
      tr.style.borderBottom = "1px solid #eee";

      const makeInput = (value, field) => {
        const el = document.createElement("input");
        el.type = "text";
        el.value = value || "";
        el.dataset.idx = i;
        el.dataset.field = field;
        el.style.cssText = "width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;";
        return el;
      };

      const makeSelect = (current, field) => {
        const sel = document.createElement("select");
        sel.dataset.idx = i;
        sel.dataset.field = field;
        sel.style.cssText = "padding:3px;border:1px solid #ddd;border-radius:3px;width:100%;";
        RULE_TYPES.forEach((rt) => {
          const opt = document.createElement("option");
          opt.value = rt;
          opt.textContent = rt;
          if (rt === current) opt.selected = true;
          sel.appendChild(opt);
        });
        return sel;
      };

      const td1 = document.createElement("td"); td1.appendChild(makeInput(r.field, "field"));
      const td2 = document.createElement("td"); td2.appendChild(makeSelect(r.rule, "rule"));
      const td3 = document.createElement("td"); td3.appendChild(makeInput(r.value, "value"));
      const td4 = document.createElement("td"); td4.appendChild(makeInput(r.message, "message"));

      const removeBtn = document.createElement("button");
      removeBtn.dataset.idx = i;
      removeBtn.className = "remove-btn";
      removeBtn.style.cssText = "cursor:pointer;border:none;background:none;color:red;";
      removeBtn.textContent = "x";
      const td5 = document.createElement("td"); td5.appendChild(removeBtn);

      tr.append(td1, td2, td3, td4, td5);
      this.rulesBody.appendChild(tr);
    });

    this.rulesBody.querySelectorAll("input, select").forEach((el) => {
      el.addEventListener("change", () => {
        const idx = parseInt(el.dataset.idx);
        const field = el.dataset.field;
        const current = [...(this.state.current.rules || [])];
        if (current[idx]) {
          current[idx] = { ...current[idx], [field]: el.value };
          this.state.updateField("rules", current);
          this.saveButton.disabled = !this.state.isDirty();
        }
      });
      // also fire on input for text fields
      if (el.tagName === "INPUT") {
        el.addEventListener("input", () => el.dispatchEvent(new Event("change")));
      }
    });

    this.rulesBody.querySelectorAll(".remove-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        const current = [...(this.state.current.rules || [])];
        current.splice(idx, 1);
        this.state.updateField("rules", current);
        this.renderRules(current);
        this.saveButton.disabled = !this.state.isDirty();
      });
    });
  }

  async save() {
    if (!this.selectedRectangle) return;
    try {
      const values = { id: this.selectedRectangle.id, rules: this.state.current.rules };
      const data = await request("/saveValidator", { method: "POST", body: values });
      const idx = (window.data?.rectangles || []).findIndex(
        (r) => String(r.id) === String(this.selectedRectangle.id)
      );
      if (idx !== -1) window.data.rectangles[idx] = data;
      this.selectedRectangle = data;
      this.state.commit();
      this.saveButton.disabled = true;
    } catch (err) {
      console.error("ValidatorComponent: save failed", err);
    }
  }
}

customElements.define("validator-component", ValidatorComponent);
