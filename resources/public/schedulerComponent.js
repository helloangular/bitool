import EventHandler from "./library/eventHandler.js";
import StateManager from "./library/state-manager.js";
import { request } from "./library/utils.js";

class SchedulerComponent extends HTMLElement {
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
      const resp = await fetch("schedulerComponent.html");
      const html = await resp.text();
      const tpl = document.createElement("template");
      tpl.innerHTML = html;
      this.shadowRoot.appendChild(tpl.content.cloneNode(true));
      this.bindElements();
    } catch (err) {
      console.error("SchedulerComponent: failed to load template", err);
    }
  }

  bindElements() {
    const sr = this.shadowRoot;
    this.saveButton = sr.querySelector("#saveButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.cronInput = sr.querySelector("#cronExpression");
    this.cronPreview = sr.querySelector("#cronPreview");
    this.timezoneSelect = sr.querySelector("#timezone");
    this.addParamBtn = sr.querySelector("#addParamBtn");
    this.paramsBody = sr.querySelector("#paramsBody");
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
    this.state = new StateManager({
      cron_expression: rect.cron_expression || "",
      timezone: rect.timezone || "UTC",
      params: rect.params || [],
    });
  }

  populateFields() {
    if (!this.cronInput) return;
    const rect = this.selectedRectangle || {};
    this.cronInput.value = rect.cron_expression || "";
    this.timezoneSelect.value = rect.timezone || "UTC";
    this.updateCronPreview(rect.cron_expression || "");
    this.renderParams(rect.params || []);
    this.saveButton.disabled = true;
  }

  setupEventListeners() {
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "Scheduler");
    EventHandler.on(this.closeButton, "click", () => {
      this.setAttribute("visibility", "close");
    }, false, "Scheduler");
    EventHandler.on(this.cronInput, "input", () => {
      this.state.updateField("cron_expression", this.cronInput.value);
      this.updateCronPreview(this.cronInput.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Scheduler");
    EventHandler.on(this.timezoneSelect, "change", () => {
      this.state.updateField("timezone", this.timezoneSelect.value);
      this.saveButton.disabled = !this.state.isDirty();
    }, false, "Scheduler");
    EventHandler.on(this.addParamBtn, "click", () => this.addParam(), false, "Scheduler");
  }

  updateCronPreview(expr) {
    if (!this.cronPreview) return;
    const parts = (expr || "").trim().split(/\s+/);
    if (parts.length !== 5) {
      this.cronPreview.textContent = parts.length > 0 && expr.trim()
        ? "⚠ Expected 5 fields: minute hour day month weekday"
        : "";
      return;
    }
    const [min, hour, day, month, weekday] = parts;
    const allWild = (v) => v === "*";
    if (allWild(min) && allWild(hour) && allWild(day) && allWild(month) && allWild(weekday)) {
      this.cronPreview.textContent = "Every minute";
    } else if (allWild(min) && allWild(day) && allWild(month) && allWild(weekday)) {
      this.cronPreview.textContent = `Every hour at :${hour.padStart(2,"0")} (hour field shows minute here — check fields)`;
    } else if (allWild(day) && allWild(month) && allWild(weekday)) {
      this.cronPreview.textContent = `Daily at ${hour.padStart(2,"0")}:${min.padStart(2,"0")}`;
    } else if (allWild(day) && allWild(month)) {
      this.cronPreview.textContent = `Weekly on ${weekday} at ${hour.padStart(2,"0")}:${min.padStart(2,"0")}`;
    } else {
      this.cronPreview.textContent = `${expr}`;
    }
  }

  addParam() {
    const current = [...(this.state.current.params || [])];
    current.push({ name: "", value: "", data_type: "varchar" });
    this.state.updateField("params", current);
    this.renderParams(current);
    this.saveButton.disabled = !this.state.isDirty();
  }

  renderParams(params) {
    if (!this.paramsBody) return;
    this.paramsBody.innerHTML = "";
    (params || []).forEach((p, i) => {
      const tr = document.createElement("tr");
      tr.style.borderBottom = "1px solid #eee";

      const makeInput = (value, field, placeholder) => {
        const el = document.createElement("input");
        el.type = "text";
        el.value = value || "";
        el.dataset.idx = i;
        el.dataset.field = field;
        if (placeholder) el.placeholder = placeholder;
        el.style.cssText = "width:90%;padding:3px;border:1px solid #ddd;border-radius:3px;";
        return el;
      };

      const typeSelect = document.createElement("select");
      typeSelect.dataset.idx = i;
      typeSelect.dataset.field = "data_type";
      typeSelect.style.cssText = "padding:3px;border:1px solid #ddd;border-radius:3px;";
      ["varchar", "integer", "boolean"].forEach((t) => {
        const opt = document.createElement("option");
        opt.value = t;
        opt.textContent = t;
        if (t === (p.data_type || "varchar")) opt.selected = true;
        typeSelect.appendChild(opt);
      });

      const removeBtn = document.createElement("button");
      removeBtn.dataset.idx = i;
      removeBtn.className = "remove-btn";
      removeBtn.style.cssText = "cursor:pointer;border:none;background:none;color:red;";
      removeBtn.textContent = "x";

      const td1 = document.createElement("td"); td1.appendChild(makeInput(p.name, "name", "e.g. report_date"));
      const td2 = document.createElement("td"); td2.appendChild(makeInput(p.value, "value", "e.g. {{triggered_at|date}}"));
      const td3 = document.createElement("td"); td3.appendChild(typeSelect);
      const td4 = document.createElement("td"); td4.appendChild(removeBtn);
      tr.append(td1, td2, td3, td4);
      this.paramsBody.appendChild(tr);
    });

    this.paramsBody.querySelectorAll("input, select").forEach((el) => {
      const handler = () => {
        const idx = parseInt(el.dataset.idx);
        const field = el.dataset.field;
        const current = [...(this.state.current.params || [])];
        if (current[idx]) {
          current[idx] = { ...current[idx], [field]: el.value };
          this.state.updateField("params", current);
          this.saveButton.disabled = !this.state.isDirty();
        }
      };
      el.addEventListener("change", handler);
      if (el.tagName === "INPUT") el.addEventListener("input", handler);
    });

    this.paramsBody.querySelectorAll(".remove-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        const current = [...(this.state.current.params || [])];
        current.splice(idx, 1);
        this.state.updateField("params", current);
        this.renderParams(current);
        this.saveButton.disabled = !this.state.isDirty();
      });
    });
  }

  async save() {
    if (!this.selectedRectangle) return;
    try {
      const values = {
        id: this.selectedRectangle.id,
        cron_expression: this.state.current.cron_expression,
        timezone: this.state.current.timezone,
        params: this.state.current.params,
      };
      const data = await request("/saveSc", { method: "POST", body: values });
      const idx = (window.data?.rectangles || []).findIndex(
        (r) => String(r.id) === String(this.selectedRectangle.id)
      );
      if (idx !== -1) window.data.rectangles[idx] = data;
      this.selectedRectangle = data;
      this.state.commit();
      this.saveButton.disabled = true;
    } catch (err) {
      console.error("SchedulerComponent: save failed", err);
    }
  }
}

customElements.define("scheduler-component", SchedulerComponent);
