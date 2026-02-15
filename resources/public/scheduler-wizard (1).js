const template = document.createElement('template');
template.innerHTML = `
  <style>
    :host { font-family: Arial, sans-serif; }
    .wizard { background: #fff; border: 1px solid #ccc; padding: 15px; max-width: 700px; }
    h3 { font-size: 14px; margin: 0 0 10px; }
    .section { border: 1px solid #ddd; padding: 10px; margin-bottom: 15px; }
    .form-row { display: flex; align-items: center; margin: 8px 0; }
    .form-row label { min-width: 120px; font-size: 13px; }
    input[type="text"], select, textarea, input[type="date"], input[type="time"] { border: 1px solid #aaa; padding: 5px; font-size: 13px; flex: 1; }
    textarea { resize: none; height: 40px; }
    .radio-group { display: flex; flex-direction: column; margin-left: 20px; }
    .inline-group { display: flex; align-items: center; gap: 8px; margin-top: 5px; }
    .footer { display: flex; justify-content: flex-end; gap: 8px; margin-top: 15px; }
    .footer button { padding: 6px 14px; border: 1px solid #999; border-radius: 3px; font-size: 13px; cursor: pointer; }
    .primary { background: #0078d7; color: #fff; border-color: #0078d7; }
    .secondary { background: #eee; }
    .cron-box { margin-left: 20px; margin-top: 8px; display: none; gap: 10px; align-items: center; }
    .cron-box.active { display: flex; }
    .cron-hint { font-size: 11px; color: #555; font-style: italic; }
  </style>
  <div class="wizard">
    <!-- General -->
    <div class="section">
      <div class="form-row">
        <label>Name *</label>
        <input type="text" id="name" value="UTC_Scheduled_Task">
      </div>
      <div class="form-row">
        <label>Description</label>
        <textarea id="desc">Runs every 5 minutes daily...</textarea>
      </div>
    </div>

    <!-- Recurrence -->
    <div class="section">
      <h3>Recurrence</h3>
      <div class="form-row">
        <input type="radio" name="recurrence" id="once"> <label for="once">Run once</label>
      </div>
      <div class="form-row">
        <input type="radio" name="recurrence" id="repeat" checked> <label for="repeat">Recurring Schedule</label>
      </div>
      <div class="radio-group" id="scheduleOptions">
        <label><input type="radio" name="schedule" value="daily" checked> Daily</label>
        <label><input type="radio" name="schedule" value="weekly"> Weekly</label>
        <label><input type="radio" name="schedule" value="monthly"> Monthly</label>
        <label><input type="radio" name="schedule" value="cron"> Cron</label>
      </div>
      <div class="inline-group">
        <label><input type="radio" name="intervalType" value="days"> Run every</label>
        <input type="text" id="daysInterval" value="1" size="2"> days
      </div>
      <div class="inline-group">
        <label><input type="radio" name="intervalType" value="minutes" checked> Run after every</label>
        <input type="text" id="minutesInterval" value="5" size="2">
        <select id="minutesUnit">
          <option>Minutes</option>
          <option>Hours</option>
        </select>
      </div>
      <!-- Cron input -->
      <div class="cron-box" id="cronBox">
        <label for="cron">Cron:</label>
        <input type="text" id="cron" placeholder="* * * * *">
      </div>
      <div class="cron-box" id="cronHint">
        <span class="cron-hint">Example: <code>0 10 * * *</code> → Run every day at 10:00</span>
      </div>
    </div>

    <!-- Start & End -->
    <div class="section" style="display: flex; gap: 20px;">
      <div style="flex:1">
        <h3>Start</h3>
        <div class="form-row">
          <label>Time Zone</label>
          <select id="timezone">
            <option>(UTC) Dublin, Edinburgh, Lisbon</option>
          </select>
        </div>
        <div class="form-row">
          <label>Start Date</label>
          <input type="date" id="startDate" value="2016-06-08">
        </div>
        <div class="form-row">
          <label>Start Time</label>
          <input type="time" id="startTime" value="10:15">
        </div>
      </div>
      <div style="flex:1">
        <h3>End</h3>
        <div class="radio-group" id="endOptions">
          <label><input type="radio" name="end" value="none" checked> No End Date</label>
          <label><input type="radio" name="end" value="date"> End Date</label>
          <label><input type="radio" name="end" value="time"> End Time</label>
          <label><input type="radio" name="end" value="after"> End After <input type="text" id="endAfter" value="10" size="2"> Runs</label>
        </div>
      </div>
    </div>

    <!-- Footer -->
    <div class="footer">
      <button class="secondary" id="saveBtn">Save</button>
      <button class="secondary" id="cancelBtn">Cancel</button>
    </div>
  </div>
`;

class SchedulerWizard extends HTMLElement {
  static get observedAttributes() {
    return ["visibility"]
  }

  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.shadowRoot.append(template.content.cloneNode(true));

    this.$ = (sel) => this.shadowRoot.querySelector(sel);
  }

  connectedCallback() {
    this.updateVisibility();
    // Show/hide cron input
    this.shadowRoot.querySelectorAll("input[name='schedule']").forEach(radio => {
      radio.addEventListener("change", () => {
        const isCron = this.$("input[name='schedule'][value='cron']").checked;
        this.$("#cronBox").classList.toggle("active", isCron);
        this.$("#cronHint").classList.toggle("active", isCron);
      });
    });

    // Button events
    this.$("#saveBtn").addEventListener("click", () => this.save(this.getConfig()));
    this.$("#cancelBtn").addEventListener("click", () => this.setAttribute("visibility", "close"));
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      this.updateVisibility();
    }
  }

  updateVisibility() {
    this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  getConfig() {
    return {
      name: this.$("#name").value,
      description: this.$("#desc").value,
      recurrence: this.$("input[name='recurrence']:checked")?.id,
      schedule: this.$("input[name='schedule']:checked")?.value,
      intervalType: this.$("input[name='intervalType']:checked")?.value,
      daysInterval: this.$("#daysInterval").value,
      minutesInterval: this.$("#minutesInterval").value,
      minutesUnit: this.$("#minutesUnit").value,
      cron: this.$("#cron").value,
      timezone: this.$("#timezone").value,
      startDate: this.$("#startDate").value,
      startTime: this.$("#startTime").value,
      end: this.$("input[name='end']:checked")?.value,
      endAfter: this.$("#endAfter").value
    };
  }

  save(data) {
    fetch("/saveSchedule", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data)
    }).then((response) => response.json())
      .then((json) => {
        console.log(json);
        this.setAttribute("visibility", "close");
      }).catch((error) => {
        console.error("Error:", error);
      });
  }
}

customElements.define("scheduler-wizard", SchedulerWizard);
