import EventHandler from "./library/eventHandler.js";
import { customConfirm, request } from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `
<style>
  :host {
    visibility: hidden;
    background: #eff4f0;
    color: #17251c;
    overflow: auto;
    font-family: Georgia, "Times New Roman", serif;
  }
  .shell {
    min-height: 100vh;
    padding: 24px;
    background:
      linear-gradient(135deg, rgba(53, 94, 59, 0.14), transparent 36%),
      linear-gradient(180deg, #f7fbf8 0%, #ebf3ee 100%);
  }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
  }
  .title h2 { margin: 0; font-size: 28px; font-weight: 600; }
  .title p { margin: 4px 0 0 0; color: #5c6c60; font-size: 14px; }
  .actions { display: flex; gap: 10px; flex-wrap: wrap; }
  button {
    border: 1px solid #203326;
    background: #203326;
    color: #fffdf8;
    padding: 10px 14px;
    cursor: pointer;
  }
  button.secondary { background: transparent; color: #203326; }
  .card {
    background: rgba(255, 255, 255, 0.88);
    border: 1px solid rgba(32, 51, 38, 0.15);
    box-shadow: 0 18px 42px rgba(32, 51, 38, 0.08);
    padding: 18px;
    margin-bottom: 16px;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 12px;
  }
  label { display: block; font-size: 13px; color: #526055; margin-bottom: 4px; }
  input, select, textarea {
    width: 100%;
    box-sizing: border-box;
    border: 1px solid rgba(32, 51, 38, 0.18);
    background: #fffefb;
    padding: 8px 10px;
    color: #17251c;
    font-size: 14px;
  }
  textarea {
    min-height: 92px;
    resize: vertical;
    font-family: "SFMono-Regular", Consolas, monospace;
  }
  .status { min-height: 20px; color: #526055; font-size: 13px; margin-top: 10px; }
  h3 { margin: 0 0 10px 0; font-size: 16px; font-weight: 600; color: #203326; }
  .topic-entry { border: 1px solid rgba(32,51,38,0.12); padding: 14px; margin-bottom: 10px; }
  .topic-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
  .topic-header span { font-weight: 600; font-size: 14px; }
  button.remove-topic { background: #c00; border-color: #c00; padding: 4px 10px; font-size: 12px; }
  button.add-topic { background: transparent; color: #203326; border: 1px dashed #203326; padding: 6px 14px; font-size: 13px; }
</style>
<div class="shell">
  <div class="header">
    <div class="title">
      <h2>Kafka Source</h2>
      <p>Configure Kafka consumer connection and topic subscriptions.</p>
    </div>
    <div class="actions">
      <button id="saveButton" type="button">Save</button>
      <button id="closeButton" class="secondary" type="button">Close</button>
    </div>
  </div>

  <div class="card">
    <h3>Connection</h3>
    <div class="grid">
      <div>
        <label for="sourceSystem">Source System</label>
        <input id="sourceSystem" data-field="source_system" type="text" />
      </div>
      <div>
        <label for="connectionId">Connection ID</label>
        <input id="connectionId" data-field="connection_id" type="number" />
      </div>
      <div>
        <label for="bootstrapServers">Bootstrap Servers</label>
        <input id="bootstrapServers" data-field="bootstrap_servers" type="text" placeholder="broker1:9092,broker2:9092" />
      </div>
      <div>
        <label for="securityProtocol">Security Protocol</label>
        <select id="securityProtocol" data-field="security_protocol">
          <option value="PLAINTEXT">PLAINTEXT</option>
          <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
          <option value="SASL_SSL">SASL_SSL</option>
          <option value="SSL">SSL</option>
        </select>
      </div>
      <div>
        <label for="consumerGroupId">Consumer Group ID</label>
        <input id="consumerGroupId" data-field="consumer_group_id" type="text" />
      </div>
    </div>
  </div>

  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
      <h3 style="margin:0;">Topic Configs</h3>
      <button id="addTopicBtn" class="add-topic" type="button">+ Add Topic</button>
    </div>
    <div id="topicContainer"></div>
  </div>

  <div class="status" id="statusText"></div>
</div>
`;

function currentRectangle() {
  const selectedId = window.data?.selectedRectangle;
  const items = window.data?.rectangles || window.data?.panelItems || [];
  return items.find((item) => String(item.id) === String(selectedId)) || null;
}

function defaultTopicConfig() {
  return {
    topic_name: "",
    endpoint_name: "",
    enabled: true,
    key_deserializer: "string",
    value_deserializer: "json",
    auto_offset_reset: "earliest",
    max_poll_records: 500,
    primary_key_fields: [],
    watermark_column: "",
    bronze_table_name: "",
    schema_mode: "manual",
    batch_flush_rows: 1000,
    rate_limit_per_poll_ms: 0,
  };
}

class KafkaSourceComponent extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.state = null;
    this.savedState = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
    this.bind();
    this.closeImmediately();
  }

  disconnectedCallback() { EventHandler.removeGroup("KafkaSource"); }

  attributeChangedCallback(name, _oldVal, newVal) {
    if (name !== "visibility") return;
    if (newVal === "open") this.open();
    else this.close();
  }

  bind() {
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.closeButton = this.shadowRoot.querySelector("#closeButton");
    this.addTopicBtn = this.shadowRoot.querySelector("#addTopicBtn");
    this.topicContainer = this.shadowRoot.querySelector("#topicContainer");
    this.statusText = this.shadowRoot.querySelector("#statusText");
  }

  hydrate() {
    const rect = currentRectangle() || {};
    return {
      id: rect.id || null,
      source_system: rect.source_system || "kafka",
      connection_id: rect.connection_id ?? "",
      bootstrap_servers: rect.bootstrap_servers || "",
      security_protocol: rect.security_protocol || "PLAINTEXT",
      consumer_group_id: rect.consumer_group_id || "",
      topic_configs: Array.isArray(rect.topic_configs) && rect.topic_configs.length
        ? rect.topic_configs.map((tc) => ({ ...defaultTopicConfig(), ...tc }))
        : [defaultTopicConfig()],
    };
  }

  open() {
    this.style.visibility = "visible";
    this.state = this.hydrate();
    this.savedState = JSON.stringify(this.state);
    this.render();
    this.attachEvents();
  }

  closeImmediately() { this.style.visibility = "hidden"; }

  async close() {
    if (this.style.visibility === "hidden") return;
    if (JSON.stringify(this.state) !== this.savedState) {
      const discard = await customConfirm("Discard unsaved Kafka source changes?");
      if (!discard) return;
    }
    this.closeImmediately();
  }

  markDirty() {
    this.saveButton.disabled = JSON.stringify(this.state) === this.savedState;
  }

  esc(value) {
    const div = document.createElement("div");
    div.textContent = String(value ?? "");
    return div.innerHTML;
  }

  attachEvents() {
    EventHandler.removeGroup("KafkaSource");
    EventHandler.on(this.saveButton, "click", () => this.save(), false, "KafkaSource");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "KafkaSource");
    EventHandler.on(this.addTopicBtn, "click", () => {
      this.state.topic_configs.push(defaultTopicConfig());
      this.renderTopics();
      this.markDirty();
    }, false, "KafkaSource");

    // connection-level fields
    this.shadowRoot.querySelectorAll("[data-field]").forEach((el) => {
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (e) => {
        const field = e.target.dataset.field;
        const value = e.target.value;
        if (field === "connection_id") {
          this.state[field] = value ? Number(value) : "";
        } else {
          this.state[field] = value;
        }
        this.markDirty();
      }, false, "KafkaSource");
    });
  }

  attachTopicEvents() {
    this.topicContainer.querySelectorAll("[data-topic-idx]").forEach((el) => {
      const idx = Number(el.dataset.topicIdx);
      const field = el.dataset.topicField;
      const eventName = el.tagName === "SELECT" ? "change" : "input";
      EventHandler.on(el, eventName, (e) => {
        const tc = this.state.topic_configs[idx];
        if (!tc) return;
        const val = e.target.type === "checkbox" ? e.target.checked : e.target.value;
        if (field === "primary_key_fields") {
          tc[field] = val.split(",").map((s) => s.trim()).filter(Boolean);
        } else if (["max_poll_records", "batch_flush_rows", "rate_limit_per_poll_ms"].includes(field)) {
          tc[field] = val ? Number(val) : 0;
        } else {
          tc[field] = val;
        }
        this.markDirty();
      }, false, "KafkaSource");
    });

    this.topicContainer.querySelectorAll("[data-remove-topic]").forEach((btn) => {
      EventHandler.on(btn, "click", () => {
        const idx = Number(btn.dataset.removeTopic);
        this.state.topic_configs.splice(idx, 1);
        this.renderTopics();
        this.markDirty();
      }, false, "KafkaSource");
    });
  }

  render() {
    const sr = this.shadowRoot;
    sr.querySelector("#sourceSystem").value = this.state.source_system;
    sr.querySelector("#connectionId").value = this.state.connection_id;
    sr.querySelector("#bootstrapServers").value = this.state.bootstrap_servers;
    sr.querySelector("#securityProtocol").value = this.state.security_protocol;
    sr.querySelector("#consumerGroupId").value = this.state.consumer_group_id;
    this.renderTopics();
    this.saveButton.disabled = true;
  }

  renderTopics() {
    const html = this.state.topic_configs.map((tc, idx) => `
      <div class="topic-entry">
        <div class="topic-header">
          <span>Topic ${idx + 1}</span>
          <button class="remove-topic" data-remove-topic="${idx}" type="button">Remove</button>
        </div>
        <div class="grid">
          <div>
            <label>Topic Name</label>
            <input data-topic-idx="${idx}" data-topic-field="topic_name" type="text" value="${this.esc(tc.topic_name || "")}" />
          </div>
          <div>
            <label>Endpoint Name</label>
            <input data-topic-idx="${idx}" data-topic-field="endpoint_name" type="text" value="${this.esc(tc.endpoint_name || "")}" />
          </div>
          <div>
            <label>Key Deserializer</label>
            <select data-topic-idx="${idx}" data-topic-field="key_deserializer">
              ${["string", "json", "bytes"].map((v) => `<option value="${v}" ${tc.key_deserializer === v ? "selected" : ""}>${v}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Value Deserializer</label>
            <select data-topic-idx="${idx}" data-topic-field="value_deserializer">
              ${["json", "string", "bytes"].map((v) => `<option value="${v}" ${tc.value_deserializer === v ? "selected" : ""}>${v}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Auto Offset Reset</label>
            <select data-topic-idx="${idx}" data-topic-field="auto_offset_reset">
              ${["earliest", "latest"].map((v) => `<option value="${v}" ${tc.auto_offset_reset === v ? "selected" : ""}>${v}</option>`).join("")}
            </select>
          </div>
          <div>
            <label>Max Poll Records</label>
            <input data-topic-idx="${idx}" data-topic-field="max_poll_records" type="number" value="${tc.max_poll_records}" />
          </div>
          <div>
            <label>Primary Key Fields (CSV)</label>
            <input data-topic-idx="${idx}" data-topic-field="primary_key_fields" type="text" value="${this.esc((tc.primary_key_fields || []).join(", "))}" />
          </div>
          <div>
            <label>Watermark Column</label>
            <input data-topic-idx="${idx}" data-topic-field="watermark_column" type="text" value="${this.esc(tc.watermark_column || "")}" />
          </div>
          <div>
            <label>Bronze Table Name</label>
            <input data-topic-idx="${idx}" data-topic-field="bronze_table_name" type="text" value="${this.esc(tc.bronze_table_name || "")}" />
          </div>
          <div>
            <label>Batch Flush Rows</label>
            <input data-topic-idx="${idx}" data-topic-field="batch_flush_rows" type="number" value="${tc.batch_flush_rows}" />
          </div>
          <div>
            <label>Rate Limit / Poll (ms)</label>
            <input data-topic-idx="${idx}" data-topic-field="rate_limit_per_poll_ms" type="number" value="${tc.rate_limit_per_poll_ms}" />
          </div>
        </div>
      </div>
    `).join("");
    this.topicContainer.innerHTML = html;
    this.attachTopicEvents();
  }

  payload() {
    return {
      id: this.state.id,
      source_system: this.state.source_system,
      connection_id: this.state.connection_id,
      bootstrap_servers: this.state.bootstrap_servers,
      security_protocol: this.state.security_protocol,
      consumer_group_id: this.state.consumer_group_id,
      topic_configs: this.state.topic_configs,
    };
  }

  async save() {
    try {
      const response = await request("/saveKafkaSource", { method: "POST", body: this.payload() });
      const rects = window.data?.rectangles || window.data?.panelItems || [];
      const idx = rects.findIndex((item) => String(item.id) === String(response.id));
      if (idx >= 0) rects[idx] = { ...rects[idx], ...response };
      this.state = this.hydrate();
      this.savedState = JSON.stringify(this.state);
      this.statusText.textContent = "Kafka source saved.";
      this.render();
    } catch (error) {
      this.statusText.textContent = error.message || "Save failed.";
    }
  }
}

customElements.define("kafka-source-component", KafkaSourceComponent);
