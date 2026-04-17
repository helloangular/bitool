import EventHandler from "./library/eventHandler.js";
import { request } from "./library/utils.js";

function buildTemplate() {
  const t = document.createElement("template");
  t.innerHTML = `
<style>
  :host { display: none; box-sizing: border-box; }
  :host([visibility="open"]) { display: block; }
  * { box-sizing: border-box; font-family: 'DM Sans', -apple-system, sans-serif; }
  .panel {
    height: 100%;
    display: flex;
    flex-direction: column;
    background: #ffffff;
    border-left: 1px solid #e8ebf0;
    box-shadow: -12px 0 32px rgba(23, 28, 38, 0.08);
  }
  .header {
    padding: 18px 18px 14px;
    border-bottom: 1px solid #eceff4;
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    background: linear-gradient(180deg, #fafbff 0%, #f4f7fb 100%);
  }
  .title { margin: 0; font-size: 16px; font-weight: 700; color: #172133; }
  .subtitle { margin: 4px 0 0; font-size: 12px; color: #667085; line-height: 1.4; }
  .close {
    border: 1px solid #d9dfeb;
    background: #fff;
    color: #4a5568;
    border-radius: 8px;
    width: 30px;
    height: 30px;
    cursor: pointer;
    font-size: 18px;
    line-height: 1;
  }
  .body {
    flex: 1;
    overflow: auto;
    padding: 16px 18px 22px;
    display: flex;
    flex-direction: column;
    gap: 18px;
  }
  .section {
    border: 1px solid #e8ebf0;
    border-radius: 14px;
    padding: 14px;
    background: #fff;
  }
  .section h4 {
    margin: 0 0 10px;
    font-size: 13px;
    font-weight: 700;
    color: #172133;
  }
  .row {
    display: flex;
    flex-direction: column;
    gap: 6px;
    margin-bottom: 10px;
  }
  label {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    color: #667085;
  }
  input, select {
    width: 100%;
    border: 1px solid #d9dfeb;
    border-radius: 10px;
    padding: 10px 12px;
    font-size: 13px;
    color: #172133;
    background: #fff;
  }
  input:focus, select:focus {
    outline: none;
    border-color: #4f46e5;
    box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.12);
  }
  .actions {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
  }
  button.primary, button.secondary {
    border-radius: 10px;
    padding: 9px 12px;
    font-size: 12px;
    font-weight: 700;
    cursor: pointer;
  }
  button.primary {
    border: 1px solid #4f46e5;
    background: #4f46e5;
    color: white;
  }
  button.secondary {
    border: 1px solid #d9dfeb;
    background: white;
    color: #172133;
  }
  .status {
    min-height: 18px;
    font-size: 12px;
    color: #667085;
  }
  .status.error { color: #d92d20; }
  .status.success { color: #027a48; }
  .pill-row {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    padding: 4px 8px;
    border-radius: 999px;
    background: #eef2ff;
    color: #3730a3;
    font-size: 11px;
    font-weight: 700;
  }
  .meta {
    display: grid;
    grid-template-columns: 1fr;
    gap: 8px;
    font-size: 12px;
    color: #344054;
  }
  .member-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .member {
    border: 1px solid #eef2f6;
    border-radius: 10px;
    padding: 10px;
    background: #fafbfd;
  }
  .member strong {
    display: block;
    color: #172133;
    font-size: 12px;
  }
  .helper {
    font-size: 12px;
    color: #667085;
    line-height: 1.4;
  }
</style>
<div class="panel">
  <div class="header">
    <div>
      <h3 class="title">Session & Workspace</h3>
      <p class="subtitle">Sign in, switch workspace, and manage local team access.</p>
    </div>
    <button class="close" type="button" aria-label="Close">&times;</button>
  </div>
  <div class="body">
    <div class="status" id="status"></div>
    <div class="section" id="signinSection">
      <h4>Sign In</h4>
      <div class="row">
        <label for="username">Username</label>
        <input id="username" type="text" placeholder="admin" />
      </div>
      <div class="row">
        <label for="workspace">Workspace</label>
        <input id="workspace" type="text" placeholder="default" />
      </div>
      <div class="actions">
        <button class="primary" id="signinBtn" type="button">Sign In</button>
        <button class="secondary" id="refreshBtn" type="button">Refresh</button>
      </div>
      <p class="helper">Local installs seed an <code>admin</code> user in the <code>default</code> workspace.</p>
    </div>
    <div class="section" id="sessionSection" hidden>
      <h4>Current Session</h4>
      <div class="meta" id="sessionMeta"></div>
      <div class="row">
        <label for="workspaceSelect">Switch Workspace</label>
        <select id="workspaceSelect"></select>
      </div>
      <div class="actions">
        <button class="primary" id="switchBtn" type="button">Switch Workspace</button>
        <button class="secondary" id="logoutBtn" type="button">Sign Out</button>
      </div>
    </div>
    <div class="section" id="adminSection" hidden>
      <h4>Team Access</h4>
      <div class="row">
        <label for="newUsername">Create User</label>
        <input id="newUsername" type="text" placeholder="analyst-1" />
      </div>
      <div class="row">
        <label for="newDisplayName">Display Name</label>
        <input id="newDisplayName" type="text" placeholder="Analyst One" />
      </div>
      <div class="row">
        <label for="newRoles">Global Roles</label>
        <input id="newRoles" type="text" placeholder="viewer, api.audit" />
      </div>
      <div class="actions">
        <button class="secondary" id="createUserBtn" type="button">Save User</button>
      </div>
      <div class="row">
        <label for="memberUsername">Grant Workspace Access</label>
        <input id="memberUsername" type="text" placeholder="analyst-1" />
      </div>
      <div class="row">
        <label for="memberRole">Workspace Role</label>
        <select id="memberRole">
          <option value="viewer">viewer</option>
          <option value="editor">editor</option>
          <option value="admin">admin</option>
        </select>
      </div>
      <div class="actions">
        <button class="secondary" id="grantBtn" type="button">Grant Access</button>
      </div>
      <div class="member-list" id="memberList"></div>
    </div>
  </div>
</div>`;
  return t;
}

class SessionControl extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.session = null;
  }

  static get observedAttributes() { return ["visibility"]; }

  connectedCallback() {
    this.shadowRoot.appendChild(buildTemplate().content.cloneNode(true));
    this.bind();
  }

  attributeChangedCallback(name, _oldValue, newValue) {
    if (name === "visibility") {
      if (newValue === "open") this.open();
      else this.close();
    }
  }

  bind() {
    const q = (selector) => this.shadowRoot.querySelector(selector);
    this.$close = q(".close");
    this.$status = q("#status");
    this.$signinSection = q("#signinSection");
    this.$sessionSection = q("#sessionSection");
    this.$adminSection = q("#adminSection");
    this.$username = q("#username");
    this.$workspace = q("#workspace");
    this.$signinBtn = q("#signinBtn");
    this.$refreshBtn = q("#refreshBtn");
    this.$sessionMeta = q("#sessionMeta");
    this.$workspaceSelect = q("#workspaceSelect");
    this.$switchBtn = q("#switchBtn");
    this.$logoutBtn = q("#logoutBtn");
    this.$newUsername = q("#newUsername");
    this.$newDisplayName = q("#newDisplayName");
    this.$newRoles = q("#newRoles");
    this.$createUserBtn = q("#createUserBtn");
    this.$memberUsername = q("#memberUsername");
    this.$memberRole = q("#memberRole");
    this.$grantBtn = q("#grantBtn");
    this.$memberList = q("#memberList");

    EventHandler.on(this.$close, "click", () => this.close(), false, "SessionControl");
    EventHandler.on(this.$signinBtn, "click", () => this.signIn(), false, "SessionControl");
    EventHandler.on(this.$refreshBtn, "click", () => this.refresh(), false, "SessionControl");
    EventHandler.on(this.$switchBtn, "click", () => this.switchWorkspace(), false, "SessionControl");
    EventHandler.on(this.$logoutBtn, "click", () => this.signOut(), false, "SessionControl");
    EventHandler.on(this.$createUserBtn, "click", () => this.createUser(), false, "SessionControl");
    EventHandler.on(this.$grantBtn, "click", () => this.grantAccess(), false, "SessionControl");
  }

  open() {
    if (this.getAttribute("visibility") !== "open") {
      this.setAttribute("visibility", "open");
      return;
    }
    this.style.display = "block";
    this.refresh();
  }

  close() {
    if (this.getAttribute("visibility") !== "close") {
      this.setAttribute("visibility", "close");
      return;
    }
    this.style.display = "none";
  }

  setStatus(message, type = "") {
    this.$status.textContent = message || "";
    this.$status.className = type ? `status ${type}` : "status";
  }

  isAdmin() {
    const roles = this.session?.user?.global_roles || [];
    return roles.includes("admin") || roles.includes("api.ops");
  }

  async refresh() {
    try {
      const data = await request("/controlPlane/session");
      this.session = data;
      this.render();
      if (data.authenticated && this.isAdmin()) {
        await this.loadMembers();
      } else {
        this.$memberList.innerHTML = "";
      }
      this.setStatus(data.authenticated ? "Session loaded." : "Not signed in.");
    } catch (e) {
      this.setStatus("Failed to load session: " + (e.message || e), "error");
    }
  }

  render() {
    const data = this.session || {};
    const authenticated = Boolean(data.authenticated);
    this.$signinSection.hidden = authenticated;
    this.$sessionSection.hidden = !authenticated;
    this.$adminSection.hidden = !(authenticated && this.isAdmin());
    this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";

    if (!authenticated) return;

    const user = data.user || {};
    const workspaceLabel = data.workspace_key || "default";
    const roleLabel = data.workspace_role || "unassigned";
    const roles = Array.isArray(user.global_roles) ? user.global_roles : [];

    this.$sessionMeta.innerHTML = "";
    const blocks = [
      ["User", `${user.display_name || user.username || "Unknown"} (${user.username || ""})`],
      ["Workspace", `${workspaceLabel} (${roleLabel})`],
      ["Global Roles", roles.join(", ") || "none"],
    ];
    blocks.forEach(([label, value]) => {
      const div = document.createElement("div");
      div.innerHTML = `<strong>${label}:</strong> ${value}`;
      this.$sessionMeta.appendChild(div);
    });

    const memberships = Array.isArray(data.memberships) ? data.memberships : [];
    this.$workspaceSelect.innerHTML = "";
    memberships.forEach((m) => {
      const option = document.createElement("option");
      option.value = m.workspace_key;
      option.textContent = `${m.workspace_key} (${m.role})`;
      if (m.workspace_key === data.workspace_key) option.selected = true;
      this.$workspaceSelect.appendChild(option);
    });
    if (!memberships.length) {
      const option = document.createElement("option");
      option.value = data.workspace_key || "default";
      option.textContent = data.workspace_key || "default";
      this.$workspaceSelect.appendChild(option);
    }
  }

  async signIn() {
    try {
      const payload = {
        username: this.$username.value.trim(),
        workspace_key: this.$workspace.value.trim() || undefined,
      };
      const data = await request("/controlPlane/login", { method: "POST", body: payload });
      this.session = data.session;
      this.render();
      await this.refresh();
      this.setStatus("Signed in.", "success");
    } catch (e) {
      this.setStatus("Sign in failed: " + (e.message || e), "error");
    }
  }

  async switchWorkspace() {
    try {
      const username = this.session?.user?.username;
      if (!username) throw new Error("No signed-in user");
      const data = await request("/controlPlane/login", {
        method: "POST",
        body: { username, workspace_key: this.$workspaceSelect.value }
      });
      this.session = data.session;
      await this.refresh();
      this.setStatus("Workspace switched.", "success");
    } catch (e) {
      this.setStatus("Workspace switch failed: " + (e.message || e), "error");
    }
  }

  async signOut() {
    try {
      await request("/controlPlane/logout", { method: "POST", body: {} });
      this.session = null;
      this.render();
      await this.refresh();
      this.setStatus("Signed out.", "success");
    } catch (e) {
      this.setStatus("Sign out failed: " + (e.message || e), "error");
    }
  }

  async createUser() {
    try {
      const roles = this.$newRoles.value.split(",").map((x) => x.trim()).filter(Boolean);
      await request("/controlPlane/users", {
        method: "POST",
        body: {
          username: this.$newUsername.value.trim(),
          display_name: this.$newDisplayName.value.trim(),
          global_roles: roles
        }
      });
      this.$newUsername.value = "";
      this.$newDisplayName.value = "";
      this.$newRoles.value = "";
      this.setStatus("User saved.", "success");
    } catch (e) {
      this.setStatus("Save user failed: " + (e.message || e), "error");
    }
  }

  async grantAccess() {
    try {
      await request("/controlPlane/workspaceMembers", {
        method: "POST",
        body: {
          workspace_key: this.session?.workspace_key || "default",
          username: this.$memberUsername.value.trim(),
          role: this.$memberRole.value
        }
      });
      this.$memberUsername.value = "";
      await this.loadMembers();
      this.setStatus("Workspace access granted.", "success");
    } catch (e) {
      this.setStatus("Grant access failed: " + (e.message || e), "error");
    }
  }

  async loadMembers() {
    try {
      const data = await request(`/controlPlane/workspaceMembers?workspace_key=${encodeURIComponent(this.session?.workspace_key || "default")}`);
      const members = Array.isArray(data.members) ? data.members : [];
      this.$memberList.innerHTML = "";
      members.forEach((member) => {
        const el = document.createElement("div");
        el.className = "member";
        const roles = Array.isArray(member.global_roles) ? member.global_roles.join(", ") : "";
        el.innerHTML = `
          <strong>${member.display_name || member.username}</strong>
          <div>${member.username} • ${member.role}</div>
          <div>${roles || "no global roles"}</div>`;
        this.$memberList.appendChild(el);
      });
    } catch (e) {
      this.setStatus("Failed to load members: " + (e.message || e), "error");
    }
  }
}

customElements.define("session-control", SessionControl);
