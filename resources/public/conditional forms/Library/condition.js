import { deleteBtn, storeData } from "./utils.js";
import EventHandler from "../../library/eventHandler.js";

const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="section">
        <textarea></textarea>
        <label class="margin-left">Group Name</label>
        <input type="text" class="margin-left" name="group-name" />
    </div>
    <div id="list" class="section"></div>
    <button type="button" class="button" id="add-condition-btn">Add Condition</button>
    <div class="section padding">
        <label>Default Group</label>
        <input type="text" name="default" />
    </div>
    <button type="button" class="button" id="save-btn">Save</button>
`;

class Condition extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));
    }

    connectedCallback() {
        this.addConditionBtn = this.shadowRoot.querySelector("#add-condition-btn");
        this.saveBtn = this.shadowRoot.querySelector("#save-btn");
        this.list = this.shadowRoot.querySelector("#list");
        this.default = this.shadowRoot.querySelector('input[name="default"]');
        this.setUpEventListener();
    }

    setUpEventListener() {
        EventHandler.on(this.addConditionBtn, 'click', () => this.addCondition(this.list));
        EventHandler.on(this.saveBtn, 'click', () => this.save(this));
    }

    addCondition(list) {
        const section = this.shadowRoot.querySelectorAll('.section')[0].cloneNode(true);
        const container = document.createElement("div");
        container.append(section);
        container.appendChild(deleteBtn(container));
        list.insertBefore(container, list.firstChild);
        this.resetFields(this.shadowRoot);
    }

    resetFields(container) {
        container.querySelector('textarea').value = "";
        container.querySelector('input[name="group-name"]').value = "";
    }

    collectData() {
        const branches = [];
        branches.push({
            condition: this.shadowRoot.querySelector('textarea').value,
            group: this.shadowRoot.querySelector('input[name="group-name"]').value
        });
        for (const content of this.list.childNodes) {
            branches.push({
                condition: content.querySelector('textarea')?.value || "",
                group: content.querySelector('input[name="group-name"]')?.value || ""
            });
        }
        return {
            branches,
            default_branch: this.default.value
        };
    }

    loadData(data) {
        const branches = data.branches || [];
        while (this.list.firstChild) this.list.removeChild(this.list.firstChild);

        if (branches.length > 0) {
            this.shadowRoot.querySelector('textarea').value = branches[0].condition || "";
            this.shadowRoot.querySelector('input[name="group-name"]').value = branches[0].group || "";
        }
        for (let i = 1; i < branches.length; i++) {
            this.addCondition(this.list);
            const last = this.list.children[0];
            const ta = last.querySelector('textarea');
            if (ta) ta.value = branches[i].condition || "";
            const gn = last.querySelector('input[name="group-name"]');
            if (gn) gn.value = branches[i].group || "";
        }
        this.default.value = data.default_branch || "";
    }

    save(object) {
        const d = this.collectData();
        storeData({
            cond_type: "cond",
            branches: d.branches,
            default_branch: d.default_branch
        });
    }
}

customElements.define('my-condition', Condition);
