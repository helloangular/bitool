import { deleteBtn, storeData } from "./utils.js";
import EventHandler from "../../library/eventHandler.js";

const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="section">
        <label>Case</label>
        <textarea></textarea>
    </div>
    <div class="section">
        <input type="text" name="do-this" />
        <label class="margin-left">Group Name</label>
        <input type="text" class="margin-left" name="group-name" />
    </div>
    <div id="list" class="section"></div>
    <button type="button" class="button" id="add-Case-btn">Add Case</button>
    <div class="section padding">
        <label>Default Group</label>
        <input type="text" name="default" />
    </div>
    <button type="button" class="button" id="save-btn">Save</button>
`;

class Case extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));
    }

    connectedCallback() {
        this.addCaseBtn = this.shadowRoot.querySelector("#add-Case-btn");
        this.saveBtn = this.shadowRoot.querySelector("#save-btn");
        this.list = this.shadowRoot.querySelector("#list");
        this.default = this.shadowRoot.querySelector('input[name="default"]');
        this.setUpEventListener();
    }

    setUpEventListener() {
        EventHandler.on(this.addCaseBtn, 'click', () => this.addCase(this.list));
        EventHandler.on(this.saveBtn, 'click', () => this.save(this));
    }

    addCase(list) {
        const caseSection = this.shadowRoot.querySelectorAll('.section')[0].cloneNode(true);
        const doSection = this.shadowRoot.querySelectorAll('.section')[1].cloneNode(true);
        const container = document.createElement("div");
        container.append(caseSection, doSection);
        container.appendChild(deleteBtn(container));
        list.insertBefore(container, list.firstChild);
        this.resetFields(this.shadowRoot);
    }

    resetFields(container) {
        container.querySelector('textarea').value = "";
        container.querySelector('input[name="do-this"]').value = "";
        container.querySelector('input[name="group-name"]').value = "";
    }

    collectData() {
        const branches = [];
        branches.push({
            condition: this.shadowRoot.querySelector('textarea').value,
            value: this.shadowRoot.querySelector('input[name="do-this"]').value,
            group: this.shadowRoot.querySelector('input[name="group-name"]').value
        });
        for (const content of this.list.childNodes) {
            branches.push({
                condition: content.querySelector('textarea')?.value || "",
                value: content.querySelector('input[name="do-this"]')?.value || "",
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
            this.shadowRoot.querySelector('input[name="do-this"]').value = branches[0].value || "";
            this.shadowRoot.querySelector('input[name="group-name"]').value = branches[0].group || "";
        }
        for (let i = 1; i < branches.length; i++) {
            this.addCase(this.list);
            const last = this.list.children[0];
            const ta = last.querySelector('textarea');
            if (ta) ta.value = branches[i].condition || "";
            const doThis = last.querySelector('input[name="do-this"]');
            if (doThis) doThis.value = branches[i].value || "";
            const gn = last.querySelector('input[name="group-name"]');
            if (gn) gn.value = branches[i].group || "";
        }
        this.default.value = data.default_branch || "";
    }

    save(object) {
        const d = this.collectData();
        storeData({
            cond_type: "case",
            branches: d.branches,
            default_branch: d.default_branch
        });
    }
}

customElements.define('my-case', Case);
