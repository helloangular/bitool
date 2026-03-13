import {deleteBtn, storeData} from "./utils.js";
import EventHandler from "../../library/eventHandler.js";

const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="section">
        <button id="add-column-btn">Add Column</button>
        <button id="add-Expression-btn">Add Expression</button>
    </div>
    <div class="section">
        <label>Guard</label>
        <input type="text" name="guard" />
    </div>
    <div class="section">
        <label class="margin-left">Group</label>
        <input type="text" class="margin-left" name="group" />
    </div>
    <div id="list" class="section"></div>
    <button type="button" class="button" id="add-pattern-btn">Add Pattern</button>
    <div class="section padding">
        <label>Default Group</label>
        <input type="text" name="default" />
    </div>
    <button type="button" class="button" id="save-btn">Save</button>
`;

class PatternMatchEditor extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));
    }

    connectedCallback() {
        this.addColumnBtn = this.shadowRoot.querySelector("#add-column-btn");
        this.addExpressionBtn = this.shadowRoot.querySelector("#add-Expression-btn");
        this.addPatternBtn = this.shadowRoot.querySelector("#add-pattern-btn");
        this.saveBtn = this.shadowRoot.querySelector("#save-btn");
        this.list = this.shadowRoot.querySelector("#list");
        this.default = this.shadowRoot.querySelector('input[name="default"]');
        this.setUpEventListener();
    }

    setUpEventListener() {
        EventHandler.on(this.addColumnBtn, 'click', () => this.addColumn(this.addColumnBtn.parentElement));
        EventHandler.on(this.addExpressionBtn, 'click', () => this.addExpression(this.addExpressionBtn.parentElement));
        EventHandler.on(this.addPatternBtn, 'click', () => this.addPattern(this.list));
        EventHandler.on(this.saveBtn, 'click', () => this.save(this));
    }

    addColumn(parentElement) {
        if (this.list.childNodes.length > 0) {
            alert("Please empty the list before adding new fields or dropdowns.");
            return;
        }
        const selectElement = document.createElement("select");
        selectElement.classList.add("margin-left");
        selectElement.innerHTML = `
            <option value="Column1">Column1</option>
            <option value="Column2">Column2</option>
            <option value="Column3">Column3</option>
        `;
        parentElement.insertBefore(selectElement, this.addColumnBtn);

        const inputElement = document.createElement("input");
        inputElement.type = "text";
        inputElement.classList.add("margin-left");

        const guardSection = parentElement.nextElementSibling;
        guardSection.insertBefore(inputElement, guardSection.querySelector('label'));
    }

    addExpression(parentElement) {
        if (this.list.childNodes.length > 0) {
            alert("Please empty the list before adding new fields or dropdowns.");
            return;
        }
        const inputElement1 = document.createElement("input");
        inputElement1.type = "text";
        inputElement1.classList.add("margin-left");
        parentElement.insertBefore(inputElement1, this.addColumnBtn);

        const inputElement2 = document.createElement("input");
        inputElement2.type = "text";
        inputElement2.classList.add("margin-left");

        const guardSection = parentElement.nextElementSibling;
        guardSection.insertBefore(inputElement2, guardSection.querySelector('label'));
    }

    addPattern(list) {
        const guardSection = this.shadowRoot.querySelectorAll('.section')[1].cloneNode(true);
        const groupSection = this.shadowRoot.querySelectorAll('.section')[2].cloneNode(true);
        const container = document.createElement("div");
        container.append(guardSection, groupSection);
        container.appendChild(deleteBtn(container));
        list.insertBefore(container, list.firstChild);
        this.resetFields(this.shadowRoot);
    }

    resetFields(container) {
        container.querySelector('input[name="guard"]').value = "";
        container.querySelector('input[name="group"]').value = "";
    }

    collectData() {
        const branches = [];
        // First pattern from main fields
        branches.push({
            guard: this.shadowRoot.querySelector('input[name="guard"]').value,
            group: this.shadowRoot.querySelector('input[name="group"]').value
        });
        // Additional patterns from list
        for (const content of this.list.childNodes) {
            const guardSection = content.querySelectorAll('.section')[0];
            const groupSection = content.querySelectorAll('.section')[1];
            branches.push({
                guard: guardSection?.querySelector('input[name="guard"]')?.value || "",
                group: groupSection?.querySelector('input[name="group"]')?.value || ""
            });
        }
        // Collect headers from first section
        const firstSection = this.shadowRoot.querySelector('.section');
        const headerInputs = firstSection.querySelectorAll('input[type="text"], select');
        const headers = [];
        headerInputs.forEach(input => headers.push(input.value));

        return {
            branches,
            default_branch: this.default.value,
            headers
        };
    }

    loadData(data) {
        const branches = data.branches || [];
        while (this.list.firstChild) this.list.removeChild(this.list.firstChild);

        if (branches.length > 0) {
            this.shadowRoot.querySelector('input[name="guard"]').value = branches[0].guard || "";
            this.shadowRoot.querySelector('input[name="group"]').value = branches[0].group || "";
        }
        for (let i = 1; i < branches.length; i++) {
            this.addPattern(this.list);
            const last = this.list.children[0];
            const guardEl = last.querySelector('input[name="guard"]');
            if (guardEl) guardEl.value = branches[i].guard || "";
            const groupEl = last.querySelector('input[name="group"]');
            if (groupEl) groupEl.value = branches[i].group || "";
        }
        this.default.value = data.default_branch || "";
    }

    save(object) {
        const d = this.collectData();
        storeData({
            cond_type: "pattern-match",
            branches: d.branches,
            default_branch: d.default_branch
        });
    }
}

customElements.define('pattern-match-editor', PatternMatchEditor);
