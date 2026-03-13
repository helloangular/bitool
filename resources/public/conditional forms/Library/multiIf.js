import { deleteBtn, storeData } from "./utils.js";
import EventHandler from "../../library/eventHandler.js";

const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="section bl bt br">
        <label>IF</label>
        <select class="margin-left" id="condition">
            <option value="Upper(country) = 'INDIA'">Upper(country) = 'INDIA'</option>
            <option value="True">True</option>
            <option value="False">False</option>
        </select>
        <input type="checkbox" name="checkbox" class="margin-left" />
        <label for="checkbox-label">condition</label>
    </div>
    <div class="section padding bl bb br">
        <label>Guard Name</label>
        <input type="text" name="then" />
    </div>
    <div id="list" class="section"></div>
    <button type="button" class="button" id="add-if-btn">Add If</button>
    <div class="section padding">
        <label>Default Group</label>
        <input type="text" name="default" />
    </div>
    <button type="button" class="button" id="save-btn">Save</button>
`;

class MultiIf extends HTMLElement {
    constructor() {
        super();
        const shadowRoot = this.attachShadow({ mode: 'open' });
        shadowRoot.appendChild(template.content.cloneNode(true));
    }

    connectedCallback() {
        this.ifDropdown = this.shadowRoot.querySelector("#condition");
        this.ifCheckbox = this.shadowRoot.querySelector('input[name="checkbox"]');
        this.ifThen = this.shadowRoot.querySelector('input[name="then"]');
        this.default = this.shadowRoot.querySelector('input[name="default"]');
        this.addIfBtn = this.shadowRoot.querySelector("#add-if-btn");
        this.saveBtn = this.shadowRoot.querySelector("#save-btn");
        this.list = this.shadowRoot.querySelector("#list");
        this.setUpEventListener();
    }

    setUpEventListener() {
        EventHandler.on(this.ifDropdown, 'change', (e) => this.handleDropdown(e.target));
        EventHandler.on(this.ifCheckbox, 'change', (e) => this.toggleElement(e.target));
        EventHandler.on(this.addIfBtn, 'click', () => this.addIf(this.list));
        EventHandler.on(this.saveBtn, 'click', () => this.save(this));
    }

    handleDropdown(select) {
        let optionsList = select.querySelectorAll("option");
        let value = select.value;
        for (const option of optionsList) {
            if (option.value == value) option.setAttribute("selected", true);
            else option.removeAttribute("selected");
        }
    }

    toggleElement(checkbox) {
        const newElement = document.createElement(checkbox.checked ? 'textarea' : 'select');
        newElement.id = "condition";
        newElement.classList.add("margin-left");
        if (!checkbox.checked) {
            newElement.innerHTML = `
                <option value="Upper(country) = 'INDIA'">Upper(country) = 'INDIA'</option>
                <option value="True">True</option>
                <option value="False">False</option>
            `;
        }
        this.shadowRoot.querySelector("#condition").replaceWith(newElement);
    }

    addIf(list) {
        const ifSection = this.shadowRoot.querySelectorAll('.section')[0].cloneNode(true);
        const thenSection = this.shadowRoot.querySelectorAll('.section')[1].cloneNode(true);
        const checkbox = ifSection.querySelector("input[name='checkbox']");
        if (checkbox) checkbox.remove();
        const label = ifSection.querySelector("label[for='checkbox-label']");
        if (label) label.remove();
        const container = document.createElement("div");
        container.append(ifSection, thenSection);
        container.appendChild(deleteBtn(container));
        list.insertBefore(container, list.firstChild);
        this.resetFields(this.shadowRoot);
    }

    resetFields(container) {
        container.querySelector("#condition").value = "Upper(country) = 'INDIA'";
        container.querySelector('input[name="checkbox"]').checked = false;
        container.querySelector('input[name="then"]').value = "";
    }

    collectData() {
        const branches = [];
        // First branch from main fields
        branches.push({
            condition: this.shadowRoot.querySelector("#condition").value,
            group: this.shadowRoot.querySelector('input[name="then"]').value
        });
        // Additional branches from list
        for (const content of this.list.childNodes) {
            branches.push({
                condition: content.querySelector("#condition")?.value || "",
                group: content.querySelector("input[name='then']")?.value || ""
            });
        }
        return {
            branches,
            default_branch: this.default.value
        };
    }

    loadData(data) {
        const branches = data.branches || [];
        // Clear existing list items
        while (this.list.firstChild) this.list.removeChild(this.list.firstChild);

        if (branches.length > 0) {
            const cond = this.shadowRoot.querySelector('#condition');
            const opts = [...(cond.options || [])].map(o => o.value);
            if (cond.tagName === 'SELECT' && !opts.includes(branches[0].condition)) {
                this.ifCheckbox.checked = true;
                this.toggleElement(this.ifCheckbox);
                this.shadowRoot.querySelector('#condition').value = branches[0].condition;
            } else {
                cond.value = branches[0].condition;
            }
            this.ifThen.value = branches[0].group || "";
        }
        // Add remaining branches
        for (let i = 1; i < branches.length; i++) {
            this.addIf(this.list);
            const items = this.list.children;
            const last = items[0]; // addIf inserts at the beginning
            const condEl = last.querySelector('#condition');
            if (condEl) condEl.value = branches[i].condition || "";
            const thenEl = last.querySelector("input[name='then']");
            if (thenEl) thenEl.value = branches[i].group || "";
        }
        this.default.value = data.default_branch || "";
    }

    save(object) {
        const d = this.collectData();
        storeData({
            cond_type: "multi-if",
            branches: d.branches,
            default_branch: d.default_branch
        });
    }
}

customElements.define('multi-if', MultiIf);
