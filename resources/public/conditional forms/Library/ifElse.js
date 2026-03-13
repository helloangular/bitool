import {storeData} from "./utils.js";
import EventHandler from "../../library/eventHandler.js";

const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="section">
        <label>IF</label>
        <select class="margin-left" id="condition">
            <option value="Upper(country) = 'INDIA'">Upper(country) = 'INDIA'</option>
            <option value="True">True</option>
            <option value="False">False</option>
        </select>
        <input type="checkbox" name="checkbox" class="margin-left" />
        <label>condition</label>
    </div>
    <div class="section padding">
        <label>Guard Name</label>
        <input type="text" name="then" />
    </div>
    <label>Else</label>
    <div class="section padding">
        <label>Guard Name</label>
        <input type="text" name="else" />
    </div>
    <button type="button" class="button">Save</button>
`;

class IfElseComponent extends HTMLElement {
    constructor() {
        super();
        const shadowRoot = this.attachShadow({ mode: 'open' });
        shadowRoot.appendChild(template.content.cloneNode(true));
    }
    connectedCallback() {
        this.checkbox = this.shadowRoot.querySelector('input[type="checkbox"]');
        this.saveButton = this.shadowRoot.querySelector('button');
        this.setUpEventListerners();
    }
    setUpEventListerners() {
        EventHandler.on(this.checkbox, 'change', () => this.toggle());
        EventHandler.on(this.saveButton, 'click', () => this.save());
    }
    toggle() {
        const conditionSelect = this.shadowRoot.querySelector('#condition');
        const newElement = document.createElement(this.checkbox.checked ? 'textarea' : 'select');

        if (this.checkbox.checked) {
            newElement.id = 'condition';
        } else {
            newElement.id = 'condition';
            newElement.innerHTML = `
                    <option value="Upper(country) = 'INDIA'">Upper(country) = 'INDIA'</option>
                    <option value="True">True</option>
                    <option value="False">False</option>
                `;
            newElement.value = conditionSelect.value;
        }

        conditionSelect.replaceWith(newElement);
    }

    collectData() {
        return {
            branches: [{
                condition: this.shadowRoot.querySelector('#condition').value,
                group: this.shadowRoot.querySelector('input[name="then"]').value
            }],
            default_branch: this.shadowRoot.querySelector('input[name="else"]').value
        };
    }

    loadData(data) {
        const branches = data.branches || [];
        if (branches.length > 0) {
            const cond = this.shadowRoot.querySelector('#condition');
            // If condition doesn't match a <select> option, switch to textarea
            const opts = [...(cond.options || [])].map(o => o.value);
            if (cond.tagName === 'SELECT' && !opts.includes(branches[0].condition)) {
                this.checkbox.checked = true;
                this.toggle();
                this.shadowRoot.querySelector('#condition').value = branches[0].condition;
            } else {
                cond.value = branches[0].condition;
            }
            this.shadowRoot.querySelector('input[name="then"]').value = branches[0].group || "";
        }
        this.shadowRoot.querySelector('input[name="else"]').value = data.default_branch || "";
    }

    save(){
        const d = this.collectData();
        storeData({
            cond_type: "if-else",
            branches: d.branches,
            default_branch: d.default_branch
        });
    }
}

customElements.define('if-else-component', IfElseComponent);
