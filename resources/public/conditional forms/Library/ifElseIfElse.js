import { storeData } from "./utils.js";
import EventHandler from "../../library/eventHandler.js";

const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <div class="section">
        <select id="if-condition">
            <option value="Upper(country) = 'INDIA'">Upper(country) = 'INDIA'</option>
            <option value="True">True</option>
            <option value="False">False</option>
        </select>
        <input type="checkbox" name="if-checkbox" class="margin-left" />
        <label>condition</label>
    </div>
    <div class="section padding">
        <label>Guard Name</label>
        <input type="text" name="if-then" />
    </div>
    <div class="section">
        <label>Else If</label>
        <select class="margin-left" id="elseif-condition">
            <option value="Upper(country) = 'INDIA'">Upper(country) = 'INDIA'</option>
            <option value="True">True</option>
            <option value="False">False</option>
        </select>
        <input type="checkbox" name="elseif-checkbox" class="margin-left" />
        <label>condition</label>
    </div>
    <div class="section padding">
        <label>Guard Name</label>
        <input type="text" name="elseif-then" />
    </div>
    <label>Else</label>
    <div class="section padding">
        <label>Guard Name</label>
        <input type="text" name="else" />
    </div>
    <button type="button" class="button">Save</button>
`;


class IfElseIfElse extends HTMLElement {
    constructor() {
        super();
        const shadowRoot = this.attachShadow({ mode: 'open' });
        shadowRoot.appendChild(template.content.cloneNode(true));
    }
    connectedCallback() {
        this.ifDropdown = this.shadowRoot.querySelector('#if-condition');
        this.ifCheckbox = this.shadowRoot.querySelector('input[name="if-checkbox"]');
        this.ifThen = this.shadowRoot.querySelector('input[name="if-then"]');
        this.elseifDropdown = this.shadowRoot.querySelector('#elseif-condition');
        this.elseifCheckbox = this.shadowRoot.querySelector('input[name="elseif-checkbox"]');
        this.elseifThen = this.shadowRoot.querySelector('input[name="elseif-then"]');
        this.else = this.shadowRoot.querySelector('input[name="else"]');
        this.saveButton = this.shadowRoot.querySelector('button');
        this.setUpEventListeners();
    }
    setUpEventListeners() {
        EventHandler.on(this.ifCheckbox, 'change', () => this.toggleElement(this.ifCheckbox, 'if-condition'));
        EventHandler.on(this.elseifCheckbox, 'change', () => this.toggleElement(this.elseifCheckbox, 'elseif-condition'));
        EventHandler.on(this.saveButton, 'click', () => this.save());
    }
    toggleElement(checkbox, id) {
        const newElement = document.createElement(checkbox.checked ? 'textarea' : 'select');
        const dropdown = this.shadowRoot.querySelector(`#${id}`);
        newElement.id = id;
        newElement.classList.add("margin-left")
        if (!checkbox.checked) {
            newElement.innerHTML = `
                <option value="Upper(country) = 'INDIA'">Upper(country) = 'INDIA'</option>
                <option value="True">True</option>
                <option value="False">False</option>
            `;
        }
        dropdown.replaceWith(newElement);
    }

    collectData() {
        return {
            branches: [
                {
                    condition: this.shadowRoot.querySelector('#if-condition').value,
                    group: this.ifThen.value
                },
                {
                    condition: this.shadowRoot.querySelector('#elseif-condition').value,
                    group: this.elseifThen.value
                }
            ],
            default_branch: this.else.value
        };
    }

    loadData(data) {
        const branches = data.branches || [];
        if (branches[0]) {
            const ifCond = this.shadowRoot.querySelector('#if-condition');
            const opts = [...(ifCond.options || [])].map(o => o.value);
            if (ifCond.tagName === 'SELECT' && !opts.includes(branches[0].condition)) {
                this.ifCheckbox.checked = true;
                this.toggleElement(this.ifCheckbox, 'if-condition');
                this.shadowRoot.querySelector('#if-condition').value = branches[0].condition;
            } else {
                ifCond.value = branches[0].condition;
            }
            this.ifThen.value = branches[0].group || "";
        }
        if (branches[1]) {
            const eifCond = this.shadowRoot.querySelector('#elseif-condition');
            const opts = [...(eifCond.options || [])].map(o => o.value);
            if (eifCond.tagName === 'SELECT' && !opts.includes(branches[1].condition)) {
                this.elseifCheckbox.checked = true;
                this.toggleElement(this.elseifCheckbox, 'elseif-condition');
                this.shadowRoot.querySelector('#elseif-condition').value = branches[1].condition;
            } else {
                eifCond.value = branches[1].condition;
            }
            this.elseifThen.value = branches[1].group || "";
        }
        this.else.value = data.default_branch || "";
    }

    save(){
        const d = this.collectData();
        storeData({
            cond_type: "if-elif-else",
            branches: d.branches,
            default_branch: d.default_branch
        });
    }
}

customElements.define('if-elseif-else', IfElseIfElse);
