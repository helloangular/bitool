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
    save(){
        const jsonData = {
            if: this.shadowRoot.querySelector('#if-condition').value,
            then: this.ifThen.value,
            elseif: this.shadowRoot.querySelector('#elseif-condition').value,
            elseifThen: this.elseifThen.value,
            else: this.else.value
        }
        console.log(jsonData);
        storeData(jsonData);
    }
}

customElements.define('if-elseif-else', IfElseIfElse);