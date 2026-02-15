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
    save(){
        const jsonData = {
            if: this.shadowRoot.querySelector('#condition').value,
            then: this.shadowRoot.querySelector('input[name="then"]').value,
            else: this.shadowRoot.querySelector('input[name="else"]').value
        }

        console.log(jsonData);
        storeData(jsonData);
    }
}

customElements.define('if-else-component', IfElseComponent);