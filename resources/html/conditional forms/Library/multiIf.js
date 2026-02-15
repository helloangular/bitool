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
        // Clone the IF and THEN sections
        const ifSection = this.shadowRoot.querySelectorAll('.section')[0].cloneNode(true);
        const thenSection = this.shadowRoot.querySelectorAll('.section')[1].cloneNode(true);
        // Remove checkbox and its label from the cloned IF section
        const checkbox = ifSection.querySelector("input[name='checkbox']");
        if (checkbox) checkbox.remove();
        const label = ifSection.querySelector("label[for='checkbox-label']");
        if (label) label.remove();
        // Create container and append
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

    save(object) {
        const jsonData = {
            conditions: [],
            default: object.default.value
        }
        // Add the first "if" and "then" values from the main fields
        jsonData.conditions.push({
            if: object.shadowRoot.querySelector("#condition").value,
            then: object.shadowRoot.querySelector('input[name="then"]').value
        });
        for (const content of object.list.childNodes) {
            jsonData.conditions.push({
                if: content.querySelector("#condition").value,
                then: content.querySelector("input[name='then']").value
            })
        }
        console.log(jsonData);
        storeData(jsonData);
    }
}

customElements.define('multi-if', MultiIf);