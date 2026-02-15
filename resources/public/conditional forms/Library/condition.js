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
        // Clone the textarea/group-name section
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

    save(object) {
        const jsonData = {
            conditions: [],
            default: object.default.value
        };
        // Add the first "condition" and "group-name" values from the main fields
        jsonData.conditions.push({
            condition: object.shadowRoot.querySelector('textarea').value,
            groupName: object.shadowRoot.querySelector('input[name="group-name"]').value
        });
        for (const content of object.list.childNodes) {
            jsonData.conditions.push({
                condition: content.querySelector('textarea').value,
                groupName: content.querySelector('input[name="group-name"]').value
            });
        }
        console.log(jsonData);
        storeData(jsonData);
    }
}

customElements.define('my-condition', Condition);