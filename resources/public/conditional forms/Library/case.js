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
        // Clone the textarea and the do-this/group-name section
        const caseSection = this.shadowRoot.querySelectorAll('.section')[0].cloneNode(true);
        const doSection = this.shadowRoot.querySelectorAll('.section')[1].cloneNode(true);
        // Create container and append
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

    save(object) {
        const jsonData = {
            cases: [],
            default: object.default.value
        };
        // Add the first "case" and "do-this" values from the main fields
        jsonData.cases.push({
            case: object.shadowRoot.querySelector('textarea').value,
            doThis: object.shadowRoot.querySelector('input[name="do-this"]').value,
            groupName: object.shadowRoot.querySelector('input[name="group-name"]').value
        });
        for (const content of object.list.childNodes) {
            jsonData.cases.push({
                case: content.querySelector('textarea').value,
                doThis: content.querySelector('input[name="do-this"]').value,
                groupName: content.querySelector('input[name="group-name"]').value
            });
        }
        console.log(jsonData);
        storeData(jsonData);
    }
}

customElements.define('my-case', Case);