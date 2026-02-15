const template = document.createElement('template');
template.innerHTML = `
    <link rel="stylesheet" href="./app.css" />
    <link rel="stylesheet" href="./source/styles/smart.default.css" />
    <style>
        .container {
            display: flex;
            flex-direction: column;
            gap: 10px;
            height: 100%;
            box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
        }
        .smart-button {
            --smart-button-padding: 4px;
            --smart-border-width: 0;
            --smart-font-size: 20px;
            --smart-ui-state-hover: white;
        }
        .section {
            margin-top: 10px;
            margin-bottom: 10px;
        }
        .padding {
            padding: 10px;
        }
        .margin-left {
            margin-left: 10px;
        }
        .button {
            padding: 10px 20px;
            border: none;
            background-color: #4CAF50;
            color: white;
            text-align: center;
        }
    </style>
    <div class="container padding">
        <div style="display:flex;justify-content:flex-end">
            <smart-button content="&#9747;" id="closeButton" class="smart-button" style="margin-right:10px;"></smart-button>
        </div>
        <div style="display:flex;justify-content:space-between;">
            <h3 style="margin-left:10px;">Sorter</h3>
            <smart-button content="Save" id="saveButton" style="margin-right:10px;font-size:10px;"></smart-button>
        </div>
        <div id="content">
            <div class="section">
                <label>Name</label>
                <input type="text" class="margin-left" name="name">
            </div>
            <div class="section">
                <select name="option"></select>
                <select class="margin-left" name="order">
                    <option value="ASC">ASC</option>
                    <option value="DESC">DESC</option>
                </select>
            </div>
            <div id="list" class="section"></div>
            <button type="button" class="button" id="add-sorter-btn">Add Sorter</button>
        </div>
    </div>
`;

class SorterEditor extends HTMLElement {
    constructor() {
        super();
        const shadowRoot = this.attachShadow({ mode: 'open' });
        shadowRoot.appendChild(template.content.cloneNode(true));

        this.selectedRectangle = null;
        this.cacheElements();
        this.setUpEventListeners();
    }

    static get observedAttributes() {
        return ["visibility"];
    }

    connectedCallback() {
        this.updateVisibility();
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (name === "visibility") {
            this.updateVisibility();
            if (newValue === "open") {
                this.updateSelectedRectangle();
                this.populateDropDown();
            }
        }
    }

    cacheElements() {
        this.addSorterBtn = this.shadowRoot.querySelector("#add-sorter-btn");
        this.saveBtn = this.shadowRoot.querySelector("#saveButton");
        this.closeBtn = this.shadowRoot.querySelector("#closeButton");
        this.list = this.shadowRoot.querySelector("#list");
        this.content = this.shadowRoot.querySelector("#content");
        this.optionSelect = this.shadowRoot.querySelector('select[name="option"]');
        this.orderSelect = this.shadowRoot.querySelector('select[name="order"]');
    }

    setUpEventListeners() {
        this.addSorterBtn.addEventListener('click', () => this.addSorter());
        this.saveBtn.addEventListener('click', () => this.save());
        this.closeBtn.addEventListener('click', () => this.closePanel());
    }

    updateVisibility() {
        this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";
    }

    updateSelectedRectangle() {
        this.selectedRectangle = window.data.rectangles.find(
            (rectangle) => rectangle.id === window.data.selectedRectangle
        );
    }

    populateDropDown() {
        this.optionSelect.innerHTML = ''; // Clear existing options

        if (this.selectedRectangle && this.selectedRectangle.items) {
            this.selectedRectangle.items.forEach(item => {
                const option = document.createElement('option');
                option.value = item.technical_name;
                option.textContent = item.technical_name;
                this.optionSelect.appendChild(option);
            });
        }
    }

    closePanel() {
        this.setAttribute("visibility", "");
    }

    addSorter() {
        const secondSection = this.content.childNodes[3].cloneNode(true); // Assuming the second section is the fourth child

        // Clone the select elements with their selected values
        const clonedOptionSelect = this.optionSelect.cloneNode(true);
        clonedOptionSelect.value = this.optionSelect.value;

        const clonedOrderSelect = this.orderSelect.cloneNode(true);
        clonedOrderSelect.value = this.orderSelect.value;

        // Replace the original select elements with the cloned ones
        secondSection.querySelector('select[name="option"]').replaceWith(clonedOptionSelect);
        secondSection.querySelector('select[name="order"]').replaceWith(clonedOrderSelect);

        secondSection.appendChild(this.createDeleteButton(secondSection));

        this.list.insertBefore(secondSection, this.list.firstChild);
        this.resetFields();
    }

    resetFields() {
        this.optionSelect.selectedIndex = 0;
        this.orderSelect.selectedIndex = 0;
    }

    createDeleteButton(container) {
        const btn = document.createElement("button");
        btn.textContent = "Delete";
        btn.classList.add("button", "margin-left");
        btn.addEventListener("click", () => container.remove());
        return btn;
    }

    save() {
        const jsonData = {
            name: this.content.querySelector('input[name="name"]').value,
            sorters: []
        };
    
        // Add the first sorter values from the content element
        const firstItem = this.selectedRectangle.items.find(item => item.technical_name === this.optionSelect.value);
        if (firstItem) {
            jsonData.sorters.push({
                tid: firstItem.tid,
                option: this.optionSelect.value,
                order: this.orderSelect.value
            });
        }
    
        for (const content of this.list.childNodes) {
            const optionValue = content.querySelector('select[name="option"]').value;
            const item = this.selectedRectangle.items.find(item => item.technical_name === optionValue);
            if (item) {
                jsonData.sorters.push({
                    tid: item.tid,
                    option: optionValue,
                    order: content.querySelector('select[name="order"]').value
                });
            }
        }
    
        console.log(jsonData);
        this.handleAjaxCall(jsonData);
    }

    handleAjaxCall(dataObj) {
        fetch("/saveFunction", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(dataObj)
        }).then((response) => response.json())
            .then((data) => {
                this.selectedRectangle = data;
                this.setAttribute("visibility", "");
            }).catch((error) => {
                alert("Error: " + error);
                console.error("Error:", error);
            });
    }
}

customElements.define('sorter-editor', SorterEditor);
