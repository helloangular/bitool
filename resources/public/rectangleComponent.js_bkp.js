import "./source/modules/smart.grid.js";

const template = document.createElement("template");
template.innerHTML = `
<style>
  .rectangle-container {
    padding: 10px;
    border: 3px solid #004466;
    border-radius: 6px;
    cursor: pointer;
  }
</style>

<div class="rectangle-container">
  <div id="alias-info"></div>
</div>
`;

class RectangleComponent extends HTMLElement {

  constructor() {
    super();
    this.shadow = this.attachShadow({ mode: "open" });
    this.shadow.append(template.content.cloneNode(true));

    this.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      this.openMenu(this.getAttribute("id"), e.clientX, e.clientY);
    });

    this.shadow.addEventListener("click", () => {
      this.handleClick();
    });

    this.updateAlias(this.getAttribute("alias") || "");
    this.container = this.shadow.querySelector(".rectangle-container");
  }

  static get observedAttributes() {
    return ["alias", "style"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "alias") {
      this.updateAlias(newValue);
    }
  }

  async handleClick() {
    window.data.selectedRectangle = this.getAttribute("id") || "";
    
    let existingRectangle;

    try {
      this.style.cursor = "wait";

      const response = await fetch(
        `/getItem?id=${this.getAttribute("id")}`,
        {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
          },
        }
      );

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      existingRectangle = await response.json();
      if (!window.data.rectangles) {
        window.data.rectangles = [];
      }

      const index = window.data.rectangles?.findIndex(
        (rect) => rect.id === existingRectangle.id
      );
      if (index !== -1) {
        window.data.rectangles[index] = existingRectangle; // Replace the value
      } else {
        window.data.rectangles.push(existingRectangle); // Add the value if it doesn't exist
      }
    } catch (error) {
      console.error(error);
    } finally {
      this.style.cursor = "pointer";
    }

    const currentRect = window.data.rectangles?.find(
      (rect) => rect.id === this.getAttribute("id")
    );
    const splist = ["filter", "function"];
    window.data.selectedRectangle = currentRect.id;
    if (splist.includes(currentRect.btype)) {
      const filterComponent = document.querySelector("filter-component");
      filterComponent.setAttribute("visibility", "open");
    }
    else if (currentRect.btype == 'projection') {
      const projectionComponent = document.querySelector("projection-component");
      projectionComponent.setAttribute("visibility", "open");
    }
    else if (currentRect.btype == "join") {
      const joinEditorComponent = document.querySelector('join-editor-component');
      joinEditorComponent.setAttribute("visibility", "open");
    }
    else {
      const columnListComponent = document.querySelector("column-list-component");
      if (columnListComponent) {
        columnListComponent.setAttribute("selectedRectangle", currentRect);
        columnListComponent.setAttribute("visibility", "open");
      }
    }
  }

  openMenu(id, x, y) {
    let menu = document.querySelector('floater-menu');
    if (!menu) {
      menu = document.createElement('floater-menu');
      document.body.appendChild(menu);
    }
    menu.showMenu(id, x, y);
  }

  updateAlias(alias) {
    this.shadow.querySelector("#alias-info").textContent = alias;
  }
}

class FloaterMenu extends HTMLElement {
  constructor() {
    super();

    // Attach a shadow DOM
    this.attachShadow({ mode: 'open' });

    // Style the menu
    const style = document.createElement('style');
    style.textContent = `
      .menu {
        position: absolute;
        background-color: white;
        border: 1px solid #ccc;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        padding: 10px;
        display: none;
        flex-direction: column;
        z-index: 1000;
      }
      .menu-item {
        padding: 8px;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .menu-item:hover {
        background-color: #f0f0f0;
      }
    `;

    // Create the menu container
    const menu = document.createElement('div');
    menu.className = 'menu';

    // Add some sample icons (menu items)
    ['filter', 'function', 'grid', 'aggregation'].forEach((label) => {
      const item = document.createElement('div');
      item.className = 'menu-item';
      item.textContent = label;

      // Event listener for clicks on menu items
      item.addEventListener('click', () => this.handleItemClick(this.getAttribute("id"), label));

      menu.appendChild(item);
    });

    // Append styles and menu to the shadow root
    this.shadowRoot.append(style, menu);
    this.menu = menu;
  }

  showMenu(id, x, y) {
    this.menu.style.display = 'flex';
    this.menu.style.left = `${x}px`;
    this.menu.style.top = `${y}px`;
    this.setAttribute("id", id);
  }

getParent(element) {
  // Placeholder function: Replace with actual logic
  console.log(`Getting parent of: ${element}`);
  return null; // Replace with actual parent retrieval logic
}

getChildren(element) {
  // Placeholder function: Replace with actual logic
  console.log(`Getting children of: ${element}`);
  return []; // Replace with actual children retrieval logic
}

getFirstChild(element) { 
	const children = getChildren(element); 
	return children.length > 0 ? children[0] : null; 
} 

processElement(element) { 
	if (["filter", "sorter"].includes(element)) { 
		return getFirstChild(element); 
	} 
	return element; 
}

removeFromList(list, elementsToRemove) { 
	return list.filter(item => !elementsToRemove.includes(item)); 
} 

processElementWithParent(element, parent, list) { 
	let newList = [...list]; // Create a copy to keep original list unchanged 
	if (parent !== null) { 
		if (["join", "union"].includes(parent)) { 
			newList = removeFromList(newList, ["join", "union"]); // Remove "join" & "union" } 
			newList = removeFromList(newList, [element]); // Remove element itself 
		} 
	}
	if (element === "output") { 
		return "target"; 
	} 
	else if (element === "target") { 
		return []; 
	}
	return newList; 
}

  getMenuItems(item) {

	const elements = [ "join", "union", "projection", "aggregation", "filter", "function", "sorter", "association", "lookup", "decision", "boolean_logic", "logic", "target", "output", "mapper" ];
        item = processElement(item);
        return processElementWithParent(item, getParent(item), elements); 

  }

  hideMenu() {
    this.menu.style.display = 'none';
  }

  handleItemClick(id, label) {
    // console.log(`Clicked on ${label}`);

    
    // Send an AJAX request
    fetch('/addFilter', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ftype: label, item: id }),
    })
      .then((response) => response.json())
      .then((data) => {
        console.log('Server response:', data);
        window.data.panelItems = data;
        localStorage.setItem('jqdata', JSON.stringify(window.data));
        if(label == "aggregation"){
          document.querySelector("aggregation-editor-component").setAttribute("visibility", "open");
          return
        } else if(label == "join"){
          document.querySelector("join-editor-component").setAttribute("visibility", "open");
          return
        } else if(label == "function"){
          document.querySelector("function-component").setAttribute("visibility", "open");
          return
        } else if(label == "grid"){
          this.viewTableGrid();
          return
        }

        const filterComponent = document.querySelector("filter-component");
        filterComponent.setAttribute("visibility", "open");
      })
      .catch((error) => {
        console.error('Error:', error);
      });

    // Hide the menu after clicking
    this.hideMenu();
  }
  viewTableGrid(){
    const grid = document.querySelector("smart-grid");
    grid.style.display = "block";
    grid.editing.enabled = true;
    grid.behavior = {
      columnResizeMode: 'growAndShrink'
    }
    grid.filtering = {
      enabled: true
    },
    grid.dataSource = new Smart.DataAdapter(
      {
        dataSource: [
          { "EmployeeID": 1, "FirstName": "Nancy", "LastName": "Davolio", "ReportsTo": 2, "Country": "USA", "Title": "Sales Representative", "HireDate": "1992-05-01 00:00:00", "BirthDate": "1948-12-08 00:00:00", "City": "Seattle", "Address": "507 - 20th Ave. E.Apt. 2A" },
          { "EmployeeID": 2, "FirstName": "Andrew", "LastName": "Fuller", "ReportsTo": null, "Country": "USA", "Title": "Vice President, Sales", "HireDate": "1992-08-14 00:00:00", "BirthDate": "1952-02-19 00:00:00", "City": "Tacoma", "Address": "908 W. Capital Way" },
          { "EmployeeID": 3, "FirstName": "Janet", "LastName": "Leverling", "ReportsTo": 2, "Country": "USA", "Title": "Sales Representative", "HireDate": "1992-04-01 00:00:00", "BirthDate": "1963-08-30 00:00:00", "City": "Kirkland", "Address": "722 Moss Bay Blvd." },
          { "EmployeeID": 4, "FirstName": "Margaret", "LastName": "Peacock", "ReportsTo": 2, "Country": "USA", "Title": "Sales Representative", "HireDate": "1993-05-03 00:00:00", "BirthDate": "1937-09-19 00:00:00", "City": "Redmond", "Address": "4110 Old Redmond Rd." },
          { "EmployeeID": 5, "FirstName": "Steven", "LastName": "Buchanan", "ReportsTo": 2, "Country": "UK", "Title": "Sales Manager", "HireDate": "1993-10-17 00:00:00", "BirthDate": "1955-03-04 00:00:00", "City": "London", "Address": "14 Garrett Hill" },
          { "EmployeeID": 6, "FirstName": "Michael", "LastName": "Suyama", "ReportsTo": 5, "Country": "UK", "Title": "Sales Representative", "HireDate": "1993-10-17 00:00:00", "BirthDate": "1963-07-02 00:00:00", "City": "London", "Address": "Coventry House Miner Rd." },
          { "EmployeeID": 7, "FirstName": "Robert", "LastName": "King", "ReportsTo": 5, "Country": "UK", "Title": "Sales Representative", "HireDate": "1994-01-02 00:00:00", "BirthDate": "1960-05-29 00:00:00", "City": "London", "Address": "Edgeham Hollow Winchester Way" },
          { "EmployeeID": 8, "FirstName": "Laura", "LastName": "Callahan", "ReportsTo": 2, "Country": "USA", "Title": "Inside Sales Coordinator", "HireDate": "1994-03-05 00:00:00", "BirthDate": "1958-01-09 00:00:00", "City": "Seattle", "Address": "4726 - 11th Ave. N.E." },
          { "EmployeeID": 9, "FirstName": "Anne", "LastName": "Dodsworth", "ReportsTo": 5, "Country": "UK", "Title": "Sales Representative", "HireDate": "1994-11-15 00:00:00", "BirthDate": "1966-01-27 00:00:00", "City": "London", "Address": "7 Houndstooth Rd." }
        ],
        dataFields:
          [
            'EmployeeID: number',
            'ReportsTo: number',
            'FirstName: string',
            'LastName: string',
            'Country: string',
            'City: string',
            'Address: string',
            'Title: string',
            'HireDate: date',
            'BirthDate: date'
          ]
      });
    grid.columns = [
      { label: 'ID', dataField: 'EmployeeID', width: 30 },
      { label: 'First Name', dataField: 'FirstName', width: 200 },
      { label: 'Last Name', dataField: 'LastName', width: 200 },
      { label: 'Title', dataField: 'Title', width: 160 },
      { label: 'Birth Date', dataField: 'BirthDate', cellsFormat: 'd', width: 120 },
      { label: 'Hire Date', dataField: 'HireDate', cellsFormat: 'd', width: 120 },
      { label: 'Address', dataField: 'Address', width: 250 },
      { label: 'City', dataField: 'City', width: 120 },
      { label: 'Country', dataField: 'Country' }
    ];

    // document.body.appendChild(grid);
  }
}

customElements.define("rectangle-component", RectangleComponent);
customElements.define('floater-menu', FloaterMenu);