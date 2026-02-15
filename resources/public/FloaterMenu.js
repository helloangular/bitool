class FloaterMenu extends HTMLElement {
  constructor() {
    super();
    // console.log("FloaterMenu Initialize");
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
    ['Icon 1', 'Icon 2', 'Icon 3'].forEach((label) => {
      const item = document.createElement('div');
      item.className = 'menu-item';
      item.textContent = label;

      // Event listener for clicks on menu items
      item.addEventListener('click', () => this.handleItemClick(label));

      menu.appendChild(item);
    });

    // Append styles and menu to the shadow root
    this.shadowRoot.append(style, menu);
    this.menu = menu;
  }

  showMenu(x, y) {
    this.menu.style.display = 'flex';
    this.menu.style.left = `${x}px`;
    this.menu.style.top = `${y}px`;
  }

  hideMenu() {
    this.menu.style.display = 'none';
  }

  handleItemClick(label) {
    // console.log(`Clicked on ${label}`);

    // Send an AJAX request
    fetch('/submit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: label }),
    })
      .then((response) => response.json())
      .then((data) => {
        // console.log('Server response:', data);
        alert(`Server responded: ${JSON.stringify(data)}`);
      })
      .catch((error) => {
        console.error('Error:', error);
      });

    // Hide the menu after clicking
    this.hideMenu();
  }
}

