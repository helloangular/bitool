/**
 * getRelativeBounds
 * @param {HTMLElement} parent
 * @param {HTMLElement} element
 * @returns Relative bounds.
 */
export function getRelativeBounds(parent, element) {
  parent = parent.getBoundingClientRect();
  element = element.getBoundingClientRect();
  return {
    top: element.top - parent.top,
    left: element.left - parent.left,
    right: element.right - parent.left,
    bottom: element.bottom - parent.top,
    width: element.width,  // Width and height remain the same
    height: element.height
  };
}

/**
 * CreateElement add attribute, style, class and child element.
 * @param {String} tag 
 * @param {Object} options: { className: String, text: String, html: String, style: { JS style properties and values}, attrs: [], children: []} 
 * @returns Element
 */
export function createElement(tag, options = {}) {
  const el = document.createElement(tag);

  if (options.className) el.className = options.className;
  if (options.text) el.textContent = options.text;
  if (options.html) el.innerHTML = options.html;
  if (options.attrs) {
    for (const [key, value] of Object.entries(options.attrs)) {
      el.setAttribute(key, value);
    }
  }
  if (options.style) {
    for (const [key, value] of Object.entries(options.style)) {
      el.style[key] = value;
    }
  }
  if (options.events) {
    for (const [event, handler] of Object.entries(options.events)) {
      el.addEventListener(event, handler);
    }
  }

  if (options.children) {
    options.children.forEach(child => {
      const childEl = createElement(child.tag, child.options);
      el.appendChild(childEl);
    }
    );
  }

  return el;
}

export function closeOpenedPanel() {
  const panels = [
    "function-component",
    "calculated-column-component",
    "association-editor-component",
    "filter-component",
    "join-editor-component",
    "aggregation-editor-component",
    "column-list-component",
    "sorter-editor",
    "union-editor-component",
    "mapping-editor",
    "transform-editor",
    "projection-component",
    "api-component",
    "control-flow-component"
  ];

  for (const componentName of panels) {
    if (document.querySelector(componentName).getAttribute("visibility") === "open")
      document.querySelector(componentName).setAttribute("visibility", "close")
  }

  if (document.querySelector("smart-grid").style.display && document.querySelector("smart-grid").style.display === "block") {
    document.querySelector("smart-grid").style.display = "none";
  }
}

/**
 * Get short form of given btype.
 * @param {String} btype: For Example: Fi, Fu, J, ...etc.
 * @returns String | undefined
 */
export function getShortBtype(btype) {
  const BTYPES = {
    "function": "Fu",
    "aggregation": "A",
    "sorter": "S",
    "union": "U",
    "mapping": "Mp",
    "filter": "Fi",
    "table": "T",
    "join": "J",
    "union": "U",
    "projection": "P",
    "target": "Tg",
    "api-connection": "Ap",
    "conditionals": "C",
    "grid": "G",
    "output": "O"
  }
  return BTYPES[btype]
}

/**
 * Get Icon by passing btype.
 * @param {String} btype: For Example: Fi, Fu, J, ...etc. 
 * @returns String | undefined
 */
export function getBtypeIcon(btype) {
  const ICON = {
    Fi: "&#128481;",
    Fu: "&#955;",
    J: "&#x2795;",
    A: "&#x1F4E6;",
    U: "&#8746;",
    S: "&#x25B2;",
    Mp: "&#x1F4CD;",
    Ap: "&Alpha;",
    C: "&#128295;",
    P: "&#960;"
  };
  return ICON[btype]
}


/**
 * Makes an HTTP request using Fetch API.
 * @param {string} url - The endpoint to request.
 * @param {object} options - The request options.
 * @param {"GET"|"POST"|"PUT"|"DELETE"} [options.method="GET"] - The HTTP method.
 * @param {object} [options.body] - The request payload.
 * @param {object} [options.headers] - Custom headers.
 * @returns {Promise<any>} - The parsed JSON response.
 */
export async function request(url, options = {}) {
  const {
    method = "GET",
    body,
    headers = {},
  } = options;

  const fetchOptions = {
    method,
    headers: {
      "Content-Type": body ? "application/json" : undefined,
      ...headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  };

  try {
    const response = await fetch(url, fetchOptions);

    const text = await response.text();
    const data = text ? JSON.parse(text) : null;

    if (!response.ok) {
      alert(data?.message || `Request failed (${response.status})`);
      throw new Error(data?.message || `HTTP ${response.status}`);
    }

    return data;
  } catch (error) {
    console.error("Request error:", error);
    throw { message: error.message };
  }
}


/**
 * Validate an object against a schema
 * @param {Object} data - The input object to validate
 * @param {Object} schema - Validation rules
 * @returns {Object} { valid: boolean, errors: [] }
 */
export function validateData(data, schema) {
  const errors = [];

  for (const key in schema) {
    const rules = schema[key];
    const value = data[key];

    // Required check
    if (rules.required && (value === undefined || value === null || value === "")) {
      errors.push(`${key} is required`);
      continue;
    }

    // Skip further checks if value is missing
    if (value === undefined || value === null || value === "") continue;

    // Type check
    if (rules.type && typeof value !== rules.type) {
      errors.push(`${key} must be of type ${rules.type}`);
    }

    // String length checks
    if (rules.type === "string") {
      if (rules.minLength && value.length < rules.minLength) {
        errors.push(`${key} must be at least ${rules.minLength} characters`);
      }
      if (rules.maxLength && value.length > rules.maxLength) {
        errors.push(`${key} must be at most ${rules.maxLength} characters`);
      }
    }

    // Number range checks
    if (rules.type === "number") {
      if (rules.min !== undefined && value < rules.min) {
        errors.push(`${key} must be >= ${rules.min}`);
      }
      if (rules.max !== undefined && value > rules.max) {
        errors.push(`${key} must be <= ${rules.max}`);
      }
    }

    // Regex pattern check
    if (rules.pattern && !rules.pattern.test(value)) {
      errors.push(`${key} is invalid`);
    }
  }

  return {
    valid: errors.length === 0,
    errors
  };
}

/**
 * Simple debounce helper.
 * Executes the function after `delay` ms of inactivity.
 * @param {Function} fn - Function to debounce.
 * @param {number} delay - Time to wait before execution (ms).
 * @returns {Function} Debounced version of the function.
 */
export function debounce(fn, delay = 150) {
  let timer;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), delay);
  };
}

/**
 * Show an asynchronous confirm dialog.
 * @param {string} message - The message shown in the dialog.
 * @returns {Promise<boolean>} Resolves true if user clicks OK, false if Cancel.
 */
export function customConfirm(message) {
  return new Promise((resolve) => {
    // Create backdrop
    const backdrop = document.createElement("div");
    backdrop.style.position = "fixed";
    backdrop.style.top = "0";
    backdrop.style.left = "0";
    backdrop.style.width = "100vw";
    backdrop.style.height = "100vh";
    backdrop.style.background = "rgba(0,0,0,0.4)";
    backdrop.style.display = "flex";
    backdrop.style.alignItems = "center";
    backdrop.style.justifyContent = "center";
    backdrop.style.zIndex = "9999";

    // Create dialog box
    const box = document.createElement("div");
    box.style.background = "#fff";
    box.style.padding = "20px";
    box.style.borderRadius = "8px";
    box.style.minWidth = "260px";
    box.style.textAlign = "center";
    box.style.fontFamily = "sans-serif";
    box.style.boxShadow = "0 2px 10px rgba(0,0,0,0.3)";

    // Message
    const msg = document.createElement("div");
    msg.textContent = message;
    msg.style.marginBottom = "15px";

    // Buttons
    const okBtn = document.createElement("button");
    okBtn.textContent = "Yes";
    okBtn.style.marginRight = "10px";

    const cancelBtn = document.createElement("button");
    cancelBtn.textContent = "No";

    // Resolve on click
    okBtn.onclick = () => {
      document.body.removeChild(backdrop);
      resolve(true);
    };

    cancelBtn.onclick = () => {
      document.body.removeChild(backdrop);
      resolve(false);
    };

    box.appendChild(msg);
    box.appendChild(okBtn);
    box.appendChild(cancelBtn);
    backdrop.appendChild(box);
    document.body.appendChild(backdrop);
  });
}

export function getPanelItems() { return window.data?.panelItems || []; }

export function setPanelItems(items) {
  window.data = window.data || {};
  window.data.panelItems = items;
  localStorage.setItem('jqdata', JSON.stringify(window.data));
}

/**
 * Safely convert any value to a text node (prevents XSS by not setting innerHTML).
 * @param {any} v
 * @returns {string}
 */
function toText(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

export function populateTable(tableEl, data) {
  if (typeof tableEl === 'string') tableEl = document.querySelector(tableEl);
  if (!tableEl) throw new Error('Table element not found');

  const thead = tableEl.querySelector('thead');
  const tbody = tableEl.querySelector('tbody');
  thead.innerHTML = '';
  tbody.innerHTML = '';

  if (!Array.isArray(data) || data.length === 0) {
    thead.appendChild(document.createElement('tr')).appendChild(document.createElement('th')).textContent = 'No data';
    return;
  }

  // Determine union of keys while preserving order: keys from first object, then any new keys from subsequent objects
  const seen = new Set();
  const columns = [];
  for (const obj of data) {
    if (obj && typeof obj === 'object') {
      for (const k of Object.keys(obj)) {
        if (!seen.has(k)) { seen.add(k); columns.push(k); }
      }
    }
  }

  // Build header row
  const headerRow = document.createElement('tr');
  for (const col of columns) {
    const th = document.createElement('th');
    th.textContent = col;
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);

  // Build body rows
  for (const rowObj of data) {
    const tr = document.createElement('tr');
    for (const col of columns) {
      const td = document.createElement('td');
      td.textContent = toText(rowObj ? rowObj[col] : '');
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

export function escapeHtml(unsafe) {
  if (unsafe === null || unsafe === undefined) return '';
  return String(unsafe).replace(/[&<>"]|'/g, (m) => {
    switch (m) {
      case '&': return '&amp;';
      case '<': return '&lt;';
      case '>': return '&gt;';
      case '"': return '&quot;';
      case "'": return '&#039;';
      default: return m;
    }
  });
}