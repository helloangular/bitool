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

// testlib.js
const tests = [];

// Collect tests instead of running immediately
export function test(name, fn) {
  tests.push({ name, fn });
}

export function expect(received) {
  return {
    toBe(expected) {
      if (received !== expected) {
        throw new Error(`Expected ${expected} but got ${received}`);
      }
    },
    toEqual(expected) {
      const r = JSON.stringify(received);
      const e = JSON.stringify(expected);
      if (r !== e) {
        throw new Error(`Expected ${e} but got ${r}`);
      }
    }
  };
}

// Run all tests sequentially
export async function runTests() {
  for (const { name, fn } of tests) {
    try {
      await fn(); // works for sync + async
      console.log(`✅ PASS: ${name}`);
    } catch (error) {
      console.error(`❌ FAIL: ${name}`);
      console.error("   ", error);
    }
  }
  tests.length = 0;
}

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
    options.children.forEach(child => el.appendChild(child));
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
    "kafka-source-component",
    "file-source-component",
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
    "kafka-source": "Kf",
    "file-source": "Fs",
    "conditionals": "C",
    "grid": "G",
    "output": "O"
  }
  return BTYPES[btype]
}

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
    Kf: "&#9107;",
    Fs: "&#128194;",
    C: "&#128295;",
    P: "&#960;"
  };
  return ICON[btype]
}

export function request(url, options) {
  return new Promise((resolve, reject) => {
    fetch(url, {
        method: options.method,
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(options.body)
      }).then((response) => response.json())
        .then((data) => {
          resolve(data);
        }).catch((error) => {
          reject({message: error.message});
        });
  });
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

export class StateManager {
  constructor(initialState = {}) {
    // backing storage
    this._currentState = { ...initialState };
    this._newState = { ...initialState };

    // dynamically define getters/setters for each key
    for (const key of Object.keys(initialState)) {
      Object.defineProperty(this, key, {
        get: () => this._newState[key],
        set: (value) => {
          this._newState[key] = value;
        },
        enumerable: true
      });
    }
  }

  // check if a single field changed
  isFieldDirty(key) {
    return this._currentState[key] !== this._newState[key];
  }

  // return all dirty fields
  getDirtyFields() {
    const dirty = {};
    for (const key in this._newState) {
      if (this.isFieldDirty(key)) {
        dirty[key] = {
          from: this._currentState[key],
          to: this._newState[key]
        };
      }
    }
    return dirty;
  }

  // check if any change exists
  isDirty() {
    return Object.keys(this.getDirtyFields()).length > 0;
  }

  // reset current state to new state (e.g., after save)
  commit() {
    this._currentState = { ...this._newState };
  }

  // reset new state back to original
  revert() {
    this._newState = { ...this._currentState };
  }
}

export class SvgConnector {
  constructor(container) {
    this.container = container;
    this.nodes = new Map();       // id => { el, observer }
    this.connections = new Map(); // connId => { from, to, pathEl, options }
    this._idCounter = 1;

    // create SVG layer
    this.svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    this.svg.classList.add("connector-layer");
    this.svg.setAttribute("preserveAspectRatio", "none");

    // marker defs
    const defs = document.createElementNS(this.svg.namespaceURI, "defs");
    defs.innerHTML = `
      <marker id="arrow" markerWidth="10" markerHeight="10" refX="10" refY="5"
              orient="auto" markerUnits="strokeWidth">
        <path d="M0,0 L10,5 L0,10 z" fill="#374151"></path>
      </marker>
      `;

    // this.svg.appendChild(rect);
    this.svg.appendChild(defs);

    // group for paths
    this.pathGroup = document.createElementNS(this.svg.namespaceURI, "g");
    this.svg.appendChild(this.pathGroup);

    // ensure container is positioned
    this.container.style.position =
      getComputedStyle(this.container).position === "static" ? "relative"
        : this.container.style.position;
    this.container.prepend(this.svg);

    // bound methods
    this._onWindowResize = this._onWindowResize.bind(this);
    window.addEventListener("resize", this._onWindowResize);
  }

  // ---------- Public API ----------

  addNode(el, id = null) {
    if (!id) id = `n${this._idCounter++}`;
    if (this.nodes.has(id)) throw new Error("Node id already exists: " + id);

    this.nodes.set(id, { el, id });

    const ro = new ResizeObserver(() => this._scheduleUpdate());
    ro.observe(el);
    this.nodes.get(id).observer = ro;

    this._scheduleUpdate();
    return id;
  }

  removeNode(id) {
    const node = this.nodes.get(id);
    if (!node) return;
    if (node.observer) node.observer.disconnect();
    this.nodes.delete(id);

    // remove connections referencing this node
    for (const [cid, conn] of [...this.connections.entries()]) {
      if (conn.from === id || conn.to === id) {
        this._removeConnectionElement(cid);
        this.connections.delete(cid);
      }
    }
    this._scheduleUpdate();
  }

  connect(fromId, toId, opts = {}) {
    if (!this.nodes.has(fromId) || !this.nodes.has(toId)) {
      throw new Error("Both nodes must be added before connecting.");
    }

    // ✅ check if connection already exists
    if (this.hasConnection(fromId, toId)) return false;

    const id = `c${this._idCounter++}`;
    const path = document.createElementNS(this.svg.namespaceURI, "path");
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", opts.stroke || "#374151");
    path.setAttribute("stroke-width", String(opts.strokeWidth || 2));
    if (opts.dasharray) path.setAttribute("stroke-dasharray", opts.dasharray);
    if (opts.arrow) path.setAttribute("marker-end", "url(#arrow)");
    // path.style.pointerEvents = "none";

    this.pathGroup.appendChild(path);
    this.connections.set(id, { from: fromId, to: toId, pathEl: path, options: opts });

    this._scheduleUpdate();
    return id;
  }

  hasConnection(fromId, toId) {
    for (const conn of this.connections.values()) {
      if (conn.from === fromId && conn.to === toId) return true;
    }
    return false;
  }

  isOneOfThemConnected(fromId, toId) {
    for (const conn of this.connections.values()) {
      if (conn.from === fromId || conn.to === toId) return true;
    }
    return false;
  }

  disconnect(connId) {
    if (!this.connections.has(connId)) return;
    this._removeConnectionElement(connId);
    this.connections.delete(connId);
  }

  clear() {
    this.pathGroup.innerHTML = "";
    this.connections.clear();
    for (const node of this.nodes.values()) {
      if (node.observer) node.observer.disconnect();
    }
    for (const child of this.svg.childNodes) {
      if (child.nodeName !== "defs" && child.nodeName !== "g") child.remove();
    }
    this.nodes.clear();
    this._idCounter = 1;
  }

  destroy() {
    window.removeEventListener("resize", this._onWindowResize);
    this.clear();
    if (this.svg.parentNode) this.svg.parentNode.removeChild(this.svg);
  }

  // ---------- Internal helpers ----------

  _removeConnectionElement(connId) {
    const conn = this.connections.get(connId);
    if (conn?.pathEl?.parentNode) {
      conn.pathEl.parentNode.removeChild(conn.pathEl);
    }
  }

  _onWindowResize() {
    this._scheduleUpdate();
  }

  _scheduleUpdate() {
    if (this._frame) return;
    this._frame = requestAnimationFrame(() => {
      this._frame = null;
      this._updateAll();
    });
  }

  _updateAll() {
    this.connections.forEach((conn) => {
      const fromNode = this.nodes.get(conn.from);
      const toNode = this.nodes.get(conn.to);
      if (!fromNode || !toNode) return;
      const p = this._computePathForPair(fromNode.el, toNode.el, conn.options);
      conn.pathEl.setAttribute("d", p);
    });
  }

  _computePathForPair(aEl, bEl, opts = {}) {
    const aRect = aEl.getBoundingClientRect();
    const bRect = bEl.getBoundingClientRect();
    const containerRect = this.container.getBoundingClientRect();

    const ax = aRect.left - containerRect.left;
    const ay = aRect.top - containerRect.top;
    const bx = bRect.left - containerRect.left;
    const by = bRect.top - containerRect.top;

    const anchorsA = [
      { x: ax + aRect.width / 2, y: ay },
      { x: ax + aRect.width / 2, y: ay + aRect.height },
      { x: ax, y: ay + aRect.height / 2 },
      { x: ax + aRect.width, y: ay + aRect.height / 2 },
    ];
    const anchorsB = [
      { x: bx + bRect.width / 2, y: by },
      { x: bx + bRect.width / 2, y: by + bRect.height },
      { x: bx, y: by + bRect.height / 2 },
      { x: bx + bRect.width, y: by + bRect.height / 2 },
    ];

    let best = null;
    for (const a of anchorsA) {
      for (const b of anchorsB) {
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const dist = Math.hypot(dx, dy);
        if (!best || dist < best.dist) best = { a, b, dist };
      }
    }
    if (!best) return "";

    const { a, b } = best;
    const offset = Math.min(Math.max(best.dist * 0.25, 30), 200);

    let c1 = { x: a.x, y: a.y };
    let c2 = { x: b.x, y: b.y };

    if (Math.abs(a.x - b.x) > Math.abs(a.y - b.y)) {
      c1.x = a.x + (a.x < b.x ? offset : -offset);
      c2.x = b.x + (b.x < a.x ? offset : -offset);
      c1.y = a.y;
      c2.y = b.y;
    } else {
      c1.y = a.y + (a.y < b.y ? offset : -offset);
      c2.y = b.y + (b.y < a.y ? offset : -offset);
      c1.x = a.x;
      c2.x = b.x;
    }

    const curveTweak = opts.curve || 0.5;
    const cx1 = a.x + (c1.x - a.x) * curveTweak;
    const cy1 = a.y + (c1.y - a.y) * curveTweak;
    const cx2 = b.x + (c2.x - b.x) * curveTweak;
    const cy2 = b.y + (c2.y - b.y) * curveTweak;

    return `M ${a.x} ${a.y} C ${cx1} ${cy1} ${cx2} ${cy2} ${b.x} ${b.y}`;
  }
}

export class FollowUpLine {
  constructor(svg) {
    this.svg = svg;
    this.svgBound = null;
    this.previewLine = null;
  }

  start(fromEl, ev) {
    if (fromEl) {
      this.svgBound = this.svg.getBoundingClientRect();
      const {x, y} = this.calculatePoint(fromEl, ev);

      this.previewLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
      this.previewLine.setAttribute("x1", x);
      this.previewLine.setAttribute("y1", y);
      this.previewLine.setAttribute("x2", x);
      this.previewLine.setAttribute("y2", y);
      this.previewLine.setAttribute("stroke", "#999");
      this.previewLine.setAttribute("stroke-dasharray", "4 2");
      this.previewLine.setAttribute("stroke-width", 2);

      this.svg.appendChild(this.previewLine);
    }
  }

  preview(ev) {
    this.previewLine.setAttribute("x2", ev.clientX - this.svgBound.left);
    this.previewLine.setAttribute("y2", ev.clientY - this.svgBound.top);
  }

  end(endRect = null, ev = null) {
    if (endRect, ev) {
      const point = this.calculatePoint(endRect, ev);

      this.svg.removeChild(this.previewLine);
      this.previewLine = null;
      
      return point
    } else {
      this.svg.removeChild(this.previewLine);
      this.previewLine = null;
    }
  }

  calculatePoint(el, ev) {
    const rect = el.getBoundingClientRect();
    // check whether click was closer to left or right edge
    const offsetX = ev.clientX - rect.left;
    const edge = (offsetX < rect.width / 2) ? "left" : "right";

    let x, y;
    if (edge === "left") {
      x = rect.left - this.svgBound.left; // left edge
      y = rect.top + rect.height / 2 - this.svgBound.top;
    } else {
      x = rect.right - this.svgBound.left; // right edge
      y = rect.top + rect.height / 2 - this.svgBound.top;
    }

    return {x, y}
  }
}

export class PanZoom {
  constructor(container, viewport) {
    this.container = container;
    this.viewport = viewport;

    this.scale = 1;
    this.minScale = 0.2;
    this.maxScale = 3;

    this.offsetX = 0;
    this.offsetY = 0;
    this.dragging = false;
    this.startX = 0;
    this.startY = 0;

    this.autoPanSpeed = 10;   // how fast to pan
    this.autoPanMargin = 40;  // px from edge to trigger auto-pan
    this.autoPanInterval = null;

    // this._init();
  }

  _init() {
    // Mouse wheel zoom
    this.container.addEventListener("wheel", (e) => {
      e.preventDefault();
      const scaleAmount = -e.deltaY * 0.001;
      const newScale = Math.min(this.maxScale, Math.max(this.minScale, this.scale + scaleAmount));

      // Zoom around mouse pointer
      const rect = this.container.getBoundingClientRect();
      const cx = e.clientX - rect.left;
      const cy = e.clientY - rect.top;

      this.offsetX -= (cx / this.scale - cx / newScale);
      this.offsetY -= (cy / this.scale - cy / newScale);

      this.scale = newScale;
      this._applyTransform();
    });
  }

  start(e) {
    // Dragging (panning)
    this.dragging = true;
    this.startX = e.clientX - this.offsetX;
    this.startY = e.clientY - this.offsetY;
    this.container.setPointerCapture(e.pointerId);
  }

  update(e) {
    if (!this.dragging) return;
    this.offsetX = e.clientX - this.startX;
    this.offsetY = e.clientY - this.startY;
    this._applyTransform();
  }

  end(e) {
    this.dragging = false;
    this.container.releasePointerCapture(e.pointerId);

    this._applyTransform();
  }

  _applyTransform() {
    this.viewport.style.transform = `translate(${this.offsetX}px, ${this.offsetY}px) scale(${this.scale})`;
    this.viewport.style.transformOrigin = "0 0";
  }

  checkAutoPan(clientX, clientY) {
    const rect = this.container.getBoundingClientRect();

    let dx = 0, dy = 0;

    if (clientX < rect.left + this.autoPanMargin) dx = this.autoPanSpeed;
    if (clientX > rect.right - this.autoPanMargin) dx = -this.autoPanSpeed;
    if (clientY < rect.top + this.autoPanMargin) dy = this.autoPanSpeed;
    if (clientY > rect.bottom - this.autoPanMargin) dy = -this.autoPanSpeed;

    this.offsetX += dx;
    this.offsetY += dy;
    this._applyTransform();
    // if (dx !== 0 || dy !== 0) {
    //   if (!this.autoPanInterval) {
    //     this.autoPanInterval = setInterval(() => {
    //     }, 100); // ~10fps -> 100ms | ~20fps -> 50ms | ~30fps -> 33ms
    //   }
    // } else {
    //   this.stopAutoPan();
    // }
  }

  stopAutoPan() {
    if (this.autoPanInterval) {
      clearInterval(this.autoPanInterval);
      this.autoPanInterval = null;
    }
  }

  reset() {
    this.scale = 1;
    this.offsetX = 0;
    this.offsetY = 0;
    this._applyTransform();
  }
}