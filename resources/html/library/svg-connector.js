export default class SvgConnector {
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