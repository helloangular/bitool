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
        <path d="M0,0 L10,5 L0,10 z" fill="#3b7ddd"></path>
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

  _debugEnabled() {
    return Boolean(window.BITOOL_DEBUG_CONNECTORS);
  }

  _debug(...args) {
    if (this._debugEnabled()) {
      console.warn("[SvgConnector]", ...args);
    }
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
    path.setAttribute("stroke", opts.stroke || "#3b7ddd");
    path.setAttribute("stroke-width", String(opts.strokeWidth || 2));
    if (opts.dasharray) path.setAttribute("stroke-dasharray", opts.dasharray);
    if (opts.arrow) path.setAttribute("marker-end", "url(#arrow)");
    // path.style.pointerEvents = "none";

    this.pathGroup.appendChild(path);
    this.connections.set(id, { from: fromId, to: toId, pathEl: path, options: opts });

    this._debug("connect", { id, fromId, toId, opts });

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

  _getNodeBox(el) {
    // Connector coordinates live in viewport-local space, so use authored
    // left/top while sizing against the visible rectangle body inside the host.
    const x = parseFloat(el.style.left) || 0;
    const y = parseFloat(el.style.top) || 0;
    const inner = el.shadowRoot?.querySelector(".rectangle-container") || el.querySelector(".rectangle-container");
    let w = inner?.offsetWidth || el.offsetWidth;
    let h = inner?.offsetHeight || el.offsetHeight;
    if (!w || !h) {
      w = 110;
      h = 45;
    }
    return { x, y, w, h };
  }

  _computePathForPair(aEl, bEl, opts = {}) {
    const boxA = this._getNodeBox(aEl);
    const boxB = this._getNodeBox(bEl);
    const ax = boxA.x, ay = boxA.y, aw = boxA.w, ah = boxA.h;
    const bx = boxB.x, by = boxB.y, bw = boxB.w, bh = boxB.h;

    if (opts.fromAnchor && opts.toAnchor) {
      const a = {
        x: ax + (aw * opts.fromAnchor.rx),
        y: ay + (ah * opts.fromAnchor.ry),
      };
      const b = {
        x: bx + (bw * opts.toAnchor.rx),
        y: by + (bh * opts.toAnchor.ry),
      };
      const dist = Math.hypot(a.x - b.x, a.y - b.y);
      const offset = Math.min(Math.max(dist * 0.25, 30), 200);
      let c1 = { x: a.x, y: a.y };
      let c2 = { x: b.x, y: b.y };

      if (Math.abs(a.x - b.x) > Math.abs(a.y - b.y)) {
        c1.x = a.x + (a.x < b.x ? offset : -offset);
        c2.x = b.x + (b.x < a.x ? offset : -offset);
      } else {
        c1.y = a.y + (a.y < b.y ? offset : -offset);
        c2.y = b.y + (b.y < a.y ? offset : -offset);
      }

      const curveTweak = opts.curve || 0.5;
      const cx1 = a.x + (c1.x - a.x) * curveTweak;
      const cy1 = a.y + (c1.y - a.y) * curveTweak;
      const cx2 = b.x + (c2.x - b.x) * curveTweak;
      const cy2 = b.y + (c2.y - b.y) * curveTweak;

      this._debug("path", {
        fromId: aEl?.getAttribute?.("id"),
        toId: bEl?.getAttribute?.("id"),
        fromConnId: aEl?.getAttribute?.("conn_id"),
        toConnId: bEl?.getAttribute?.("conn_id"),
        boxA,
        boxB,
        fromAnchor: opts.fromAnchor,
        toAnchor: opts.toAnchor,
        start: a,
        end: b,
        control1: { x: cx1, y: cy1 },
        control2: { x: cx2, y: cy2 },
        viewportTransform: this.container?.style?.transform || null,
      });

      return `M ${a.x} ${a.y} C ${cx1} ${cy1} ${cx2} ${cy2} ${b.x} ${b.y}`;
    }

    const leftToRight = ax <= bx;
    const a = leftToRight
      ? { x: ax + aw, y: ay + ah / 2 }
      : { x: ax, y: ay + ah / 2 };
    const b = leftToRight
      ? { x: bx, y: by + bh / 2 }
      : { x: bx + bw, y: by + bh / 2 };
    const dist = Math.hypot(a.x - b.x, a.y - b.y);
    const offset = Math.min(Math.max(dist * 0.25, 30), 200);

    let c1 = { x: a.x, y: a.y };
    let c2 = { x: b.x, y: b.y };

    c1.x = a.x + (leftToRight ? offset : -offset);
    c2.x = b.x + (leftToRight ? -offset : offset);
    c1.y = a.y;
    c2.y = b.y;

    const curveTweak = opts.curve || 0.5;
    const cx1 = a.x + (c1.x - a.x) * curveTweak;
    const cy1 = a.y + (c1.y - a.y) * curveTweak;
    const cx2 = b.x + (c2.x - b.x) * curveTweak;
    const cy2 = b.y + (c2.y - b.y) * curveTweak;

    this._debug("path", {
      fromId: aEl?.getAttribute?.("id"),
      toId: bEl?.getAttribute?.("id"),
      fromConnId: aEl?.getAttribute?.("conn_id"),
      toConnId: bEl?.getAttribute?.("conn_id"),
      boxA,
      boxB,
      start: a,
      end: b,
      control1: { x: cx1, y: cy1 },
      control2: { x: cx2, y: cy2 },
      viewportTransform: this.container?.style?.transform || null,
    });

    return `M ${a.x} ${a.y} C ${cx1} ${cy1} ${cx2} ${cy2} ${b.x} ${b.y}`;
  }
}
