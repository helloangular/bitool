export default class FollowUpLine {
  constructor(svg) {
    this.svg = svg;
    this.svgBound = null;
    this.previewLine = null;
  }

  _toSvgPoint(ev) {
    const ctm = this.svg.getScreenCTM?.();
    if (ctm && typeof DOMPoint === "function") {
      const point = new DOMPoint(ev.clientX, ev.clientY).matrixTransform(ctm.inverse());
      return { x: point.x, y: point.y };
    }
    return {
      x: ev.clientX - this.svgBound.left,
      y: ev.clientY - this.svgBound.top,
    };
  }

  start(fromEl, ev) {
    if (fromEl) {
      this.svgBound = this.svg.getBoundingClientRect();
      const {x, y} = this._toSvgPoint(ev);

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
    const {x, y} = this._toSvgPoint(ev);
    this.previewLine.setAttribute("x2", x);
    this.previewLine.setAttribute("y2", y);
  }

  end(endRect = null, ev = null) {
    if (!this.previewLine) return null;
    const points = {
      x1: Number(this.previewLine.getAttribute("x1")),
      y1: Number(this.previewLine.getAttribute("y1")),
      x2: Number(this.previewLine.getAttribute("x2")),
      y2: Number(this.previewLine.getAttribute("y2")),
    };
    this.svg.removeChild(this.previewLine);
    this.previewLine = null;
    return points;
  }

  calculatePoint(el, ev) {
    const rect = el.getBoundingClientRect();

    // distances to each side
    const distLeft = ev.clientX - rect.left;
    const distRight = rect.right - ev.clientX;
    const distTop = ev.clientY - rect.top;
    const distBottom = rect.bottom - ev.clientY;

    const minDist = Math.min(distLeft, distRight, distTop, distBottom);

    // helper to clamp a value between min and max
    const clamp = (v, a, b) => Math.max(a, Math.min(v, b));

    let x, y;
    if (minDist === distLeft) {
      // attach to left edge, keep y at the click's vertical position but inside rect
      x = rect.left - this.svgBound.left;
      y = clamp(ev.clientY, rect.top, rect.bottom) - this.svgBound.top;
    } else if (minDist === distRight) {
      // attach to right edge
      x = rect.right - this.svgBound.left;
      y = clamp(ev.clientY, rect.top, rect.bottom) - this.svgBound.top;
    } else if (minDist === distTop) {
      // attach to top edge, keep x at the click's horizontal position but inside rect
      y = rect.top - this.svgBound.top;
      x = clamp(ev.clientX, rect.left, rect.right) - this.svgBound.left;
    } else {
      // attach to bottom edge
      y = rect.bottom - this.svgBound.top;
      x = clamp(ev.clientX, rect.left, rect.right) - this.svgBound.left;
    }

    return { x, y };
  }
}
