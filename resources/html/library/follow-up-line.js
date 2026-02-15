export default class FollowUpLine {
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