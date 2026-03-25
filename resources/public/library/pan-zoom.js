export default class PanZoom {
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

  fitToContent(padding = 40) {
    const nodes = this.viewport.querySelectorAll("rectangle-component");
    if (!nodes.length) return;

    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    nodes.forEach(n => {
      const x = parseFloat(n.style.left) || 0;
      const y = parseFloat(n.style.top) || 0;
      const w = n.offsetWidth || 110;
      const h = n.offsetHeight || 50;
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x + w > maxX) maxX = x + w;
      if (y + h > maxY) maxY = y + h;
    });

    const contentW = maxX - minX;
    const contentH = maxY - minY;
    const containerRect = this.container.getBoundingClientRect();
    const availW = containerRect.width - padding * 2;
    const availH = containerRect.height - padding * 2;

    const scaleX = availW / contentW;
    const scaleY = availH / contentH;
    this.scale = Math.min(scaleX, scaleY, 1.2);
    this.scale = Math.max(this.minScale, Math.min(this.maxScale, this.scale));

    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;
    this.offsetX = (containerRect.width / 2) - (centerX * this.scale);
    this.offsetY = (containerRect.height / 2) - (centerY * this.scale);

    this._applyTransform();
  }
}