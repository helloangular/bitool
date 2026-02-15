export default class Modal {
  constructor({ title = "Modal Title", content = "Modal Content" } = {}) {
    // Create overlay
    this.overlay = document.createElement("div");
    this.overlay.style.position = "fixed";
    this.overlay.style.top = "0";
    this.overlay.style.left = "0";
    this.overlay.style.width = "100%";
    this.overlay.style.height = "100%";
    this.overlay.style.background = "rgba(0, 0, 0, 0.5)";
    this.overlay.style.display = "flex";
    this.overlay.style.justifyContent = "center";
    this.overlay.style.alignItems = "center";
    this.overlay.style.zIndex = "1000";
    this.overlay.style.visibility = "hidden";
    this.overlay.style.opacity = "0";
    this.overlay.style.transition = "opacity 0.3s ease";

    // Modal box
    this.modal = document.createElement("div");
    this.modal.style.background = "#fff";
    this.modal.style.padding = "20px";
    this.modal.style.borderRadius = "8px";
    this.modal.style.maxWidth = "400px";
    this.modal.style.width = "100%";
    this.modal.style.boxShadow = "0 4px 10px rgba(0,0,0,0.3)";
    this.modal.style.fontFamily = "sans-serif";

    // Title
    const header = document.createElement("h2");
    header.textContent = title;
    header.style.margin = "0 0 10px 0";

    // Content
    const body = document.createElement("div");
    body.innerHTML = content;

    // Close button
    const closeBtn = document.createElement("button");
    closeBtn.textContent = "Close";
    closeBtn.style.marginTop = "15px";
    closeBtn.style.padding = "6px 12px";
    closeBtn.style.background = "#f44336";
    closeBtn.style.color = "#fff";
    closeBtn.style.border = "none";
    closeBtn.style.borderRadius = "4px";
    closeBtn.style.cursor = "pointer";
    closeBtn.onclick = () => this.close();

    this.modal.appendChild(header);
    this.modal.appendChild(body);
    this.modal.appendChild(closeBtn);
    this.overlay.appendChild(this.modal);
    document.body.appendChild(this.overlay);

    // Close on overlay click
    this.overlay.addEventListener("click", (e) => {
      if (e.target === this.overlay) this.close();
    });
  }

  open() {
    this.overlay.style.visibility = "visible";
    this.overlay.style.opacity = "1";
  }

  close() {
    this.overlay.style.opacity = "0";
    setTimeout(() => {
      this.overlay.style.visibility = "hidden";
    }, 300);
  }
}

/**
 * Example Usage.
 * // Create modal instance
  const myModal = new Modal({
    title: "Welcome",
    content: "<p>Hello Ankush 👋, this is a reusable modal box!</p>"
  });

  // Open modal when button is clicked
  const openBtn = document.createElement("button");
  openBtn.textContent = "Open Modal";
  openBtn.style.padding = "8px 16px";
  openBtn.style.margin = "20px";
  openBtn.style.cursor = "pointer";
  openBtn.onclick = () => myModal.open();
  document.body.appendChild(openBtn);
 */