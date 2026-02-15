import EventHandler from "./library/eventHandler.js";

export const updateDropdownTooltips = (columnListComponent) => {
  const customWebComponents = columnListComponent.shadow.querySelectorAll(
    "custom-web-component"
  );

  const dropdownTooltips = Array.from(customWebComponents).map((component) => {
    return component.shadowRoot.querySelector("#dropDowntooltip");
  });

  dropdownTooltips.forEach((tooltip) => {
    const content = tooltip.querySelector(".smart-tooltip-content");
    content.style.overflow = "visible";

    const dropdownContent = content.querySelector(".custom-dropdown-content");

    EventHandler.on(tooltip, "open", () => {
      dropdownTooltips.forEach((t) => {
        if (t !== tooltip) {
          t.close();
          if (dropdownContent) dropdownContent.style.display = "none";
        }
      });
    });
  });
};
