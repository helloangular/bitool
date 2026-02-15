import EventHandler from "./library/eventHandler.js";

export const setEventListenersToDropdownItems = (columnListComponent) => {
  const customWebComponents = columnListComponent.shadowRoot.querySelectorAll(
    "custom-web-component"
  );
  const textAssociationDialog = document.querySelector(".association-dialog");

  customWebComponents.forEach((component) => {
    const changeToAttributeItem = component.shadowRoot.querySelector(
      ".change-to-attribute"
    );

    if (changeToAttributeItem)
      EventHandler.on(changeToAttributeItem, "click", () => {
        const businessName = component.getAttribute("business_name");

        window.data.rectangles = window.data.rectangles.map((rectangle) => {
          if (rectangle.alias == columnListComponent.selectedRectangle.alias) {
            return {
              ...rectangle,
              items: rectangle.items.map((item) =>
                item.business_name == businessName &&
                item.column_type == "measure"
                  ? { ...item, column_type: "attribute" }
                  : item
              ),
            };
          }

          return rectangle;
        });

        columnListComponent.updateSelectedRectangle();
        columnListComponent.renderContent();
      });

    const changeToMeasureItem =
      component.shadowRoot.querySelector(".change-to-measure");

    if (changeToMeasureItem)
      EventHandler.on(changeToMeasureItem, "click", () => {
        const businessName = component.getAttribute("business_name");
        const key = component.getAttribute("key");

        if (key === "yes") return;

        window.data.rectangles = window.data.rectangles.map((rectangle) => {
          if (rectangle.alias == columnListComponent.selectedRectangle.alias) {
            return {
              ...rectangle,
              items: rectangle.items.map((item) =>
                item.business_name == businessName &&
                item.column_type == "attribute"
                  ? { ...item, column_type: "measure" }
                  : item
              ),
            };
          }

          return rectangle;
        });

        columnListComponent.updateSelectedRectangle();
        columnListComponent.renderContent();
      });

    const dropdowns = component.shadowRoot.querySelectorAll(".custom-dropdown");

    dropdowns.forEach((dropdown) => {
      const button = dropdown.querySelector(".dropdown-button");

      EventHandler.on(button, "click", () => {
        const width = button.offsetWidth;
        const height = button.offsetHeight;

        const dropdownContent = dropdown.querySelector(
          ".custom-dropdown-content"
        );

        if (dropdownContent.style.display == "none")
          dropdownContent.style.display = "block";
        else dropdownContent.style.display = "none";

        dropdownContent.style.right = `${width}px`;
        dropdownContent.style.top = `-${height}px`;
      });
    });

    const aggregationButtons = component.shadowRoot.querySelectorAll(
      ".aggregation-button"
    );

    if (aggregationButtons.length > 0)
      aggregationButtons.forEach((button) => {
        EventHandler.on(button, "click", () => {
          const businessName = component.getAttribute("business_name");
          const value = button.textContent;

          window.data.rectangles = window.data.rectangles.map((rectangle) => {
            if (
              rectangle.alias == columnListComponent.selectedRectangle.alias
            ) {
              return {
                ...rectangle,
                items: rectangle.items.map((item) =>
                  item.business_name == businessName &&
                  item.column_type == "measure"
                    ? { ...item, aggregation: value }
                    : item
                ),
              };
            }

            return rectangle;
          });

          columnListComponent.updateSelectedRectangle();
          columnListComponent.renderContent();
        });
      });

    const symantictypeButtons = component.shadowRoot.querySelectorAll(
      ".semantictype-button"
    );

    if (symantictypeButtons.length > 0)
      symantictypeButtons.forEach((button) => {
        EventHandler.on(button, "click", () => {
          const businessName = component.getAttribute("business_name");
          const value = button.textContent;

          window.data.rectangles = window.data.rectangles.map((rectangle) => {
            if (
              rectangle.alias == columnListComponent.selectedRectangle.alias
            ) {
              return {
                ...rectangle,
                items: rectangle.items.map((item) =>
                  item.business_name == businessName &&
                  item.column_type == "attribute"
                    ? { ...item, semantic_type: value }
                    : item
                ),
              };
            }

            return rectangle;
          });

          columnListComponent.updateSelectedRectangle();
          columnListComponent.renderContent();
        });
      });

    const setkeyButton = component.shadowRoot.querySelector(".setkey-button");

    if (setkeyButton)
      EventHandler.on(setkeyButton, "click", () => {
        const businessName = component.getAttribute("business_name");

        window.data.rectangles = window.data.rectangles.map((rectangle) => {
          if (rectangle.alias == columnListComponent.selectedRectangle.alias) {
            return {
              ...rectangle,
              items: rectangle.items.map((item) =>
                item.business_name == businessName &&
                item.column_type == "attribute"
                  ? { ...item, key: "yes" }
                  : item
              ),
            };
          }

          return rectangle;
        });

        columnListComponent.updateSelectedRectangle();
        columnListComponent.renderContent();
      });

    const removekeyButton =
      component.shadowRoot.querySelector(".removekey-button");

    if (removekeyButton)
      EventHandler.on(removekeyButton, "click", () => {
        const businessName = component.getAttribute("business_name");

        window.data.rectangles = window.data.rectangles.map((rectangle) => {
          if (rectangle.alias == columnListComponent.selectedRectangle.alias) {
            return {
              ...rectangle,
              items: rectangle.items.map((item) =>
                item.business_name == businessName &&
                item.column_type == "attribute"
                  ? { ...item, key: "no" }
                  : item
              ),
            };
          }

          return rectangle;
        });

        columnListComponent.updateSelectedRectangle();
        columnListComponent.renderContent();
      });

    const addTextAssociationButton = component.shadowRoot.querySelector(
      ".add-text-association"
    );

    if (addTextAssociationButton)
      EventHandler.on(addTextAssociationButton, "click", () => {
        textAssociationDialog.open();
      });

    const dropdownButton = component.shadowRoot.querySelector("#dropDowntooltip");
    
    if (dropdownButton)
      EventHandler.on(dropdownButton, "click", (e) => {
        const tooltip = e.target.closest(".tooltip-item");
        if (tooltip.textContent === "Exclude Column") {
          component.setAttribute("excluded", "YES");
          component.dispatchEvent(new CustomEvent("excluded", { detail: { excluded: "YES" } }));
        }
        if (tooltip.textContent === "Include Column") {
          component.setAttribute("excluded", "NO");
          component.dispatchEvent(new CustomEvent("excluded", { detail: { excluded: "NO" } }));
        }
        if (tooltip.textContent === "Add Alias") {
          const renameDialog = document.querySelector("rename-dialog");
          const item = window.data.rectangles.find(r => r.id === window.data.selectedRectangle).items.find(
            (item) => item.business_name === component.business_name
          );
          if (item) {
            renameDialog.setAttribute("business-name", item.business_name);
            renameDialog.setAttribute("visibility", "open");
          }
        }
        if (tooltip.textContent === "Transform") {
          document.querySelector("transform-editor").setAttribute("visibility", "open");
          document.querySelector("transform-editor").setAttribute("item", JSON.stringify(
            window.data.rectangles.filter(r => r.id === window.data.selectedRectangle)[0].items.find(i => i.business_name === component.business_name)
          ));
        }
      });
  });
};
