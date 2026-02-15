import "./source/modules/smart.dropdownlist.js";

export const renderContentOfEditDialog = (ColumnListComponent, columnType) => {
  const oldTable = ColumnListComponent.columnEditDialog.querySelector("table");
  if (oldTable) oldTable.remove();

  const table = document.createElement("table");
  table.style.width = "100%";
  table.style.borderCollapse = "collapse";
  table.style.marginBottom = "10px";

  const style = document.createElement("style");
  style.textContent = `
    .edit-dialog-table td, .edit-dialog-table th {
      border: 1px solid #ddd;
    }
  `;
  document.head.appendChild(style);
  table.classList.add("edit-dialog-table");

  const thead = document.createElement("thead");
  const tbody = document.createElement("tbody");

  const headers =
    columnType == "measure"
      ? [
          "",
          "Business Name",
          "Technical Name",
          "Data Type",
          "Aggregation",
          "Semantic Type",
          "Unit Column",
        ]
      : [
          "",
          "Business Name",
          "Technical Name",
          "Data Type",
          "Semantic Type",
          "Label Column",
        ];

  const keys =
    columnType == "measure"
      ? [
          "key",
          "business_name",
          "technical_name",
          "data_type",
          "aggregation",
          "semantic_type",
          "unit_column",
        ]
      : [
          "key",
          "business_name",
          "technical_name",
          "data_type",
          "semantic_type",
          "label_column",
        ];

  const headerRow = document.createElement("tr");
  headers.forEach((header, index) => {
    const th = document.createElement("th");
    if (index === 0 && columnType === "attribute") {
      const headerContent = document.createElement("div");
      headerContent.style.display = "flex";
      headerContent.style.alignItems = "center";
      headerContent.style.gap = "4px";

      const keyIcon = document.createElement("span");
      keyIcon.innerHTML = "&#128273;";
      keyIcon.style.fontSize = "14px";
      headerContent.appendChild(keyIcon);

      th.appendChild(headerContent);
    } else {
      th.textContent = header;
    }
    th.style.textAlign = "left";
    th.style.padding = "8px";
    if (index === 0) {
      th.style.width = "30px";
    }
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  const measureItems = ColumnListComponent.selectedRectangle.items.filter(
    (item) => item.column_type == columnType
  );
  measureItems.forEach((item) => {
    const tr = document.createElement("tr");
    keys.forEach((key, index) => {
      const td = document.createElement("td");
      td.style.padding = "8px";
      if (index === 0) {
        td.style.width = "30px";
      }
      switch (key) {
        case "key":
          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          checkbox.classList.add("row-checkbox");
          checkbox.setAttribute("data-technical-name", item.technical_name);
          checkbox.style.margin = "0";
          if (columnType === "attribute") {
            checkbox.checked = item[key] === "yes";
          }
          td.appendChild(checkbox);
          break;
        case "business_name":
          td.innerHTML = `<smart-input value="${item[key]}" style="width:100%;" class="${key}" data-technical-name="${item.technical_name}" data-field="${key}"></smart-input>`;
          break;
        case "technical_name":
          td.innerHTML = `<smart-input value="${item[key]}" disabled style="width:100%;" class="${key}" data-technical-name="${item.technical_name}" data-field="${key}"></smart-input>`;
          break;
        case "data_type":
          td.innerHTML = `<smart-input value="${item[key]}" disabled style="width:100%;" class="${key}" data-technical-name="${item.technical_name}" data-field="${key}"></smart-input>`;
          break;
        case "aggregation":
          const input = document.createElement("smart-input");
          input.dropDownButtonPosition = "right";
          input.dataSource = ["SUM", "COUNT", "MIN", "MAX", "NONE"];
          input.value = item[key] || "SUM";
          input.style.width = "100%";
          input.className = key;
          input.setAttribute("data-technical-name", item.technical_name);
          input.setAttribute("data-field", key);

          td.appendChild(input);
          break;
        case "semantic_type":
          const semanticTypeInput = document.createElement("smart-input");

          semanticTypeInput.dataSource = [
            "Calendar - Day",
            "Fiscal - Year",
            "Business Data - From",
            "Geolocation - Latitude",
            "Language",
          ];
          semanticTypeInput.value = item[key] || "NONE";
          semanticTypeInput.dropDownButtonPosition = "right";
          semanticTypeInput.style.width = "100%";
          semanticTypeInput.className = key;
          semanticTypeInput.setAttribute(
            "data-technical-name",
            item.technical_name
          );
          semanticTypeInput.setAttribute("data-field", key);

          td.appendChild(semanticTypeInput);
          break;
        case "unit_column":
          td.innerHTML = `<smart-input value="${
            item[key] || ""
          }" disabled style="width:100%;" class="${key}" data-technical-name="${
            item.technical_name
          }" data-field="${key}"></smart-input>`;
          break;
        case "label_column":
          td.innerHTML = `<smart-input value="${
            item[key] || ""
          }" disabled style="width:100%;" class="${key}" data-technical-name="${
            item.technical_name
          }" data-field="${key}"></smart-input>`;
          break;
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);

  ColumnListComponent.columnEditDialog.appendChild(table);
  ColumnListComponent.columnEditDialog.open();

  setSmartInputListeners(ColumnListComponent, columnType);
};

const setSmartInputListeners = (ColumnListComponent, columnType) => {
  const inputs = document.querySelectorAll("smart-input");
  inputs.forEach((input) => {
    if (!input.hasAttribute("data-listener")) {
      input.addEventListener("changing", (e) => {
        const field = e.target.getAttribute("data-field");

        if (field === "aggregation" || field === "semantic_type") {
          return;
        }

        const technicalName = e.target.getAttribute("data-technical-name");
        const value = e.target.value;

        updateItem(ColumnListComponent, technicalName, field, value);
      });
      input.addEventListener("change", (e) => {
        const field = e.target.getAttribute("data-field");

        if (field !== "aggregation" && field !== "semantic_type") {
          return;
        }

        const technicalName = e.target.getAttribute("data-technical-name");
        const value = e.target.value;

        updateItem(ColumnListComponent, technicalName, field, value);
      });
      input.setAttribute("data-listener", "true");
    }
  });

  const checkboxes =
    ColumnListComponent.columnEditDialog.querySelectorAll(".row-checkbox");
  checkboxes.forEach((checkbox) => {
    if (!checkbox.hasAttribute("data-listener")) {
      checkbox.addEventListener("change", (e) => {
        const technicalName = e.target.getAttribute("data-technical-name");
        const checked = e.target.checked;

        if (columnType === "attribute") {
          const item = ColumnListComponent.selectedRectangle.items.find(
            (item) => item.technical_name === technicalName
          );
          if (item) {
            updateItem(
              ColumnListComponent,
              technicalName,
              "key",
              checked ? "yes" : "no"
            );
          }
        }
      });
      checkbox.setAttribute("data-listener", "true");
    }
  });
};

const updateItem = (ColumnListComponent, technicalName, field, value) => {
  const item = ColumnListComponent.selectedRectangle.items.find(
    (item) => item.technical_name === technicalName
  );
  if (item) {
    window.data.rectangles = window.data.rectangles.map((rect) => {
      if (rect.alias === ColumnListComponent.selectedRectangle.alias) {
        return {
          ...rect,
          items: rect.items.map((item) => {
            if (item.technical_name === technicalName) {
              return { ...item, [field]: value };
            }

            return item;
          }),
        };
      }

      return rect;
    });

    ColumnListComponent.updateSelectedRectangle();
    ColumnListComponent.renderContent();
  }
};
