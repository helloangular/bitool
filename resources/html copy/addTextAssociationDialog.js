const textAssociationDialog = document.querySelector(".association-dialog");

const headers = ["Business Name", "Technical Name"];
const fields = ["business_name", "technical_name"];

const searchInput = document.createElement("smart-input");
searchInput.style.width = "100%";
searchInput.style.marginBottom = "10px";
searchInput.placeholder = "Search";

const smartTable = document.createElement("smart-table");
const table = document.createElement("table");
const thead = document.createElement("thead");
const tbody = document.createElement("tbody");
const tr = document.createElement("tr");

headers.forEach((header) => {
  const th = document.createElement("th");
  th.textContent = header;
  tr.appendChild(th);
});

thead.appendChild(tr);
table.appendChild(thead);

window.data.rectangles
  .filter((rect) => rect.parent_object === "Table_view")
  .forEach((tableRect) => {
    const tr = document.createElement("tr");

    fields.forEach((field) => {
      const td = document.createElement("td");
      td.textContent = tableRect[field];
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

table.appendChild(tbody);

smartTable.className = "table-hover table-bordered table-striped";
smartTable.appendChild(table);

textAssociationDialog.appendChild(searchInput);
textAssociationDialog.appendChild(smartTable);

smartTable.addEventListener("cellClick", (e) => {
  const tableBusinessName = e.detail.row["Business Name"];

  const selectedAssociation = window.data.rectangles.find(
    (rect) => rect.business_name === tableBusinessName
  );

  window.data.selectedAssociation = selectedAssociation.alias || "";

  const associationEditorComponent = document.querySelector(
    "association-editor-component"
  );

  if (associationEditorComponent) {
    associationEditorComponent.setAttribute("visibility", "open");
  }

  const columnListComponent = document.querySelector("column-list-component");

  if (columnListComponent) {
    columnListComponent.setAttribute("visibility", "");
  }

  textAssociationDialog.close();
});

searchInput.addEventListener("changing", (e) => {
  const searchQuery = e.detail.value;
  const rows = smartTable.querySelectorAll("tr");

  rows.forEach((row) => {
    const columns = row.querySelectorAll("td");

    const matched = Array.from(columns).some((column) =>
      column.textContent.toLowerCase().includes(searchQuery.toLowerCase())
    );

    if (matched) row.style.display = "table-row";
    else row.style.display = "none";
  });
});
