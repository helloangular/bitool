export function getTreeItems() {
  return window.data?.treeItems || [];
}

export function setTreeItems(items) {
  window.data = window.data || {};
  window.data.treeItems = items;
  localStorage.setItem("jqdata", JSON.stringify(window.data));
}

export function updateTreeItems(updater) {
  if (typeof updater !== "function") return;
  const current = getTreeItems();
  const next = updater(current);
  if (next && Array.isArray(next)) {
    setTreeItems(next);
  }
}

export function findItemData(label, items = getTreeItems()) {
  for (const item of items) {
    if (item.label === label) {
      return item;
    }
    if (item.items) {
      const found = findItemData(label, item.items);
      if (found) return found;
    }
  }
  return null;
}

export function findItemDataByPath(path, items = getTreeItems()) {
  if (path === undefined || path === null || path === "") return null;
  const parts = String(path).split(".").map((part) => Number(part));
  let currentItems = items;
  let currentItem = null;

  for (const index of parts) {
    if (!Array.isArray(currentItems) || Number.isNaN(index) || !currentItems[index]) {
      return null;
    }
    currentItem = currentItems[index];
    currentItems = currentItem.items;
  }

  return currentItem;
}

export function mapItems(item, treeItems, data) {
  return treeItems.map((treeItem) => {
    if (treeItem.label === item.label) {
      if (!treeItem.items) treeItem.items = [];
      return {
        ...treeItem,
        items: [...treeItem.items, data],
      };
    } else if (treeItem.items) {
      return {
        ...treeItem,
        items: mapItems(item, treeItem.items, data),
      };
    }
    return treeItem;
  });
}

export function updateItemByPath(path, treeItems, updater) {
  const parts = String(path).split(".").map((part) => Number(part));

  function visit(items, depth = 0) {
    if (!Array.isArray(items)) return items;

    return items.map((item, index) => {
      if (index !== parts[depth]) return item;
      if (depth === parts.length - 1) {
        return updater(item);
      }
      return {
        ...item,
        items: visit(item.items, depth + 1),
      };
    });
  }

  return visit(treeItems);
}

export function addTableMetadata(item, connId, schema) {
  if (item.items && item.items.length > 0) {
    const isTable = item.items.some((subItem) => subItem.items === null);
    if (isTable) {
      if (!item.conn_id) item.conn_id = connId;
      if (!item.schema && schema) item.schema = schema;
      if (!item.nodetype) item.nodetype = "table";
    }
    item.items = item.items.map((subItem) =>
      addTableMetadata(subItem, connId, schema)
    );
  }
  return item;
}
