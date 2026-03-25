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

export function addTableMetadata(item, connId, schema) {
  if (item.items && item.items.length > 0) {
    const isTable = item.items.some((subItem) => subItem.items === null);
    if (isTable) {
      item.conn_id = connId;
      item.schema = schema;
    }
    item.items = item.items.map((subItem) =>
      addTableMetadata(subItem, connId, schema)
    );
  }
  return item;
}
