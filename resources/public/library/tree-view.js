export function renderTree(treeEl, treeItems, options = {}) {
  if (!treeEl || !Array.isArray(treeItems)) return;
  treeEl.dataSource = treeItems;

  const waitForTreeItems = () => {
    const items = treeEl.querySelectorAll("smart-tree-item");
    const groups = treeEl.querySelectorAll("smart-tree-items-group");

    if (items.length === 0 || groups.length === 0) {
      requestAnimationFrame(waitForTreeItems);
      return;
    }

    if (typeof options.populateAttributes === "function") {
      options.populateAttributes([...items, ...groups], treeItems);
    }
    if (typeof options.onContextMenuReady === "function") {
      options.onContextMenuReady(items);
    }
    if (typeof options.onDragReady === "function") {
      options.onDragReady(groups);
    }
  };

  waitForTreeItems();
}

export function populateAttributes(iterable, treeItems, findItemData) {
  if (!Array.isArray(treeItems)) return;
  iterable.forEach((element) => {
    const itemData = findItemData(element.label, treeItems);
    if (itemData) {
      Object.keys(itemData).forEach((key) => {
        if (key !== "label" && key !== "items") {
          element.setAttribute(`data-${key}`, itemData[key]);
        }
      });
    }
  });
}
