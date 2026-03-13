import EventHandler from "../../library/eventHandler.js";
import { request } from "../../library/utils.js";

export function deleteBtn(container) {
    const btn = document.createElement("button");
    btn.textContent = "Delete";
    btn.classList.add("button", "margin-left");
    EventHandler.on(btn, "click", () => container.remove());
    return btn;
}

export async function storeData(jsondata) {
    const id = window.data?.selectedRectangle;
    if (!id) {
        console.error("storeData: no selected rectangle id");
        return;
    }
    try {
        const data = await request("/saveConditional", {
            method: "POST",
            body: { id, ...jsondata },
        });
        console.log("saveConditional response:", data);
    } catch (error) {
        console.error("Error saving conditional:", error);
    }
}
