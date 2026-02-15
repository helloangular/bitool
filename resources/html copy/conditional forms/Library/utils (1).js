import EventHandler from "../../eventHandler.js";

export function deleteBtn(container) {
    const btn = document.createElement("button");
    btn.textContent = "Delete";
    btn.classList.add("button", "margin-left");
    EventHandler.on(btn, "click", () => container.remove());
    return btn;
}

export async function storeData(jsondata) {
    try {
        const response = await fetch("/addtable", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(jsondata),
        });
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log("Response addtable : ", data);

    } catch (error) {
        console.error("Error:", error);
    }
}