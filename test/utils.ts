import type { Collection } from "../src/collection.js";

export async function init(c: Collection) {
    await c.drop();
    await c.migrate();
}