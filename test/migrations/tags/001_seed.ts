import type { Collection } from "../../../src/collection.js";
import type { SQL } from "../../../src/utils.js";

export async function before(tags: Collection, sql: SQL) {
}

export async function after(tags: Collection, sql: SQL) {
    await tags.create([
        { name: 'electronics' },
        { name: 'other' }
    ]);
}