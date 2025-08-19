import type { Collection } from "../../../src/collection.js";
import type { SQL } from "../../../src/utils.js";

export async function before(products: Collection, sql: SQL) {
    console.log('Running before migration for products');
    await products.update`category = ${"other"} WHERE category NOT IN ('electronics', 'other')`;
}

export async function after(products: Collection, sql: SQL) {
    console.log('Running after migration for products');
}