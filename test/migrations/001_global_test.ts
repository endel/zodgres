import type { SQL } from '../../src/index.js';

export async function before(sql: SQL) {
    await sql`CREATE TABLE IF NOT EXISTS testing123 (id SERIAL PRIMARY KEY, name TEXT)`;
}

export async function after(sql: SQL) {
    await sql`INSERT INTO testing123 (name) VALUES ('test')`;
}
