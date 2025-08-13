import postgres from 'postgres';

import * as zod from 'zod';
export { zod as z };

import { Database } from './db.js';
export { Database };

export async function connect<T extends Record<string, postgres.PostgresType> = {}>(
  uri: string,
  options?: postgres.Options<T> | undefined
) {
  const db = new Database();
  await db.connect(uri, options); // if :memory:, connect to pglite
  return db;
}

export { typemap } from './typemap.js';