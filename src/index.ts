import postgres from 'postgres';

// Import extended Zod with unique() method
import { zod } from './zod-ext.js';
export { zod as z };

import { Database } from './database.js';
export { Database };

import { Collection } from './collection.js';
export { Collection };

import type { SQL } from './utils.js';
export type { SQL };

// import { Migrator } from './migrator.js';
// export { Migrator };

export function connect<T extends Record<string, postgres.PostgresType> = {}>(
  uri: string,
  options?: (postgres.Options<T> & { migrations?: string }) | undefined
) {
  return new Database<T>(uri, options);
}

export { typemap } from './typemap.js';

// Utility type to extract the inferred type from a Collection
export type Row<T> = T extends Collection<infer U> ? Collection<U>['Type'] : never;