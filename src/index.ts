import postgres from 'postgres';

// Import extended Zod with unique() method
import { zod } from './zod-ext.js';
export { zod as z };

import { Database, type DatabaseOptions } from './database.js';
export { Database, type DatabaseOptions };

import { Collection } from './collection.js';
export { Collection };

import type { SQL } from './utils.js';
export type { SQL };

// import { Migrator } from './migrator.js';
// export { Migrator };

export function connect<T extends Record<string, postgres.PostgresType> = {}>(
  uri: string,
  options: DatabaseOptions<T> = {}
) {
  return new Database<T>(uri, options);
}

export { typemap } from './typemap.js';

// Utility type to extract the inferred type from a Collection
export type Row<T> = T extends Collection<infer U> ? Collection<U>['Row'] : never;