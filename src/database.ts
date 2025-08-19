import postgres from 'postgres';
import * as zod from 'zod';

import { usePglite } from './pglite.js';
import { Collection, processSQLValues } from './collection.js';
import { Migrator } from './migrator.js';

import type { SQL } from './utils.js';

export class Database<T extends Record<string, postgres.PostgresType> = {}> {
  public raw!: SQL;
  public isOpen = false;

  /**
   * Migrator instance
   */
  protected migrator!: Migrator;

  /**
   * in-memory for local development
   */
  protected pglite?: ReturnType<typeof usePglite> | undefined;

  /**
   * Collections
   */
  protected collections: Record<string, Collection> = {};

  protected uri!: string;
  protected options!: (postgres.Options<T> & { migrations?: string }) | undefined;

  constructor(uri: string, options?: (postgres.Options<T> & { migrations?: string }) | undefined) {
    this.uri = uri;
    this.options = options;
    // @ts-ignore
    this.raw = (...args: any[]) => Promise.reject('Database not connected');
  }

  public async sql(strings: TemplateStringsArray | string, ...values: any[]): Promise<any[]> {
    // Handle both template literals and plain string queries
    if (typeof strings === 'string') {
      // For plain string queries, we need to manually substitute values
      // since unsafe() doesn't support positional parameters
      let query = strings;

      values.forEach((value, index) => {
        const placeholder = `$${index + 1}`;
        let escapedValue: string;

        if (value instanceof Collection) {
          // For Collection instances, use the table name directly (no quotes)
          escapedValue = value.name;
        } else if (typeof value === 'string') {
          escapedValue = `'${value.replace(/'/g, "''")}'`;
        } else {
          escapedValue = String(value);
        }

        query = query.replace(placeholder, escapedValue);
      });

      return await this.raw.unsafe(query);

    } else {
      return await this.raw(strings, ...processSQLValues(this.raw, values));
    }
  }

  public async connect<T extends Record<string, postgres.PostgresType> = {}> (
    uri: string,
    options?: (postgres.Options<T> & { migrations?: string }) | undefined
  ) {
    if (uri === ":memory:") {
      //
      // Use pglite-socket on development
      // https://github.com/electric-sql/pglite/blob/main/packages/pglite-socket/examples/basic-server.ts
      //
      this.pglite = usePglite();

      // start pglite socket socket server
      await this.pglite.server.start();

    //   // Handle SIGINT to stop the server and close the database
    //   process.on('SIGINT', async () => {
    //     if (this.pglite) {
    //       await this.pglite.close();
    //     }
    //     console.log('pglite server stopped and database closed');
    //     process.exit(0);
    //   });

      this.raw = postgres('postgres://localhost:5431');

    } else {
      this.raw = postgres(uri, options);
    }

    // Create migrator instance after connecting
    this.migrator = new Migrator(this.raw, options?.migrations);
    if (options?.migrations) {
      await this.migrator.runGlobalMigrations();
    }
  }

  public collection<T extends zod.core.$ZodLooseShape>(
    name: string,
    shape: T,
    params?: string | zod.core.$ZodObjectParams
  ) {
    const collection = new Collection<T>(name, zod.object(shape, params).strict(), this);

    this.collections[name] = collection;

    // If defining a collection after the database is open, migrate it immediately
    if (this.isOpen) {
      this.migrator.migrateCollection(name, collection, this);
    }

    return collection;
  }

  public async open() {
    if (!this.isOpen) {
      await this.connect(this.uri, this.options);
      this.isOpen = true;
    }

    for (const name in this.collections) {
      await this.migrator.migrateCollection(name, this.collections[name]!, this);
    }

    return this;
  }

  public async close(options?: { timeout?: number | undefined } | undefined) {
    await this.raw.end(options);
    if (this.pglite) {
      await this.pglite.close();
      this.pglite = undefined;
    }
    this.isOpen = false;
  }
}
