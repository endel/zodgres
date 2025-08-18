import postgres from 'postgres';
import * as zod from 'zod';

import { usePglite } from './pglite.js';
import { Collection, processSQLValues } from './collection.js';

export class Database {
  public raw!: ReturnType<typeof postgres>;

  /**
   * in-memory for local development
   */
  protected pglite?: ReturnType<typeof usePglite>;

  constructor(sql?: ReturnType<typeof postgres>) {
    if (sql) {
      this.raw = sql;
    }
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

  public async connect<T extends Record<string, postgres.PostgresType> = {}> (uri: string, options?: postgres.Options<T> | undefined) {
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

      this.raw = postgres('postgres://localhost:5432');

    } else {
      this.raw = postgres(uri, options);
    }
  }

  public async collection<T extends zod.core.$ZodLooseShape>(
    name: string,
    shape: T,
    params?: string | zod.core.$ZodObjectParams
  ) {
    const collection = new Collection<T>(name, zod.object(shape, params).strict(), this.raw);
    await collection.migrate();
    return collection;
  }

  public async close(options?: { timeout?: number | undefined } | undefined) {
    if (this.pglite) {
      await this.pglite.close();
    }
    await this.raw.end(options);
  }
}
