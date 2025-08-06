import postgres from 'postgres';
import * as zod from 'zod';

import { usePglite } from './pglite.js';
import { Collection } from './collection.js';

export { zod as z };

export class Database {
  public sql!: ReturnType<typeof postgres>;

  /**
   * in-memory for local development
   */
  protected pglite?: ReturnType<typeof usePglite>;

  constructor(sql?: ReturnType<typeof postgres>) {
    if (sql) {
      this.sql = sql;
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

      // // Handle SIGINT to stop the server and close the database
      // process.on('SIGINT', async () => {
      //   if (this.pglite) {
      //     await this.pglite.close();
      //   }
      //   console.log('pglite server stopped and database closed');
      //   process.exit(0);
      // });

      this.sql = postgres('postgres://localhost:5432');

    } else {
      this.sql = postgres(uri, options);
    }
  }

  public async collection<T extends zod.core.$ZodLooseShape>(
    name: string,
    shape: T,
    params?: string | zod.core.$ZodObjectParams
  ) {
    const collection = new Collection<T>(name, zod.object(shape, params).strict(), this.sql);
    await collection.migrate();
    return collection
  }

  public async close(options?: { timeout?: number | undefined } | undefined) {
    if (this.pglite) {
      await this.pglite.close();
    }
    await this.sql.end(options);
  }
}

export async function connect<T extends Record<string, postgres.PostgresType> = {}>(
  uri: string,
  options?: postgres.Options<T> | undefined
) {
  const db = new Database();
  await db.connect(uri, options); // if :memory:, connect to pglite
  return db;
}
