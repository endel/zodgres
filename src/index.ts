import postgres from 'postgres';
import { usePglite } from './pglite.js';

export class Database {
  public sql!: ReturnType<typeof postgres>;

  /**
   * in-memory for local development
   */
  protected pglite?: ReturnType<typeof usePglite>;

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

  public collection() {
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
