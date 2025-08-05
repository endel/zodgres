import { PGLiteSocketServer } from '@electric-sql/pglite-socket'
import { PGlite, type DebugLevel } from '@electric-sql/pglite'

export function usePglite() {
  const db = new PGlite({ debug: 0 });

  // Check if the database is working
  db.query('SELECT version()')
    .then(() => console.log('Database is working'))
    .catch((err) => console.error('Database error:', err));

  const server = new PGLiteSocketServer({
    db,
    port: 5431,
    host: '127.0.0.1',
    // path: UNIX,
    inspect: false, // Print the incoming and outgoing data to the console
  })

  return {
    server,
    db,
    close: async () => {
      console.log("server will stop...");
      await server.stop();
      console.log("server will close...");
      await db.close();
      console.log("done");
    }
  };
}
