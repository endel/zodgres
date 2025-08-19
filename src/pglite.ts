import { PGLiteSocketServer } from '@electric-sql/pglite-socket'
import { PGlite } from '@electric-sql/pglite'

export function usePglite() {
  const db = new PGlite({ debug: 0 });

  // Check if the database is working
  db.query('SELECT version()')
    // @ts-ignore
    .then((v) => console.log('PGLite version:', v?.rows?.[0]?.version))
    .catch((err) => console.error('Database error:', err));

  const server = new PGLiteSocketServer({
    db,
    port: 5431,
    host: '127.0.0.1',
    // path: UNIX,
    inspect: false, // Print the incoming and outgoing data to the console
    debug: false,
  });

  return {
    start: () => server.start(),
    db,
    close: async () => {
      try {
        await server.stop();
      } catch (error) {
        console.warn('Error stopping PGLite server:', error);
      }
      try {
        await db.close();
      } catch (error) {
        console.warn('Error closing PGLite database:', error);
      }
    }
  };
}
