import assert from "assert";
import { connect, Database, z } from "../src/index.js";

describe("database", () => {
  let db: Database;

  before(async () => db = connect("postgres://postgres:postgres@localhost:5432/postgres", { debug: false }))
  after(async () => { await db.close(); })

  it("should allow to connect to 'pglite'", async () => {
    await db.open();
    const result = await db.raw`SELECT 1`;
    assert.strictEqual(1, result.length);
  });

  it("should connect to localhost", async () => {
    await db.open();
    const result = await db.raw`SELECT 1`;
    assert.strictEqual(1, result.length);
  });

  it("should handle Collection instances in sql method", async function() {
    const coll = db.collection("test_sql_method", {
      id: z.number().serial(),
      name: z.string()
    });
    await db.open();

    await db.sql`DROP TABLE IF EXISTS ${coll}`;
    await coll.migrate();

    await coll.create({ name: "test" });

    const result1 = await db.sql`SELECT COUNT(*) FROM ${coll}`;
    assert.strictEqual(typeof result1[0].count, 'string');
    assert.strictEqual(result1[0].count, '1');

    const result2 = await db.sql("SELECT COUNT(*) FROM $1", coll);
    assert.strictEqual(typeof result2[0].count, 'string');
  });

})
