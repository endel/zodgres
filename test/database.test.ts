import assert from "assert";
import { connect, Database, z } from "../src/index.js";

describe("database", () => {
  let db: Database;

  before(async () => { db = await connect(":memory:").open(); })
  after(async () => { await db.close(); })

  it("should allow to connect to 'pglite'", async () => {
    const result = await db.raw`SELECT 1`;
    assert.strictEqual(1, result.length);
  });

  it("should connect to localhost", async () => {
    const db = await connect("postgres://postgres:postgres@localhost:5432/postgres");
    const result = await db.raw`SELECT 1`;
    assert.strictEqual(1, result.length);
  });

  it("should handle Collection instances in sql method", async function() {
    const testCollection = await db.collection("test_sql_method", {
      id: z.number().optional(),
      name: z.string()
    });

    await testCollection.create({ name: "test" });

    const result1 = await db.sql`SELECT COUNT(*) FROM ${testCollection}`;
    assert.strictEqual(typeof result1[0].count, 'string');
    assert.strictEqual(result1[0].count, '1');

    const result2 = await db.sql("SELECT COUNT(*) FROM $1", testCollection);
    assert.strictEqual(typeof result2[0].count, 'string');

    await testCollection.drop();
  });

})
