import assert from "assert";
import { connect, Database } from "../src/index.js";

describe("connect", () => {
  let db: Database;

  before(async () => { db = await connect(":memory:"); })
  after(async () => {
    await db.close();
  })

  it("should allow to connect to 'pglite'", async () => {
    const result = await db.sql`SELECT 1`;
    assert.strictEqual(1, result.length);
  });

  it("should connect to localhost", async () => {
    const db = await connect("postgres://postgres:postgres@localhost:5432/postgres");
    const result = await db.sql`SELECT 1`;
    assert.strictEqual(1, result.length);
  });

  // describe("definition ", () => {
  //   it("syntax", () => {
  //     const User = collection("postgres://local");
  //   });
  // });
})
