import assert from "assert";
import { connect, Database, z } from "../src/index.js";

describe("collection", () => {
  let db: Database;

  // before(async () => { db = await connect(":memory:"); })
  // after(async () => { await db.close(); })

  before(async () => {
    db = await connect("postgres://postgres:postgres@localhost:5432/postgres");
    // drop collection tables
    await db.sql`DROP TABLE IF EXISTS users`;
  })

  after(async () => await db.close());

  describe("insert", () => {
    it("auto-incrementing id", async () => {
      const users = await db.collection("users", {
        id: z.number().optional(),
        name: z.string().max(100),
        age: z.number().min(0).max(100).optional(),
      });

      const user1 = await users.create({ name: "Endel Dreyer" });
      assert.deepStrictEqual(user1, { id: 1, name: "Endel Dreyer" });

      const user2 = await users.create({ name: "Steve Jobs", age: 56 });
      assert.deepStrictEqual(user2, { id: 2, name: "Steve Jobs", age: 56 });
    });

    it("multiple items", async () => {
      const users = await db.collection("users", {
        id: z.number().optional(),
        name: z.string().max(100),
        age: z.number().min(0).max(100).optional(),
      });

      // allow to create multiple records at once
      const all = await users.create([
        { name: "Endel Dreyer" },
        { name: "Steve Jobs", age: 56 },
      ]);

      assert.deepStrictEqual(all, [
        { id: 3, name: "Endel Dreyer" },
        { id: 4, name: "Steve Jobs", age: 56 },
      ]);
    });
  });

  describe("select", async () => {
    const getCollection = async () => {
      const items = await db.collection("items", {
        id: z.number().optional(),
        name: z.string().max(100),
      });

      await items.drop();
      await items.migrate();

      return items;
    };

    it("all", async () => {
      const items = await getCollection();

      // allow to create multiple records at once
      await items.create([
        { name: "One" },
        { name: "Two" },
        { name: "Three" },
        { name: "Four" },
      ]);

      const all = await items.select`*`;
      assert.deepStrictEqual(all, [
        { id: 1, name: "One" },
        { id: 2, name: "Two" },
        { id: 3, name: "Three" },
        { id: 4, name: "Four" },
      ]);
    });

    it("select with no arguments", async () => {
      const items = await getCollection();

      // allow to create multiple records at once
      await items.create([
        { name: "One" },
        { name: "Two" },
        { name: "Three" },
        { name: "Four" },
      ]);

      const all = await items.select();

      assert.deepStrictEqual(all, [
        { id: 1, name: "One" },
        { id: 2, name: "Two" },
        { id: 3, name: "Three" },
        { id: 4, name: "Four" },
      ]);
    });

    it("where", async () => {
      const items = await getCollection();

      await items.create([
        { name: "One" },
        { name: "Two" },
        { name: "Three" },
        { name: "Four" },
      ]);

      const three = await items.select`* WHERE name = ${"Three"}`;
      assert.deepStrictEqual(three, [{ id: 3, name: "Three" }]);

    });
  });

})
