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

  const getItemsCollection = async () => {
    const items = await db.collection("items", {
      id: z.number().optional(),
      name: z.string().max(100),
    });
    await items.drop();
    await items.migrate();
    return items;
  };

  const getUsersCollection = async () => {
    const users = await db.collection("users", {
      id: z.number().optional(),
      name: z.string().max(100),
      age: z.number().min(0).max(100).optional(),
    });
    await users.drop();
    await users.migrate();
    return users;
  };

  describe("insert", () => {
    it("auto-incrementing id", async () => {
      const users = await getUsersCollection();

      const user1 = await users.create({ name: "Endel Dreyer" });
      assert.deepStrictEqual(user1, { id: 1, name: "Endel Dreyer" });

      const user2 = await users.create({ name: "Steve Jobs", age: 56 });
      assert.deepStrictEqual(user2, { id: 2, name: "Steve Jobs", age: 56 });
    });

    it("multiple items", async () => {
      const users = await getUsersCollection();

      // allow to create multiple records at once
      const all = await users.create([
        { name: "Endel Dreyer" },
        { name: "Steve Jobs", age: 56 },
      ]);

      assert.deepStrictEqual(all, [
        { id: 1, name: "Endel Dreyer" },
        { id: 2, name: "Steve Jobs", age: 56 },
      ]);
    });
  });

  describe("select", async () => {
    it("should select all", async () => {
      const items = await getItemsCollection();

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

    it("should accept no arguments", async () => {
      const items = await getItemsCollection();

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

  });

  describe("select one", () => {
    it("without explicit limit", async () => {
      const items = await getItemsCollection();

      await items.create([
        { name: "One" },
        { name: "Two" },
        { name: "Three" },
        { name: "Four" },
      ]);

      const one = await items.selectOne`*`;
      assert.deepStrictEqual(one, { id: 1, name: "One" });
    });

    it("should throw if LIMIT is present", async () => {
      const items = await getItemsCollection();
      assert.rejects(() => items.selectOne`* LIMIT 1`, /LIMIT/i);
    });
  });

  describe("select where", () => {
    it("should select with conditions", async () => {
      const items = await getItemsCollection();

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

  describe("update", () => {
    it("should update with basic SET clause", async () => {
      const items = await getItemsCollection();

      await items.create([
        { name: "One" },
        { name: "Two" },
        { name: "Three" },
        { name: "Four" },
      ]);

      // Update all records with same name
      const updated = await items.update`name = ${"Updated"} WHERE id = ${2}`;
      assert.deepStrictEqual(updated, [{ id: 2, name: "Updated" }]);

      // Verify the update worked
      const all = await items.select`* ORDER BY id`;
      assert.deepStrictEqual(all, [
        { id: 1, name: "One" },
        { id: 2, name: "Updated" },
        { id: 3, name: "Three" },
        { id: 4, name: "Four" },
      ]);
    });

    it("should update multiple records with WHERE condition", async () => {
      const items = await getItemsCollection();

      await items.create([
        { name: "One" },
        { name: "Two" },
        { name: "Three" },
        { name: "Four" },
      ]);

      // Update multiple records
      const updated = await items.update`name = ${"Multi-Updated"} WHERE id IN (${1}, ${3})`;
      // Sort by id for consistent comparison
      updated.sort((a, b) => (a.id as number) - (b.id as number));
      assert.deepStrictEqual(updated, [
        { id: 1, name: "Multi-Updated" },
        { id: 3, name: "Multi-Updated" },
      ]);

      // Verify the updates worked
      const all = await items.select`* ORDER BY id`;
      assert.deepStrictEqual(all, [
        { id: 1, name: "Multi-Updated" },
        { id: 2, name: "Two" },
        { id: 3, name: "Multi-Updated" },
        { id: 4, name: "Four" },
      ]);
    });

    it("should update without WHERE clause (update all)", async () => {
      const items = await getItemsCollection();

      await items.create([
        { name: "One" },
        { name: "Two" },
        { name: "Three" },
      ]);

      // Update all records
      const updated = await items.update`name = ${"All Updated"}`;
      // Sort by id for consistent comparison
      updated.sort((a, b) => (a.id as number) - (b.id as number));
      assert.deepStrictEqual(updated, [
        { id: 1, name: "All Updated" },
        { id: 2, name: "All Updated" },
        { id: 3, name: "All Updated" },
      ]);
    });

    it("should work with complex collections", async () => {
      const users = await db.collection("update_users", {
        id: z.number().optional(),
        name: z.string().max(100),
        age: z.number().min(0).max(100).optional(),
        active: z.boolean().optional(),
      });

      await users.drop();
      await users.migrate();

      await users.create([
        { name: "Alice", age: 25, active: true },
        { name: "Bob", age: 30, active: false },
        { name: "Charlie", age: 35, active: true },
      ]);

      // Update specific fields with multiple conditions
      const updated = await users.update`age = ${31}, active = ${true} WHERE name = ${"Bob"}`;
      assert.deepStrictEqual(updated, [
        { id: 2, name: "Bob", age: 31, active: true },
      ]);

      // Verify Bob was updated correctly
      const bob = await users.select`* WHERE name = ${"Bob"}`;
      assert.deepStrictEqual(bob, [{ id: 2, name: "Bob", age: 31, active: true }]);
    });

    it("should return empty array when no records match WHERE condition", async () => {
      const items = await getItemsCollection();

      await items.create([
        { name: "One" },
        { name: "Two" },
      ]);

      // Try to update non-existent record
      const updated = await items.update`name = ${"Updated"} WHERE id = ${999}`;
      assert.deepStrictEqual(updated, []);

      // Verify original records are unchanged
      const all = await items.select`* ORDER BY id`;
      assert.deepStrictEqual(all, [
        { id: 1, name: "One" },
        { id: 2, name: "Two" },
      ]);
    });
  });

})
