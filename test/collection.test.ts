import assert from "assert";
import { connect, Database, typemap, z, type Row } from "../src/index.js";
import { init } from "./utils.js";

describe("collection", () => {
  let db: Database;

  // before(async () => { db = await connect(":memory:"); })
  // after(async () => { await db.close(); })

  before(async () => {
    db = await connect("postgres://postgres:postgres@localhost:5432/postgres"); // , { debug: true }
    // drop collection tables
    await db.sql`DROP TABLE IF EXISTS users`;
    await db.sql`DROP TABLE IF EXISTS items`;
    await db.sql`DROP TABLE IF EXISTS uuid_auto_increment`;
    await db.sql`DROP TABLE IF EXISTS uuid_types`;
    await db.sql`DROP TABLE IF EXISTS auto_incrementing_test`;
    await db.sql`DROP TABLE IF EXISTS mixed_objects_test`;
    await db.sql`DROP TABLE IF EXISTS unique_test`;
    await db.sql`DROP TABLE IF EXISTS unique_optional_test`;
    await db.sql`DROP TABLE IF EXISTS number_to_string`;
    await db.sql`DROP TABLE IF EXISTS create_enum_type`;
    await db.sql`DROP TABLE IF EXISTS update_enum_type`;
    await db.sql`DROP TABLE IF EXISTS arbitrary_columns`;
  })

  after(async () => await db.close());

  const getItemsCollection = async () => {
    const items = await db.collection("items", {
      id: z.number().optional(),
      name: z.string().max(100),
    });
    await init(items);
    return items;
  };

  const getUsersCollection = async () => {
    const users = await db.collection("users", {
      id: z.number().optional(),
      name: z.string().max(100),
      age: z.number().min(0).max(100).optional(),
    });
    await init(users);
    return users;
  };

  describe("methods", () => {

    describe("insert", () => {
      it("auto-incrementing id", async () => {
        const users = await getUsersCollection();

        const user1 = await users.create({ name: "Endel Dreyer" });
        assert.deepStrictEqual(user1, { id: 1, name: "Endel Dreyer", age: null });

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
          { id: 1, name: "Endel Dreyer", age: null },
          { id: 2, name: "Steve Jobs", age: 56 },
        ]);
      });
    });

    describe("raw insert", () => {
      it("should insert with raw SQL and return results", async () => {
        const items = await getItemsCollection();

        const inserted = await items.insert`(name) VALUES (${"Raw Insert Test"})`;
        assert.deepStrictEqual(inserted, [{ id: 1, name: "Raw Insert Test" }]);

        const all = await items.select`* ORDER BY id`;
        assert.deepStrictEqual(all, [
          { id: 1, name: "Raw Insert Test" },
        ]);
      });

      it("should insert multiple records with raw SQL", async () => {
        const users = await getUsersCollection();

        const inserted = await users.insert`(name, age) VALUES (${"Alice"}, ${25}), (${"Bob"}, ${30})`;
        inserted.sort((a, b) => (a.id as number) - (b.id as number));

        assert.deepStrictEqual(inserted, [
          { id: 1, name: "Alice", age: 25 },
          { id: 2, name: "Bob", age: 30 }
        ]);
      });

      it("should work with conditional inserts", async () => {
        const users = await getUsersCollection();

        const name = "Dynamic User";
        const age = 25;
        const inserted = await users.insert`(name, age) VALUES (${name}, ${age > 18 ? age : 18})`;

        assert.deepStrictEqual(inserted, [{ id: 1, name: "Dynamic User", age: 25 }]);
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

        await init(users);

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

    describe("delete", () => {
      it("should return the number of deleted records", async () => {
        const items = await getItemsCollection();
        await items.create([{ name: "One" }, { name: "Two" }]);

        assert.strictEqual(await items.count(), 2);

        const deleted = await items.delete();
        assert.deepStrictEqual(deleted, 2);

        assert.strictEqual(await items.count(), 0);
      });

      it("should delete single record", async () => {
        const items = await getItemsCollection();
        await items.create([{ name: "One" }, { name: "Two" }]);

        assert.strictEqual(await items.count(), 2);

        const deleted = await items.delete`WHERE name = ${"One"}`;
        assert.deepStrictEqual(deleted, 1);

        assert.strictEqual(await items.count(), 1);
      });

      it("should allow delete returning data", async () => {
        const items = await getItemsCollection();
        await items.create([{ name: "One" }, { name: "Two" }]);

        const deleted = await items.delete<{ id: number, name: string }>`WHERE name = ${"One"} RETURNING *`;
        assert.deepStrictEqual(deleted, [{ id: 1, name: "One" }]);
      });
    });
  });

  describe("data types", () => {

    describe("auto-incrementing id", () => {
      it("should create a table with an auto-incrementing id", async () => {
        const c = await db.collection("auto_increment_id", {
          id: z.number().optional(),
          name: z.string().optional(),
        });
        z.string({})
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'id', data_type: typemap.id, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
          { column_name: "name", data_type: typemap.string, character_maximum_length: null, column_default: null, is_nullable: "YES" },
        ]);

        await c.create([{}, {}]);
        assert.deepStrictEqual(await c.select(), [
          { id: 1, name: undefined },
          { id: 2, name: undefined },
        ]);
      });

      xit("should create a table with an auto-incrementing id", async () => {
        // this test fails: batch inserting empty objects with table having only an auto-incrementing id
        const c = await db.collection("auto_increment_id", {
          id: z.number().optional(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'id', data_type: typemap.id, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        await c.create([{}, {}]);
        assert.deepStrictEqual(await c.select(), [
          { id: 1, },
          { id: 2, },
        ]);
      });

      it("should handle mixed arrays with empty and populated objects", async () => {
        const c = await db.collection("mixed_defaults", {
          id: z.number().optional(),
          name: z.string().optional(),
          age: z.number().optional(),
        });
        await init(c);

        // Test mixed array: empty object, object with data, empty object, object with data
        await c.create([{}, { name: "Alice" }, { age: 25 }, { name: "Bob" }]);
        const results = await c.select();

        assert.deepStrictEqual(results, [
          { id: 1, name: undefined, age: undefined },  // empty object uses defaults
          { id: 2, name: "Alice", age: undefined },  // object with data
          { id: 3, name: undefined, age: 25 },  // empty object uses defaults
          { id: 4, name: "Bob", age: undefined },   // object with data
        ]);
      });
    });

    describe("numeric types", () => {
      it("number = numeric", async () => {
        const c = await db.collection("numeric_types", {
          value: z.number(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.number, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        const insertRows = [{ value: Math.PI }, { value: 3.14 }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });

      it("float32 = real", async () => {
        const c = await db.collection("numeric_types", {
          value: z.float32(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.float32, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);
      });

      it("float64 = double precision", async () => {
        const c = await db.collection("numeric_types", {
          value: z.float64(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.float64, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);
      });
    });

    describe("string types", () => {
      it("string = text", async () => {
        const c = await db.collection("string_types", {
          value: z.string(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.string, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);
      });

      it("string = varchar(100) with default", async () => {
        const c = await db.collection("string_types", {
          value: z.string().max(100).default("default"),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.string_max, character_maximum_length: 100, column_default: "'default'::character varying", is_nullable: 'NO' },
        ]);
      });

      it("string = varchar(100) with default and optional", async () => {
        const c = await db.collection("string_types", {
          value: z.string().max(100).default("default").optional(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.string_max, character_maximum_length: 100, column_default: "'default'::character varying", is_nullable: 'YES' },
        ]);
      });

    });

    describe("date types", () => {
      it("date = timestamp without time zone", async () => {
        const c = await db.collection("date_types", {
          value: z.date(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.date, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);
      });

      it("date = timestamp without time zone with default", async () => {
        const c = await db.collection("date_types", {
          value: z.date().default(new Date()),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.date, character_maximum_length: null, column_default: "now()", is_nullable: 'NO' },
        ]);
      });

    });

    describe("uuid types", () => {
      it("primary key as uuid (auto-incrementing)", async () => {
        const c = await db.collection("uuid_auto_increment", {
          id: z.uuid().optional(),
          name: z.string(),
        });
        await init(c);

        await c.create([{ name: "John" }]);

        const rows = await c.select();
        assert.strictEqual(rows[0]?.id.length, 36);
        assert.strictEqual(rows[0]?.name, "John");
      });

      it("guid = uuid", async () => {
        const c = await db.collection("uuid_types", {
          value: z.guid().optional(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.uuid, character_maximum_length: null, column_default: "gen_random_uuid()", is_nullable: 'YES' },
        ]);

        const insertRows = [{ value: "123e4567-e89b-12d3-a456-426614174000" }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);

        const newRow = await c.create({});
        assert.ok(c.parse(newRow).value);
      });

      it("uuid = uuid", async () => {
        const c = await db.collection("uuid_types", {
          value: z.uuid(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.uuid, character_maximum_length: null, column_default: "gen_random_uuid()", is_nullable: 'NO' },
        ]);

        const insertRows = [{ value: "123e4567-e89b-12d3-a456-426614174000" }, { value: "123e4567-e89b-12d3-a456-426614174001" }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });

    });

    describe("boolean types", () => {
      it("boolean = boolean", async () => {
        const c = await db.collection("boolean_types", {
          value: z.boolean(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.boolean, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        const insertRows = [{ value: true }, { value: false }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });

      it("boolean with default", async () => {
        const c = await db.collection("boolean_default_types", {
          value: z.boolean().default(true),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.boolean, character_maximum_length: null, column_default: 'true', is_nullable: 'NO' },
        ]);

        await c.create([{}]);
        assert.deepStrictEqual(await c.select(), [{ value: true }]);
      });

      it("boolean optional", async () => {
        const c = await db.collection("boolean_optional_types", {
          value: z.boolean().optional(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.boolean, character_maximum_length: null, column_default: null, is_nullable: 'YES' },
        ]);

        const insertRows = [{ value: true }, { value: false }, {}];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), [
          { value: true },
          { value: false },
          { value: undefined }
        ]);
      });

      it("boolean with default and optional", async () => {
        const c = await db.collection("boolean_default_optional_types", {
          value: z.boolean().default(false).optional(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.boolean, character_maximum_length: null, column_default: 'false', is_nullable: 'YES' },
        ]);

        await c.create([{}, { value: true }]);
        assert.deepStrictEqual(await c.select(), [
          { value: false },
          { value: true }
        ]);
      });
    });

    describe("enum types", () => {
      it("create enum type", async () => {
        const c = await db.collection("create_enum_type", {
          value: z.enum(['electronics', 'books', 'clothing']),
        });
        type EnumCollection = Row<typeof c>;

        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.enum, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        const insertRows: EnumCollection[] = [{ value: "electronics" }, { value: "books" }, { value: "clothing" }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });

      it("update enum type", async () => {
        const c = await db.collection("update_enum_type", {
          value: z.enum(['electronics', 'books', 'clothing']),
        });
        await init(c);

        // should reject invalid option
        assert.rejects(async () => {
          // @ts-ignore
          await c.create({ value: "furniture" });
        }, /Invalid option/);

        const c2 = await db.collection("update_enum_type", {
          value: z.enum(['electronics', 'books', 'clothing', 'furniture']),
        });
        type EnumCollection = Row<typeof c2>;
        await init(c2);

        const insertRows: EnumCollection[] = [{ value: "electronics" }, { value: "books" }, { value: "clothing" }, { value: "furniture" }];
        await c2.create(insertRows);
        assert.deepStrictEqual(await c2.select(), insertRows);
      });

    });

    describe("JSONB types", () => {
      it("z.array()", async () => {
        const c = await db.collection("jsonb_types", {
          value: z.array(z.string()),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.array, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        const insertRows = [{ value: ["one", "two", "three"] }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });

      it("z.object()", async () => {
        const c = await db.collection("jsonb_types", {
          value: z.object({
            name: z.string(),
            age: z.number(),
          }),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.object, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        const insertRows = [{ value: { name: "John", age: 30 } }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });

      it("z.record()", async () => {
        const c = await db.collection("jsonb_types", {
          value: z.record(z.string(), z.number()),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.record, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        const insertRows = [{ value: { "John": 30 } }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });

      it("z.any()", async () => {
        const c = await db.collection("jsonb_types", {
          value: z.any(),
        });
        await init(c);
        assert.deepStrictEqual([...await c.columns()], [
          { column_name: 'value', data_type: typemap.any, character_maximum_length: null, column_default: null, is_nullable: 'NO' },
        ]);

        const insertRows = [{ value: "test" }, { value: 123 }, { value: { name: "John", age: 30 } }, { value: { "John": 30 } }];
        await c.create(insertRows);
        assert.deepStrictEqual(await c.select(), insertRows);
      });
    });

  });

  describe("unique constraints", () => {
    it("should create column with unique constraint", async () => {
      const c = await db.collection("unique_test", {
        id: z.number().optional(),
        email: z.string().unique(),
        username: z.string().max(50).unique(),
      });

      await init(c);

      // Insert first record - should succeed
      await c.create({ email: "test@example.com", username: "testuser" });

      // Try to insert duplicate email - should fail
      await assert.rejects(async () => {
        await c.create({ email: "test@example.com", username: "differentuser" });
      }, /duplicate key value violates unique constraint/);

      // Try to insert duplicate username - should fail
      await assert.rejects(async () => {
        await c.create({ email: "different@example.com", username: "testuser" });
      }, /duplicate key value violates unique constraint/);

      // Insert different values - should succeed
      const record = await c.create({ email: "different@example.com", username: "differentuser" });
      assert.ok(record.id > 0, "Record should have a valid ID");
      assert.strictEqual(record.email, "different@example.com");
      assert.strictEqual(record.username, "differentuser");
    });

    it("should work with optional unique fields", async () => {
      const c = await db.collection("unique_optional_test", {
        id: z.number().optional(),
        code: z.string().optional().unique(),
      });

      await init(c);

      // Insert record with code - should succeed
      await c.create({ code: "ABC123" });

      // Insert record without code - should succeed (NULL values don't conflict)
      await c.create({});

      // Try to insert duplicate non-null code - should fail
      await assert.rejects(async () => {
        await c.create({ code: "ABC123" });
      }, /duplicate key value violates unique constraint/);
    });
  });

  describe("alter table", () => {
    it("should convert integer fields to varchar", async () => {
      const users1 = await db.collection("number_to_string", {
        id: z.number().optional(),
        age: z.number().min(0).max(100),
      });
      await init(users1);

      await users1.create([{ age: 25 }, { age: 30 },]);

      const users1Rows = await users1.select();
      assert.deepStrictEqual(users1Rows, [
        { id: 1, age: 25 },
        { id: 2, age: 30 },
      ]);

      const users2 = await db.collection("number_to_string", {
        id: z.number().optional(),
        age: z.string(),
      });

      await users2.update`age = age || ${" updated!"}`;
      const users2Rows = await users2.select();
      assert.deepStrictEqual(users2Rows, [
        { id: 1, age: "25 updated!" },
        { id: 2, age: "30 updated!" },
      ]);
    });

    it("should convert string field to enum", async () => {
      const c = await db.collection("text_to_enum", {
        value: z.string().max(100)
      });
      await init(c);

      await c.create([{ value: "one" }, { value: "two" }, { value: "three" }]);

      const c2 = await db.collection("text_to_enum", {
        value: z.enum(['one', 'two', 'three']),
      });

      assert.deepStrictEqual(await c2.select(), [
        { value: "one" },
        { value: "two" },
        { value: "three" },
      ]);

    });

    it("nullable to not null", async () => {
      const c = await db.collection("nullable_to_not_null", {
        value: z.number().optional(),
      });
      await init(c);

      await c.create([{}, {}, { value: 3 }]);
      assert.deepStrictEqual(await c.select(), [
        { value: undefined },
        { value: undefined },
        { value: 3 },
      ]);

      const c2 = await db.collection("nullable_to_not_null", {
        value: z.number().default(0),
      });

      assert.deepStrictEqual(await c2.select(), [
        { value: 3 },
        { value: 0 },
        { value: 0 },
      ]);

    });
  });

  describe("querying", () => {
    it("should allow to select arbitrary columns", async () => {
      const c = await db.collection("arbitrary_columns", {
        id: z.number().optional(),
        name: z.string(),
        age: z.number(),
      });
      await init(c);

      await c.create([
        { name: "John", age: 25 },
        { name: "Jane", age: 30 },
      ]);

      const rows = await c.select`name`;
      assert.deepStrictEqual(rows, [ { name: "John" }, { name: "Jane" } ]);
    });

    it("arbitrary count", async () => {
      const c = await db.collection("arbitrary_columns", {
        id: z.number().optional(),
        name: z.string(),
        age: z.number(),
      });
      await init(c);

      await c.create([
        { name: "John", age: 25 },
        { name: "Jane", age: 30 },
      ]);

      const rows = await c.select<{ count: number }>`COUNT(*)`;
      assert.deepStrictEqual(rows, [ { count: '2' } ]);
    });

  });

});
