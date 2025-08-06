import assert from "assert";
import { connect, Database, z } from "../src/index.js";

describe("collection", () => {
  let db: Database;

  // before(async () => { db = await connect(":memory:"); })
  // after(async () => { await db.close(); })

  before(async () => { db = await connect("postgres://postgres:postgres@localhost:5432/postgres"); })
  after(async () => { await db.close(); })

  describe("definition ", () => {
    it("syntax", async () => {
      const User = await db.collection("users", {
        name: z.string().max(100)
      });

      console.log(User.schema);

      // User.create({ name: "John Doe" })

    });
  });

})
