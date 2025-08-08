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
      const users = await db.collection("users", {
        name: z.string().max(100),
      });

      const user = await users.create({ name: "Endel Dreyer" });
      console.log(user);

      // User.create({ name: "John Doe" })

    });
  });

})
