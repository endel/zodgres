import { connect, z } from "../src";
import assert from "assert";

describe("PGLite", () => {

    it("should connect", async () => {
        const db = await connect(":memory:", { onnotice: () => { } });
        await db.open();
        const result = await db.raw`SELECT 1`;
        assert.strictEqual(1, result.length);
        await db.close();
    });

    it("should create a collection", async () => {
        const db = connect(":memory:", { onnotice: () => { } });
        const my_collection = db.collection("my_collection", {
            id: z.number().optional(),
            name: z.string(),
        });
        await db.open();

        const rows = await my_collection.create([
            { name: "John" },
            { name: "Jane" },
        ]);

        assert.strictEqual(rows.length, 2);
        assert.strictEqual(rows[0].id, 1);
        assert.strictEqual(rows[1].id, 2);

        await db.close();
    });

});