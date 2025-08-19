import { connect, z } from "../src/index.js";
import path from "path";
import assert from "assert";
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe('migration scripts', () => {
    before(async() => {
        const db = await connect("postgres://postgres:postgres@localhost:5432/postgres").open();
        await db.raw`DROP TABLE IF EXISTS products`;
        await db.raw`DROP TABLE IF EXISTS migrations`;
        await db.raw`DROP TYPE IF EXISTS products_category_enum`;
        await db.close();
    });

    it('should run a migration script before migrating a collection', async () => {
        // Step 1: Create initial data with string category field
        const db1 = connect("postgres://postgres:postgres@localhost:5432/postgres")
        const products1 = db1.collection('products', {
            id: z.number().optional(),
            category: z.string().max(100),
        });
        await db1.open();

        await products1.create([
            { category: 'electronics' },
            { category: 'clothing' },
            { category: 'books' },
            { category: 'other' },
        ]);


        // Verify initial data
        const initialData = await db1.raw`SELECT * FROM products ORDER BY id`;
        assert.strictEqual(initialData.length, 4);
        assert.strictEqual(initialData[0]?.category, 'electronics');
        assert.strictEqual(initialData[1]?.category, 'clothing');
        assert.strictEqual(initialData[2]?.category, 'books');
        assert.strictEqual(initialData[3]?.category, 'other');

        await db1.close();

        // Step 2: Connect with migrations path - this should run the migration
        const db2 = connect("postgres://postgres:postgres@localhost:5432/postgres", {
            migrations: path.resolve(__dirname, 'migrations')
        });

        // Step 3: Create collection with enum constraint - this should work after migration
        const products2 = db2.collection('products', {
            id: z.number().optional(),
            category: z.enum(['electronics', 'other']),
        });

        await db2.open();

        // Step 4: Verify migration worked correctly
        const migratedData = await db2.sql`SELECT * FROM ${products2} ORDER BY id`;
        assert.strictEqual(migratedData.length, 4);
        assert.strictEqual(migratedData[0]?.category, 'electronics'); // Should remain 'electronics'
        assert.strictEqual(migratedData[1]?.category, 'other');       // 'clothing' should become 'other'
        assert.strictEqual(migratedData[2]?.category, 'other');       // 'books' should become 'other'
        assert.strictEqual(migratedData[3]?.category, 'other');       // Should remain 'other'

        // Step 5: Verify that migration was recorded
        const migrations = await db2.raw`SELECT * FROM migrations`;
        assert.strictEqual(migrations.length, 2);
        assert.strictEqual(migrations[0]?.migration, '001_global_test.ts');
        assert.strictEqual(migrations[0]?.collection, null);
        assert.strictEqual(migrations[1]?.migration, '001_update_category.ts');
        assert.strictEqual(migrations[1]?.collection, 'products');

        // Step 6: Verify enum constraint is working
        try {
            await products2.create({ category: 'invalid_category' as any });
            assert.fail('Should have thrown an error for invalid enum value');
        } catch (error) {
            // Expected - invalid enum value should be rejected
            assert(error instanceof Error);
        }

        await db2.close();
    });

    it('should not run migrations twice', async () => {
        // Connect again with migrations - should not re-run migrations
        const db = await connect("postgres://postgres:postgres@localhost:5432/postgres", {
            migrations: path.resolve(__dirname, 'migrations')
        }).open();

        const db2 = await connect("postgres://postgres:postgres@localhost:5432/postgres", {
            migrations: path.resolve(__dirname, 'migrations')
        }).open();

        // Verify migration count hasn't changed
        const migrations = await db.raw`SELECT * FROM migrations`;
        assert.strictEqual(migrations.length, 2);

        await db.close();
        await db2.close();
    });
});