import postgres from 'postgres';
import type { ZodgresMeta } from './zod-ext.js';

export type SQL = ReturnType<typeof postgres>;

export interface ColumnDefinition {
  type: string;
  nullable: boolean;
  default?: any;
  options?: any;
  meta: ZodgresMeta;
}

export async function createEnumType(
    sql: SQL,
    def: ColumnDefinition
) {
    // check if enum exists
    const enumExists = await sql.unsafe(`SELECT 1 FROM pg_type WHERE typname = '${def.type}'`);

    // check enum shape
    if (enumExists.length > 0) {
        const enumShape = await sql.unsafe(`
            SELECT enumlabel
            FROM pg_enum e
            JOIN pg_type t ON e.enumtypid = t.oid
            WHERE t.typname = '${def.type}'
            ORDER BY e.enumsortorder;
        `);

        // compare and add new values to the enum
        const existingLabels = enumShape.map((row: any) => row.enumlabel);
        const missingValues = def.options.filter((option: string) => !existingLabels.includes(option));

        for (const option of missingValues) {
            await sql.unsafe(`ALTER TYPE ${def.type} ADD VALUE '${option}'`);
        }

    } else {
        // create enum
        await sql.unsafe(`CREATE TYPE ${def.type} AS ENUM (${def.options.map((option: string) => `'${option}'`).join(', ')})`);
    }
}