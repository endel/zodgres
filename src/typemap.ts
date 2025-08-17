import * as zod from 'zod';
import type postgres from 'postgres';
import { isUnique } from './zod-ext.js';
import { Collection } from './collection.js';

export const typemap = {
    number: "numeric",
    string: "text",
    string_max: "character varying",
    date: "timestamp without time zone",
    uuid: "uuid",
    float32: "real",
    float64: "double precision",
    boolean: "boolean",

    array: "jsonb",
    object: "jsonb",
    record: "jsonb",
    map: "jsonb",
    any: "jsonb",
    // literal: "text",
    // union: "text",

    // edge cases
    enum: "USER-DEFINED",

    // special case for auto-incrementing id
    id: "integer",
};

export function zodUnwrapType(zodProperty: zod.ZodType) {
    let nullable = false;
    let defaultValue: any = undefined;
    let currentType: any = zodProperty;
    let unique = isUnique(zodProperty);

    // Handle wrapped types (Optional, Nullable, Default)
    while (currentType) {
        if (currentType instanceof zod.ZodOptional) {
            nullable = true;
            currentType = currentType.unwrap();
        } else if (currentType instanceof zod.ZodNullable) {
            nullable = true;
            currentType = currentType.unwrap();
        } else if (currentType instanceof zod.ZodDefault) {
            defaultValue = typeof currentType.def.defaultValue === 'function'
                ? currentType.def.defaultValue()
                : currentType.def.defaultValue;
            currentType = currentType.def.innerType;
        } else {
            break;
        }
    }

    return {
        type: currentType,
        nullable,
        defaultValue,
        unique,
    }
}

export function zodToMappedType(columnName: string, zodProperty: zod.ZodType) {
    const { type } = zodUnwrapType(zodProperty);

    // Check if it's a UUID type regardless of column name
    if (type instanceof zod.ZodGUID || type instanceof zod.ZodUUID) {
        return typemap.uuid;
    }

    switch (columnName) {
        case "id":
            // For id columns, check the actual type
            if (type instanceof zod.ZodString && type.maxLength !== null) {
                return typemap.string_max;
            } else if (type instanceof zod.ZodNumber) {
                // For number ID columns, use integer to support IDENTITY
                return typemap.id;
            } else {
                return typemap[type.def.type as keyof typeof typemap] || typemap.id;
            }

        default:
            if (type instanceof zod.ZodString && type.maxLength !== null) {
                return typemap.string_max;

            } else {
                return typemap[type.def.type as keyof typeof typemap] || typemap.string;
            }
    }
}

export function processSQLValues(sql: ReturnType<typeof postgres>, values: any[]): any[] {
    return values.map(value => {
        if (value instanceof Collection) {
            // Return the table name directly (no quotes)
            return sql.unsafe(value.name);
        }
        return value;
    });
}