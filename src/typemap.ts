import * as zod from 'zod';
import { isUnique } from './zod-ext.js';

export const typemap = {
    number: "numeric",
    string: "text",
    string_max: "character varying",
    date: "timestamp without time zone",
    uuid: "uuid",
    float32: "real",
    float64: "double precision",
    boolean: "boolean",

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
    switch (columnName) {
        case "id":
            return typemap.id;

        default:
            const { type } = zodUnwrapType(zodProperty);

            if (type instanceof zod.ZodString && type.maxLength !== null) {
                return typemap.string_max;

            } else {
                return typemap[type.def.type as keyof typeof typemap] || typemap.string;
            }
    }
}