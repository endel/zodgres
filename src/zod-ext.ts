/**
 * Zod extensions
 *
 * This file extends the Zod library with additional methods.
 *
 * - `unique()` method to add unique constraints to Zod types
 * - `isUnique()` function to check if a Zod type has a unique constraint
 *
 */
import type { ZodType as OriginalZodType } from "zod";
import * as z from 'zod';

export interface ZodgresMeta {
  primaryKey?: boolean;
  serial?: boolean;
  unique?: boolean;
  [key: string]: any;
}

declare module "zod" {
  interface ZodType {
    meta(meta: ZodgresMeta): this;
    // shorthands for meta
    unique(): this;
    serial(): this;
    primaryKey(): this;
  }
}

z.ZodType.prototype.primaryKey = function(this: OriginalZodType) {
  return this.meta({ primaryKey: true });
};

z.ZodType.prototype.serial = function(this: OriginalZodType) {
  return this.meta({ serial: true });
};

z.ZodType.prototype.unique = function(this: OriginalZodType) {
  return this.meta({ unique: true });
};

// Re-export zod with extensions
export const zod = z;