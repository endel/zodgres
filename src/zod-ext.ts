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

declare module "zod" {
  interface ZodType {
    unique(): this;
  }
}

// TODO: Wrap on a custom ZodUnique type instead - like z.ZodOptional.
z.ZodType.prototype.unique = function(this: OriginalZodType) {
  return this.meta({ unique: true });
};

// Re-export zod with extensions
export const zod = z;