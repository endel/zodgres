/**
 * Zod extensions
 *
 * This file extends the Zod library with additional methods.
 *
 * - `unique()` method to add unique constraints to Zod types
 * - `isUnique()` function to check if a Zod type has a unique constraint
 *
 */
import * as z from 'zod';

const uniqueConstraints = new WeakMap<z.ZodType, boolean>();

declare module "zod" {
  interface ZodType {
    unique(): this;
  }
}

// TODO: Wrap on a custom ZodUnique type instead - like z.ZodOptional.
z.ZodType.prototype.unique = function() {
  uniqueConstraints.set(this, true);
  return this;
};

export function isUnique(zodType: z.ZodType): boolean {
  return uniqueConstraints.has(zodType) && uniqueConstraints.get(zodType) === true;
}

// Re-export zod with extensions
export const zod = z;