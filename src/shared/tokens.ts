/**
 * Bearer tokens, as what serves on a token compares the one it is presented with the
 * one it holds: apart from the rest of alasio, so a program of alasio's image that
 * serves on a token takes it without loading alasio.
 */
import { timingSafeEqual } from "node:crypto";

import { Schema } from "effect";

/** The bearer token a request presents, from its `Authorization` header. */
export const BearerToken = Schema.TemplateLiteralParser(["Bearer ", Schema.NonEmptyString]);

/** Whether `presented` is `expected`, compared in constant time. */
export function sameToken(presented: string, expected: string): boolean {
  const a = Buffer.from(presented);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}
