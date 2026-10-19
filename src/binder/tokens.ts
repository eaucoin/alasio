/**
 * The binder's tokens, which say what they are for and are checked by the key that signed
 * them alone, so whatever checks one holds no store of them, as the binder holds no state:
 * `<claims>.<signature>`, the claims base64url JSON, the signature their HMAC-SHA256 under the
 * key, base64url, compared in constant time.
 *
 * A caller's token names the key it may reach, when it stops, and what its key is bound with:
 * what the binder's callers present, which the binder checks and never forwards. A binding's
 * token names the key, the binding, its Lease's uid, so one minted for a binding is no
 * other's, even of the same key, and the namespace its Lease is in: what the bound pod is
 * given, to take its calls, and its telemetry, by (bindingToken).
 */
import { createHmac, timingSafeEqual } from "node:crypto";

import { Option, Schema } from "effect";

/**
 * What a token says: the key it is for; the binding, and its Lease's namespace, for a
 * binding's; and, for a caller's, the time it stops, in ms, and what its key is bound with
 * where it is not bound yet, which the binder's gateway reads (as a session's environment and
 * internet).
 */
export const TokenClaims = Schema.Struct({
  key: Schema.String,
  binding: Schema.optional(Schema.String),
  namespace: Schema.optional(Schema.String),
  expires: Schema.optional(Schema.Number),
  options: Schema.optional(Schema.Record(Schema.String, Schema.String)),
});
export type TokenClaims = typeof TokenClaims.Type;

const decodeClaims = Schema.decodeUnknownOption(Schema.fromJsonString(TokenClaims));

const signatureOf = (secret: Buffer, claims: string): Buffer => createHmac("sha256", secret).update(claims).digest();

/** A token of `claims`, signed with `secret`. */
export function mintToken(secret: Buffer, claims: TokenClaims): string {
  const encoded = Buffer.from(JSON.stringify(claims)).toString("base64url");
  return `${encoded}.${signatureOf(secret, encoded).toString("base64url")}`;
}

/** The token of the binding `binding` of `key`, its Lease in `namespace`: the same text wherever it is minted, as its pod compares it whole. */
export const bindingToken = (secret: Buffer, { key, binding, namespace }: { readonly key: string; readonly binding: string; readonly namespace: string }): string =>
  mintToken(secret, { key, binding, namespace });

/**
 * What `token` says, where the key `secret` names of what it says signed it, as a key the
 * claims choose (./names.ts environmentKey's), and it has not stopped by `now`; else null. Its claims
 * are trusted only once its signature is checked by the key they named.
 */
export function verifyToken(secret: Buffer | ((claims: TokenClaims) => Buffer | null), token: string, now: number): TokenClaims | null {
  const dot = token.indexOf(".");
  if (dot < 0) return null;
  const [encoded, signature] = [token.slice(0, dot), Buffer.from(token.slice(dot + 1), "base64url")];
  const claims = decodeClaims(Buffer.from(encoded, "base64url").toString("utf8"));
  if (Option.isNone(claims)) return null;
  const key = typeof secret === "function" ? secret(claims.value) : secret;
  if (key === null) return null;
  const expected = signatureOf(key, encoded);
  if (signature.length !== expected.length || !timingSafeEqual(signature, expected)) return null;
  return claims.value.expires !== undefined && claims.value.expires <= now ? null : claims.value;
}
