/**
 * A harness pool's pod's grant: what its harness host's role reaches its own conversation's
 * part of alasio's state by, and no other's. Every pod of an environment's pools shares the one
 * role (../neon/harness.ts), so the role alone tells no pod from another; its grant does.
 *
 * alasio makes a binding's grant as it binds the conversation's harness ahead
 * (./pooled-host.ts), of the conversation and the binding the front answers with, signed by
 * its own key of the gateway's caller key (grantKey), so it is the same grant however often
 * alasio makes it, an alasio started again among them, and another binding's is another. It
 * keeps the grant's digest alone, as the conversation's (harness_grants, ../persistence/schema.ts),
 * which a binding of the conversation's replaces, and its release or loss deletes; and gives the
 * grant to the pod on each call it makes of a binding it bound (GRANT_HEADER). The pod sets it
 * on each of its connections (GRANT_SETTING, ../neon/granted-pool.ts), and Postgres's row-level
 * security lets the role reach only the rows of the conversation whose grant hashes to it
 * (../persistence/harness-grants.ts): no grant, or one no conversation's digest is, reaches
 * nothing.
 */
import { createHash, createHmac } from "node:crypto";

/** The setting of a connection of the harness host's role that holds its pod's grant, which the state's policies read. */
export const GRANT_SETTING = "alasio.harness_grant";

/** The header of a call of alasio's to a pod of a binding it bound that gives the pod its grant. */
export const GRANT_HEADER = "x-alasio-harness-grant";

/** A grant as alasio makes it: a SHA-256 HMAC, base64url, unpadded. */
const GRANT = /^[A-Za-z0-9_-]{43}$/u;

/** The key alasio signs its pools' grants with: its environment's own key of the gateway's caller key's, made apart, so a grant is never a token of the key's. */
export const grantKey = (environmentKey: Buffer): Buffer => createHmac("sha256", environmentKey).update("harness-grant").digest();

/** The grant of the binding `binding` of the conversation `conversationId`'s harness, signed by `key` (grantKey). */
export const harnessGrant = (key: Buffer, conversationId: string, binding: string): string =>
  createHmac("sha256", key).update(`${conversationId}\n${binding}`).digest("base64url");

/** The digest of `grant` alasio keeps, as Postgres makes it of the setting (sha256 of its UTF-8). */
export const grantDigest = (grant: string): Buffer => createHash("sha256").update(grant, "utf8").digest();

/** Whether `text` is a grant as alasio makes one, which alone a pod sets. */
export const isGrant = (text: string): boolean => GRANT.test(text);
