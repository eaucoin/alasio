/**
 * The tokens Neon's services authenticate each other with: EdDSA JWTs with a
 * scope, and a tenant for tenant-scoped ones, with no expiry, as Neon issues
 * them (libs/utils/src/auth.rs).
 */
import { createPrivateKey, createPublicKey, generateKeyPairSync, type JsonWebKey, sign, verify } from "node:crypto";

/** An Ed25519 keypair, as PEM. */
export interface KeyPair {
  privateKeyPem: string;
  publicKeyPem: string;
}

/** What a token grants: its scope, and the tenant a tenant-scoped one is limited to. */
export interface TokenClaims {
  scope: string;
  tenant_id?: string;
}

/** A JWK set, as compute_ctl reads one. */
export interface JsonWebKeySet {
  keys: JsonWebKey[];
}

const HEADER = base64url(JSON.stringify({ alg: "EdDSA", typ: "JWT" }));

function base64url(value: string): string {
  return Buffer.from(value).toString("base64url");
}

/** A new Ed25519 keypair, as PEM. */
export function generateKeyPair(): KeyPair {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519", {
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
    publicKeyEncoding: { type: "spki", format: "pem" },
  });
  return { privateKeyPem: privateKey, publicKeyPem: publicKey };
}

/**
 * A token for `scope` (tenant, pageserverapi, safekeeperdata,
 * generations_api, admin, infra, ...), limited to `tenantId` if given.
 */
export function signToken(privateKeyPem: string, scope: string, tenantId: string | null = null): string {
  const claims: TokenClaims = tenantId ? { scope, tenant_id: tenantId } : { scope };
  const unsigned = `${HEADER}.${base64url(JSON.stringify(claims))}`;
  const signature = sign(null, Buffer.from(unsigned), createPrivateKey(privateKeyPem));
  return `${unsigned}.${signature.toString("base64url")}`;
}

/** The claims of `token` if `publicKeyPem` signed it, else null. */
export function verifyToken(publicKeyPem: string, token: string): TokenClaims | null {
  const [header, claims, signature, ...rest] = String(token).split(".");
  if (claims === undefined || signature === undefined || rest.length !== 0 || header !== HEADER) return null;
  const valid = verify(
    null,
    Buffer.from(`${header}.${claims}`),
    createPublicKey(publicKeyPem),
    Buffer.from(signature, "base64url"),
  );
  if (!valid) return null;
  try {
    // Signed with the stack's key, so made by signToken.
    const parsed: TokenClaims = JSON.parse(Buffer.from(claims, "base64url").toString("utf8"));
    return parsed;
  } catch {
    return null;
  }
}

/** Whether `authorization`, an HTTP Authorization header, bears a token `publicKeyPem` signed for one of `scopes`. */
export function bearsScope(publicKeyPem: string, authorization: string | undefined, ...scopes: readonly string[]): boolean {
  if (!authorization?.startsWith("Bearer ")) return false;
  const scope = verifyToken(publicKeyPem, authorization.slice("Bearer ".length))?.scope;
  return scope !== undefined && scopes.includes(scope);
}

/** The public key as a JWK set, which compute_ctl verifies its API's tokens with. */
export function publicJwks(publicKeyPem: string): JsonWebKeySet {
  const jwk = createPublicKey(publicKeyPem).export({ format: "jwk" });
  return { keys: [{ ...jwk, use: "sig", key_ops: ["verify"], alg: "EdDSA", kid: "alasio-neon" }] };
}
