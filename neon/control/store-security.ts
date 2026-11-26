/**
 * What the bundled object store's parts prove themselves with (cli/src/manifests/object-store.ts
 * storeSecurityToml, SeaweedFS's security.toml), made once with the stack's other secrets
 * (./secrets.ts) and never rotated:
 *
 * - on their gRPC ports, mutual TLS: a certificate authority of the store's own, which no part
 *   holds the key of, and of each part a certificate of it, the part's name (commonName) its
 *   common name, which the part presents as a server and as a client alike; each port takes the
 *   parts its NetworkPolicy admits alone, by those names. Each is valid for VALID_DAYS, past any
 *   store's life, as nothing would make one anew: no rotation, by hand or otherwise, to forget.
 * - on the filers' HTTP port, and of their IAM service, the filers' signing keys, of writes and
 *   of reads, which they take requests signed with alone; and the token main's lake
 *   reconciliation reads the lake's entries with (filerReadToken), which reads nothing else.
 *
 * Made with OpenSSL, as the webhook intake's certificate is (./certificate.ts): keys of P-256,
 * which Go's TLS takes, in PKCS #8.
 */
import { execFileSync } from "node:child_process";
import { createHmac, randomBytes } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { Certificate } from "./certificate.ts";

/** The store's parts that hold a certificate: each of its servers, and the lifecycle pass, which dials the gateways. */
export const STORE_PARTS = ["master", "volume", "filer", "gateway", "lifecycle"] as const;
export type StorePart = (typeof STORE_PARTS)[number];

/** The common name of `part`'s certificate, by which a port admits it. */
export const commonName = (part: StorePart): string => `seaweedfs-${part}`;

/** The common name of the store's certificate authority. */
export const AUTHORITY_NAME = "alasio-seaweedfs";

/** How long a certificate made here is valid: a hundred years. */
export const VALID_DAYS = 36_525;

/** What the store's parts prove themselves with, as the stack's secrets keep it. */
export interface StoreSecurity {
  /** The certificate authority, whose key the root Secret alone holds. */
  authority: Certificate;
  parts: Record<StorePart, Certificate>;
  /** The filers' signing keys (jwt.filer_signing.key and jwt.filer_signing.read.key). */
  filerSigningKey: string;
  filerReadSigningKey: string;
}

/** The store's security as an earlier release may have kept it: any of it may be absent. */
export type StoredStoreSecurity = Partial<Omit<StoreSecurity, "parts">> & { parts?: Partial<StoreSecurity["parts"]> };

/** Runs `openssl` with `args` in a directory of its own, which `files` are written in first: what it wrote there, by name, of `outputs`. */
function openssl(files: Record<string, string>, outputs: readonly string[], ...commands: (readonly string[])[]): Record<string, string> {
  const directory = mkdtempSync(join(tmpdir(), "alasio-store-tls-"));
  try {
    for (const [name, text] of Object.entries(files)) writeFileSync(join(directory, name), text, { mode: 0o600 });
    for (const args of commands) execFileSync("openssl", args, { cwd: directory, stdio: "pipe" });
    return Object.fromEntries(outputs.map((name) => [name, readFileSync(join(directory, name), "utf8")]));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

const NEW_KEY = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"] as const;

/** A new certificate authority: of certificates alone, none below it. */
export function makeAuthority(): Certificate {
  const config = [
    "[req]",
    "distinguished_name = name",
    "x509_extensions = authority",
    "prompt = no",
    "[name]",
    `CN = ${AUTHORITY_NAME}`,
    "[authority]",
    "basicConstraints = critical, CA:TRUE, pathlen:0",
    "keyUsage = critical, keyCertSign, cRLSign",
    "subjectKeyIdentifier = hash",
    "",
  ].join("\n");
  const made = openssl({ "authority.cnf": config }, ["authority.crt", "authority.key"], [
    "req", "-x509", "-config", "authority.cnf", ...NEW_KEY, "-sha256", "-days", String(VALID_DAYS), "-keyout", "authority.key", "-out", "authority.crt",
  ]);
  return { cert: made["authority.crt"] ?? "", key: made["authority.key"] ?? "" };
}

/** A new certificate of `part`, `authority`'s: its name its common name, for a TLS server and client alike. */
export function makePartCertificate(authority: Certificate, part: StorePart): Certificate {
  const extensions = [
    "[part]",
    "basicConstraints = critical, CA:FALSE",
    "keyUsage = critical, digitalSignature",
    "extendedKeyUsage = serverAuth, clientAuth",
    "subjectKeyIdentifier = hash",
    "authorityKeyIdentifier = keyid",
    "",
  ].join("\n");
  const made = openssl(
    { "authority.crt": authority.cert, "authority.key": authority.key, "part.cnf": extensions },
    ["part.crt", "part.key"],
    ["req", "-new", ...NEW_KEY, "-subj", `/CN=${commonName(part)}`, "-keyout", "part.key", "-out", "part.csr"],
    [
      "x509", "-req", "-in", "part.csr", "-CA", "authority.crt", "-CAkey", "authority.key", "-sha256",
      "-set_serial", `0x${randomBytes(16).toString("hex")}`, "-days", String(VALID_DAYS), "-extfile", "part.cnf", "-extensions", "part", "-out", "part.crt",
    ],
  );
  return { cert: made["part.crt"] ?? "", key: made["part.key"] ?? "" };
}

/**
 * The store's security, `stored` completed by `kept` (./secrets.ts completeSecrets's, which
 * keeps what is there and makes what is not): an authority made anew makes every part's
 * certificate anew too, as one of another authority's would be refused.
 */
export function completeStoreSecurity(stored: StoredStoreSecurity | undefined, kept: <T>(value: T | undefined, make: () => T) => T, secret: () => string): StoreSecurity {
  const authority = kept(stored?.authority, makeAuthority);
  const parts = stored?.authority === undefined ? {} : (stored.parts ?? {});
  return {
    authority,
    parts: Object.fromEntries(STORE_PARTS.map((part) => [part, kept(parts[part], () => makePartCertificate(authority, part))])) as Record<StorePart, Certificate>,
    filerSigningKey: kept(stored?.filerSigningKey, secret),
    filerReadSigningKey: kept(stored?.filerReadSigningKey, secret),
  };
}

/**
 * A token a filer takes for reads over HTTP (SeaweedFS's SeaweedFilerClaims, signed with its
 * read key `readSigningKey`, HS256), of `bucket`'s entries alone, read alone, and with no
 * expiry: what main's lake reconciliation asks the filers whether a file of the lake is there
 * with (neon/lake/src/reconcile.ts gone), which can read no other bucket, nor write.
 */
export function filerReadToken(readSigningKey: string, bucket: string): string {
  const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const unsigned = `${encode({ alg: "HS256", typ: "JWT" })}.${encode({ allowed_prefixes: [`/buckets/${bucket}`], allowed_methods: ["GET", "HEAD"] })}`;
  return `${unsigned}.${createHmac("sha256", readSigningKey).update(unsigned).digest("base64url")}`;
}
