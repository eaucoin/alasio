/**
 * The webhook intake's certificate (webhook/src/server.ts): self-signed, for the address
 * Telegram delivers to, which alasio's command line uploads to Telegram with each bot's
 * registration (cli/src/webhook.ts), so Telegram trusts that certificate alone. The stack's
 * setup (./kube-setup.ts) keeps it while it is for the address and has more than
 * RENEW_DAYS left, and makes it anew otherwise; alasio up registers each bot again with
 * the one there is, and an alert says when one nears its end unrenewed.
 *
 * It is made with OpenSSL, as Telegram's own guide to self-signed certificates makes one:
 * an RSA key of 2048 bits, SHA-256, the address its common name, and its subject's
 * alternative name, an IP address's or a host's.
 */
import { execFileSync } from "node:child_process";
import { X509Certificate } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/** A certificate and its key, in PEM. */
export interface Certificate {
  readonly cert: string;
  readonly key: string;
}

/** How long a certificate it makes is valid. */
export const VALID_DAYS = 365;
/** How near its end a certificate is made anew, at the setup's next run. */
export const RENEW_DAYS = 60;

const IPV4 = /^(\d{1,3}\.){3}\d{1,3}$/u;

/** The subject alternative name of `address`, as Node says one. */
const altName = (address: string): string => (IPV4.test(address) ? `IP Address:${address}` : `DNS:${address}`);
/** That name as OpenSSL takes it. */
const opensslAltName = (address: string): string => (IPV4.test(address) ? `IP:${address}` : `DNS:${address}`);

/** A new self-signed certificate for `address`, valid VALID_DAYS days. */
export function makeCertificate(address: string): Certificate {
  const directory = mkdtempSync(join(tmpdir(), "alasio-webhook-tls-"));
  try {
    const [key, cert] = [join(directory, "tls.key"), join(directory, "tls.crt")];
    execFileSync("openssl", [
      "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-nodes", "-days", String(VALID_DAYS),
      "-keyout", key, "-out", cert, "-subj", `/CN=${address}`, "-addext", `subjectAltName=${opensslAltName(address)}`,
    ], { stdio: "pipe" });
    return { cert: readFileSync(cert, "utf8"), key: readFileSync(key, "utf8") };
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

/** Whether `current` serves `address` for more than RENEW_DAYS from `now`: one that does not parse does not. */
export function serves(current: Certificate, address: string, now: Date): boolean {
  try {
    const certificate = new X509Certificate(current.cert);
    const left = Date.parse(certificate.validTo) - now.getTime();
    return certificate.subjectAltName === altName(address) && left > RENEW_DAYS * 86_400_000;
  } catch {
    return false;
  }
}

/** The certificate the intake serves for `address`: `current`, while it serves it (serves), or one `make` makes. */
export function webhookCertificate(address: string, current: Certificate | null, now = new Date(), make = makeCertificate): { readonly certificate: Certificate; readonly made: boolean } {
  if (current && serves(current, address, now)) return { certificate: current, made: false };
  return { certificate: make(address), made: true };
}
