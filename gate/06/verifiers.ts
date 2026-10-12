/**
 * Every role's SCRAM verifier from one source, the stack's root Secret: its password,
 * salted by a salt derived from the role's name and a secret of the stack's, so whoever
 * holds the Secret (the compute's roles, as set here; the control plane answering the
 * proxy) writes the same bytes. Neon's proxy authenticates to the compute with the keys
 * of the verifier the control plane gives it, so the two must be identical
 * (program/06-neon-wake.md, Corrections 1).
 */
import { createHmac } from "node:crypto";

import type { StackSecrets } from "../../neon/control/secrets.ts";
import { scramVerifier } from "../../neon/control/scram.ts";

/** The roles that reach alasio's database, by the root Secret's key of each one's password. */
export const ROLES = {
  alasio: "alasioPassword",
  lake: "lakePassword",
  lake_reader: "lakeReaderPassword",
  grafana: "grafanaPassword",
} as const satisfies Record<string, keyof StackSecrets>;

export type Role = keyof typeof ROLES;

export const isRole = (name: string): name is Role => Object.hasOwn(ROLES, name);

/** `role`'s verifier, the same wherever it is made from `secrets`. */
export function verifier(secrets: StackSecrets, role: Role): string {
  const salt = createHmac("sha256", secrets.computeControlToken).update(`scram-salt:${role}`).digest().subarray(0, 16);
  return scramVerifier(secrets[ROLES[role]], salt);
}
