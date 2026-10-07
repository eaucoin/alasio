/**
 * Neon's branches, as the lake learns them from neon-control (`GET /branches`, with a
 * token that can do nothing else): each but main is a branch environment whose lake
 * reads this one's files as of its branch point, and writes its own beside them.
 */
import type { BranchesSource } from "./config.ts";

/** The names of Neon's branches but main, as neon-control lists them; any state counts, as each may read the lake's files. */
export async function branchesBesideMain({ url, token }: BranchesSource): Promise<string[]> {
  const response = await fetch(url, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`neon-control answered ${response.status} to GET ${url}`);
  const body: unknown = await response.json();
  const listed = typeof body === "object" && body !== null && "branches" in body && Array.isArray(body.branches) ? body.branches : null;
  if (!listed) throw new Error(`neon-control answered GET ${url} with no list of branches`);
  return listed.map((branch: unknown) => (typeof branch === "object" && branch !== null && "name" in branch ? String(branch.name) : "")).filter((name) => name !== "main");
}
