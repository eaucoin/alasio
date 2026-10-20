/**
 * Kubernetes' quantities, as alasio's server reads them: the bytes a pod's memory request or
 * limit is, or the metrics server says it uses.
 */

/** What each suffix of a quantity multiplies it by. */
const UNITS: Readonly<Record<string, number>> = { Ki: 2 ** 10, Mi: 2 ** 20, Gi: 2 ** 30, Ti: 2 ** 40, Pi: 2 ** 50, Ei: 2 ** 60, k: 1e3, M: 1e6, G: 1e9, T: 1e12, P: 1e15, E: 1e18 };

/** What the quantity `quantity`, as `512Mi` or `1.5G`, is, in its unit's ones; NaN where it is none. */
export function quantityOf(quantity: unknown): number {
  const [, amount = "", unit = ""] = /^([0-9]+(?:\.[0-9]+)?)([A-Za-z]*)$/u.exec(typeof quantity === "string" ? quantity : String(quantity ?? "")) ?? [];
  return amount === "" ? Number.NaN : Number(amount) * (unit === "" ? 1 : UNITS[unit] ?? Number.NaN);
}
