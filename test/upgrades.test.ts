/**
 * The cut-over of an installation of the release before (upgrades/), in what it decides
 * without a cluster: which addresses it refuses to have Telegram deliver to.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-7.0.0.sh", import.meta.url));

/** Whether the cut-over refuses `address` as private. */
function refusedAsPrivate(address: string): boolean {
  const { status, stderr } = spawnSync("bash", [SCRIPT, "private-address", address], { encoding: "utf8" });
  if (status !== 0 && status !== 1) throw new Error(`private-address ${address} exited with ${status}: ${stderr}`);
  return status === 0;
}

test("the cut-over refuses to have Telegram deliver to a private, shared, link-local, loopback, this-network, benchmarking or multicast address, and to no other", () => {
  const refused = ["10.0.0.1", "10.1.0.4", "172.16.0.1", "172.31.255.255", "192.168.1.10", "100.64.0.1", "100.101.102.103", "100.127.255.254", "169.254.169.254", "127.0.0.1", "0.0.0.0", "0.1.2.3", "198.18.0.1", "198.19.255.255", "224.0.0.1", "239.255.255.250", "255.255.255.255"];
  const taken = ["203.0.113.7", "9.255.255.255", "11.0.0.1", "172.15.255.255", "172.32.0.1", "192.167.0.1", "192.169.0.1", "100.63.255.255", "100.128.0.1", "169.253.0.1", "1.0.0.1", "198.17.255.255", "198.20.0.1", "223.255.255.255", "bot.example.com"];
  assert.deepEqual(refused.filter((address) => !refusedAsPrivate(address)), []);
  assert.deepEqual(taken.filter(refusedAsPrivate), []);
});
