import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";

import { SqliteStore } from "../src/persistence/store.js";
import { createMetadataEngine } from "../src/sandbox/metadata-engine.js";
import { SessionHost } from "../src/sandbox/session-host.js";
import { SessionVolumeManager } from "../src/sandbox/volume.js";

const dirs = [];
function freshStore() {
  const dir = mkdtempSync(join(tmpdir(), "alasio-volstore-"));
  dirs.push(dir);
  return new SqliteStore(dir, join(dir, "q.sqlite"));
}
after(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });

const engine = createMetadataEngine({ url: "redis://alasio-valkey:6379", databases: 16 });
const config = {
  s3Endpoint: "http://seaweedfs:8333", s3Bucket: "sessions",
  s3AccessKey: "KEY", s3SecretKey: "SECRET", cacheMb: 2048,
  network: "alasio-sessions", metadataPasswordFile: "/run/valkey-pw",
  sessionHostImage: "alasio/session-host",
};

test("the volume repository reserves the lowest free namespace, uniquely, until exhausted", () => {
  const store = freshStore();
  const repo = store.sessionVolumes;
  assert.deepEqual(repo.reserveVolume("fs-aaa", 1, 2), { dbIndex: 1 });
  assert.deepEqual(repo.reserveVolume("fs-bbb", 1, 2), { dbIndex: 2 });
  assert.throws(() => repo.reserveVolume("fs-aaa", 1, 2), /already exists/); // same id
  repo.deleteVolume("fs-aaa"); // frees index 1
  assert.deepEqual(repo.reserveVolume("fs-ccc", 1, 2), { dbIndex: 1 }); // lowest free reused
  assert.throws(() => repo.reserveVolume("fs-ddd", 1, 2), /no free session-filesystem/); // 1,2 taken
  assert.deepEqual(repo.getVolume("fs-bbb"), { volumeId: "fs-bbb", dbIndex: 2, formatted: false });
  repo.setVolumeFormatted("fs-bbb", true);
  assert.equal(repo.getVolume("fs-bbb").formatted, true);
  assert.deepEqual(repo.listVolumes().map((v) => v.volumeId).sort(), ["fs-bbb", "fs-ccc"]);
});

test("the volume manager records a volume and builds its mount env, formatting only until formatted", () => {
  const store = freshStore();
  const manager = new SessionVolumeManager({ engine, store: store.sessionVolumes, docker: null, config });
  const created = manager.create("fs-work01");
  assert.equal(created.dbIndex, 1);
  let env = manager.mountEnv("fs-work01");
  assert.equal(env.JFS_META, "redis://alasio-valkey:6379/1");
  assert.equal(env.JFS_NAME, "fs-work01");
  assert.equal(env.JFS_FORMAT, "1"); // not yet formatted
  assert.equal(env.JFS_BUCKET, "http://seaweedfs:8333/sessions/fs-work01/");
  assert.equal(env.JFS_STORAGE, "s3");
  assert.equal(env.JFS_CACHE_MB, "2048");
  manager.markFormatted("fs-work01");
  env = manager.mountEnv("fs-work01");
  assert.equal(env.JFS_FORMAT, "0"); // stays formatted across restarts
  assert.throws(() => manager.mountEnv("fs-none"), /no such session volume/);
});

test("destroy runs a throwaway juicefs destroy and forgets the volume", async () => {
  const store = freshStore();
  const calls = [];
  const docker = { cli: async (args) => { calls.push(args); return { stdout: "", stderr: "" }; } };
  const manager = new SessionVolumeManager({ engine, store: store.sessionVolumes, docker, config });
  manager.create("fs-gone01");
  await manager.destroy("fs-gone01");
  assert.equal(store.sessionVolumes.getVolume("fs-gone01"), null);
  const run = calls.find((a) => a[0] === "run");
  assert.ok(run, "a docker run was issued");
  assert.ok(run.includes("alasio/session-host") && run.includes("--network") && run.includes("alasio-sessions"));
  assert.ok(run.join(" ").includes("juicefs destroy"));
  await manager.destroy("fs-gone01"); // idempotent: no record, no throw
});

test("the session host run args carry the mount env, the isolation flags, and the gateway", () => {
  const host = new SessionHost({
    docker: { spawnArgs: (args) => ({ command: "docker", args }) },
    config: {
      network: "alasio-sessions", agentImage: "alasio/agent", sessionHostImage: "alasio/session-host",
      memoryMb: 2048, cpus: 2, pidsLimit: 512, hostPublicIp: "203.0.113.5",
      metadataPasswordFile: "/run/valkey-pw", gateway: { ip: "10.0.0.9", port: 8080 },
    },
  });
  const args = host.runArgs("fs-work01", { mountEnv: { JFS_META: "redis://v/1", JFS_NAME: "fs-work01" }, netMode: "full" });
  const s = args.join(" ");
  assert.ok(s.includes("--detach") && s.includes("--privileged"));
  assert.ok(s.includes("--name alasio-session-fs-work01"));
  assert.ok(s.includes("type=image,source=alasio/agent,target=/agent-root"));
  assert.ok(s.includes("--pids-limit 512") && s.includes("--memory 2048m"));
  assert.ok(s.includes("JFS_META=redis://v/1"));
  assert.ok(s.includes("NET_MODE=full") && s.includes("GATEWAY_IP=10.0.0.9") && s.includes("GATEWAY_PORT=8080"));
  assert.ok(s.includes("HOST_PUBLIC_IP=203.0.113.5") && s.includes("CHECKPOINT_ON_STOP=1"));
  assert.ok(args.at(-1) === "alasio/session-host");
  const cmd = host.execCommand("fs-work01", ["claude", "--version"]);
  assert.deepEqual(cmd, { command: "docker", args: ["exec", "-i", "alasio-session-fs-work01", "agent-exec", "claude", "--version"] });
});
