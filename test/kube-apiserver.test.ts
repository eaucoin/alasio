/** The throwaway API server the tests run on (test/support/kube-apiserver.ts). */
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { chmodSync, closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type AddressInfo, createServer, type Socket } from "node:net";
import { test } from "node:test";

import { end, ready, startApiServer, unpack } from "./support/kube-apiserver.ts";

/** A port of the loopback no one listens on now, as the support probes one. */
async function probed(): Promise<number> {
  const probe = createServer().listen(0, "127.0.0.1");
  await once(probe, "listening");
  const { port } = probe.address() as AddressInfo;
  probe.close();
  await once(probe, "close");
  return port;
}

/** What `promise` comes to, failing the test with `what` where it has not within `ms`, rather than hanging it to its timeout. */
async function within<T>(ms: number, what: string, promise: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => void (timer = setTimeout(() => reject(new Error(what)), ms)))]);
  } finally {
    clearTimeout(timer);
  }
}

test("a probe of the API server's readiness gives up on a port whose holder takes its connection and says nothing", async () => {
  // As a process that took the API server's port before it bound it may.
  const taken: Socket[] = [];
  const silent = createServer((socket) => void taken.push(socket)).listen(0, "127.0.0.1");
  await once(silent, "listening");
  try {
    assert.equal(await within(10_000, "the probe waited on past 10 s", ready((silent.address() as AddressInfo).port, Buffer.alloc(0), "token")), false);
  } finally {
    for (const socket of taken) socket.destroy();
    silent.close();
  }
});

test("a server that does not end on SIGTERM is ended by SIGKILL", async () => {
  const deaf = spawn("sh", ["-c", "trap '' TERM; echo deaf; while :; do sleep 1; done"], { stdio: ["ignore", "pipe", "ignore"] });
  // Deaf to SIGTERM once it says so.
  await once(deaf.stdout, "data");
  try {
    await within(30_000, "ending the server waited on past 30 s", end(deaf));
    assert.equal(deaf.signalCode, "SIGKILL");
  } finally {
    deaf.kill("SIGKILL");
  }
});

test("an API server whose etcd was killed by a signal is stopped, its etcd not waited for again", { timeout: 180_000 }, async () => {
  const { stop } = await startApiServer();
  // This process's etcd, a child of its own.
  const etcd = execFileSync("ps", ["-o", "pid=,comm=", "--ppid", String(process.pid)], { encoding: "utf8" })
    .split("\n").map((line) => line.trim().split(/\s+/u)).find(([, name]) => name === "etcd")?.[0];
  assert.ok(etcd, "no etcd runs as a child of this process");
  process.kill(Number(etcd), "SIGKILL");
  // Gone once this process has reaped it, as its exit is told then.
  for (const deadline = Date.now() + 10_000; ; await new Promise((resolve) => setTimeout(resolve, 50))) {
    try {
      process.kill(Number(etcd), 0);
    } catch {
      break;
    }
    assert.ok(Date.now() < deadline, `etcd ${etcd} did not end on SIGKILL`);
  }
  await within(30_000, "stopping the API server waited on past 30 s", stop());
});

test("an API server whose port another process takes between its probe and its bind is started again on ports found free anew", { timeout: 180_000 }, async () => {
  // Taken, as another test file's process may take a port found free, before kube-apiserver binds it.
  const taken = createServer().listen(0, "127.0.0.1");
  await once(taken, "listening");
  const takenPort = (taken.address() as AddressInfo).port;
  // etcd's two, then the API server's: the one taken, the first time.
  let asked = 0;
  const ports = () => ((asked += 1) === 3 ? Promise.resolve(takenPort) : probed());
  try {
    const { api, stop } = await startApiServer(ports);
    try {
      assert.notEqual(new URL(api.server).port, String(takenPort));
      // Started again at least once, each time on three ports found free anew: more than once
      // where a port found free was taken again, as the test files' processes beside it may.
      assert.ok(asked >= 6 && asked % 3 === 0, `asked for ${asked} ports`);
    } finally {
      await stop();
    }
  } finally {
    taken.close();
  }
});

/** Whether the process `pid` runs. */
const running = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

test("an API server whose kube-apiserver cannot be started, as one still being written cannot (ETXTBSY), fails to start, its etcd ended, none left to hold the process open", { timeout: 60_000 }, async () => {
  const bin = mkdtempSync(join(tmpdir(), "alasio-envtest-bin-"));
  // An etcd that says its PID and serves nothing until it is ended; a kube-apiserver held open for writing, as another process unpacking it holds it.
  writeFileSync(join(bin, "etcd"), '#!/bin/sh\necho $$ > "$(dirname "$0")/etcd.pid"\nexec sleep 600\n', { mode: 0o755 });
  const writing = openSync(join(bin, "kube-apiserver"), "w", 0o755);
  writeSync(writing, "#!/bin/sh\n");
  let etcd: number | undefined;
  try {
    await assert.rejects(startApiServer(probed, async () => bin), /ETXTBSY/u);
    etcd = existsSync(join(bin, "etcd.pid")) ? Number(readFileSync(join(bin, "etcd.pid"), "utf8")) : undefined;
    if (etcd !== undefined) {
      for (const deadline = Date.now() + 15_000; running(etcd); await new Promise((resolve) => setTimeout(resolve, 50))) {
        assert.ok(Date.now() < deadline, `etcd ${etcd} runs on after the start failed`);
      }
    }
  } finally {
    closeSync(writing);
    if (etcd !== undefined && running(etcd)) process.kill(etcd, "SIGKILL");
    rmSync(bin, { recursive: true, force: true });
  }
});

test("the release unpacked by several at once, as each test file's process with nothing in its cache unpacks it, is whole wherever it is found, each finding it", { timeout: 60_000 }, async () => {
  const work = mkdtempSync(join(tmpdir(), "alasio-envtest-unpack-"));
  try {
    // An archive as the release's: its binaries under controller-tools/envtest, large enough to take a while to write.
    const source = join(work, "source", "controller-tools", "envtest");
    mkdirSync(source, { recursive: true });
    const body = Buffer.alloc(32 * 1024 * 1024, 7);
    for (const name of ["etcd", "kube-apiserver"]) {
      writeFileSync(join(source, name), body);
      chmodSync(join(source, name), 0o755);
    }
    execFileSync("tar", ["-czf", join(work, "envtest.tar.gz"), "-C", join(work, "source"), "controller-tools"]);
    const archive = readFileSync(join(work, "envtest.tar.gz"));
    const cache = join(work, "cache");
    const unpacked = join(cache, "controller-tools", "envtest");
    // Looked at meanwhile, as a process that finds it there runs it: never there but whole.
    let looking = true;
    const partial: string[] = [];
    const look = (async () => {
      while (looking) {
        if (existsSync(unpacked)) {
          for (const name of ["etcd", "kube-apiserver"]) {
            const size = existsSync(join(unpacked, name)) ? readFileSync(join(unpacked, name)).length : -1;
            if (size !== body.length) partial.push(`${name}: ${size} bytes`);
          }
        }
        await new Promise((resolve) => setImmediate(resolve));
      }
    })();
    const found = await Promise.all(Array.from({ length: 4 }, () => unpack(cache, archive)));
    looking = false;
    await look;
    assert.deepEqual(found, Array.from({ length: 4 }, () => unpacked));
    assert.deepEqual(partial, []);
    assert.deepEqual(readdirSync(unpacked).toSorted(), ["etcd", "kube-apiserver"]);
    // Nothing left of each unpacking but the one moved into place.
    assert.deepEqual(readdirSync(cache), ["controller-tools"]);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
});
