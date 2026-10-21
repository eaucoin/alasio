/** The throwaway API server the tests run on (test/support/kube-apiserver.ts). */
import assert from "node:assert/strict";
import { once } from "node:events";
import { type AddressInfo, createServer } from "node:net";
import { test } from "node:test";

import { startApiServer } from "./support/kube-apiserver.ts";

/** A port of the loopback no one listens on now, as the support probes one. */
async function probed(): Promise<number> {
  const probe = createServer().listen(0, "127.0.0.1");
  await once(probe, "listening");
  const { port } = probe.address() as AddressInfo;
  probe.close();
  await once(probe, "close");
  return port;
}

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
      assert.equal(asked, 6, "started again, on three ports found free anew");
    } finally {
      await stop();
    }
  } finally {
    taken.close();
  }
});
