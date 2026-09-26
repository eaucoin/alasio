import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

const servicePath = fileURLToPath(new URL("../systemd/alasio.service", import.meta.url));
const gitignorePath = fileURLToPath(new URL("../.gitignore", import.meta.url));

test("alasio service contains descendant OOMs without suppressing main-process recovery", () => {
  const service = readFileSync(servicePath, "utf8");

  assert.match(service, /^OOMPolicy=continue$/m);
  assert.match(service, /^Restart=always$/m);
  assert.match(service, /^TimeoutStopSec=30$/m);
  assert.match(
    service,
    /^ExecStart=\/home\/operator\/\.nvm\/versions\/node\/v24\.14\.0\/bin\/node \.\/src\/index\.js$/m,
  );
  assert.match(
    service,
    /^Environment="PATH=\/home\/operator\/\.nvm\/versions\/node\/v24\.14\.0\/bin:/m,
  );
});

test("alasio keeps checkout-local runtime configuration outside authored source", () => {
  const ignored = readFileSync(gitignorePath, "utf8").split(/\r?\n/u);
  assert.ok(ignored.includes(".env"));
});

test("standalone unit points restart provenance at its own wrapper and polkit rule", () => {
  const standalonePath = fileURLToPath(new URL("../systemd/alasio-standalone.service", import.meta.url));
  const rulesPath = fileURLToPath(new URL("../systemd/alasio-standalone.rules", import.meta.url));
  const standaloneInstallerPath = fileURLToPath(new URL("../install-alasio-standalone-service.sh", import.meta.url));
  const wrapperPath = fileURLToPath(new URL("../restart-alasio-standalone.sh", import.meta.url));
  const service = readFileSync(standalonePath, "utf8");
  const rules = readFileSync(rulesPath, "utf8");
  const installer = readFileSync(standaloneInstallerPath, "utf8");
  const wrapper = readFileSync(wrapperPath, "utf8");

  assert.match(service, /^Environment="ALASIO_SERVICE_UNIT=alasio-standalone\.service"$/m);
  assert.match(service, /^Environment="ALASIO_RESTART_WRAPPER=\/home\/operator\/alasio\/restart-alasio-standalone\.sh"$/m);
  assert.match(rules, /action\.lookup\("unit"\) === "alasio-standalone\.service"/);
  assert.match(rules, /action\.lookup\("verb"\) === "restart"/);
  assert.match(rules, /subject\.user === "operator"/);
  assert.match(installer, /^POLKIT_TARGET="\/etc\/polkit-1\/rules\.d\/50-alasio-standalone\.rules"$/m);
  assert.match(installer, /^sudo install -m 0644 "\$POLKIT_SOURCE" "\$POLKIT_TARGET"$/m);
  assert.match(wrapper, /^if ! systemctl --no-ask-password restart "\$UNIT_NAME"; then$/m);
});

test("standalone unit runs the bot in its container, stopped through the container's init", () => {
  const standalonePath = fileURLToPath(new URL("../systemd/alasio-standalone.service", import.meta.url));
  const runPath = fileURLToPath(new URL("../container/run.sh", import.meta.url));
  const service = readFileSync(standalonePath, "utf8");
  const run = readFileSync(runPath, "utf8");

  assert.match(service, /^Requires=docker\.service$/m);
  assert.match(
    service,
    /^ExecStart=\/home\/operator\/alasio\/container\/run\.sh \/home\/operator\/\.nvm\/versions\/node\/v24\.14\.0\/bin\/node \.\/src\/index\.js$/m,
  );
  assert.match(service, /^ExecStop=\/usr\/bin\/docker stop --timeout 25 alasio-standalone$/m);
  assert.match(service, /^Restart=always$/m);
  assert.match(run, /--rm --init --name "\$NAME"/);
  assert.match(run, /--network host --pid host --ipc host/);
  assert.match(run, /--volume \/home:\/home /);
  assert.match(run, /--volume \/run\/dbus\/system_bus_socket:\/run\/dbus\/system_bus_socket/);
});
