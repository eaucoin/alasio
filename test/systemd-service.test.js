import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

const servicePath = fileURLToPath(new URL("../systemd/alasio.service", import.meta.url));
const installerPath = fileURLToPath(new URL("../install-alasio-service.sh", import.meta.url));
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
  assert.match(
    service,
    /^Environment="BAYMA_PYTHON_BIN=\/home\/operator\/monorepo\/\.agents\/skills\/monorepo-breadbutter\/\.venv\/bin\/python"$/m,
  );
});

test("alasio installer provisions locked runtime dependencies before capability checks", () => {
  const installer = readFileSync(installerPath, "utf8");
  const runtimeInstallIndex = installer.indexOf('npm ci --prefix "$SCRIPT_DIR"');
  const pythonSyncIndex = installer.indexOf('uv sync \\');
  const pythonBindingIndex = installer.indexOf('export BAYMA_PYTHON_BIN="${BAYMA_PYTHON_BIN:-$BREADBUTTER_ROOT/.venv/bin/python}"');
  const rustProvisionIndex = installer.indexOf('node "$BREADBUTTER_ROOT/provision-rust-workbench.mjs"');
  const capabilityDoctorIndex = installer.indexOf('npm --prefix "$SCRIPT_DIR" run doctor:breadbutter');

  assert.notEqual(runtimeInstallIndex, -1);
  assert.notEqual(pythonSyncIndex, -1);
  assert.notEqual(pythonBindingIndex, -1);
  assert.notEqual(rustProvisionIndex, -1);
  assert.notEqual(capabilityDoctorIndex, -1);
  assert.ok(runtimeInstallIndex < capabilityDoctorIndex);
  assert.ok(pythonSyncIndex < pythonBindingIndex);
  assert.ok(pythonBindingIndex < capabilityDoctorIndex);
  assert.ok(rustProvisionIndex < capabilityDoctorIndex);
});

test("alasio keeps checkout-local runtime configuration outside authored source", () => {
  const ignored = readFileSync(gitignorePath, "utf8").split(/\r?\n/u);
  assert.ok(ignored.includes(".env"));
});

test("standalone unit points restart provenance at its own wrapper and sudoers rule", () => {
  const standalonePath = fileURLToPath(new URL("../systemd/alasio-standalone.service", import.meta.url));
  const sudoersPath = fileURLToPath(new URL("../systemd/alasio-standalone.sudoers", import.meta.url));
  const standaloneInstallerPath = fileURLToPath(new URL("../install-alasio-standalone-service.sh", import.meta.url));
  const service = readFileSync(standalonePath, "utf8");
  const sudoers = readFileSync(sudoersPath, "utf8");
  const installer = readFileSync(standaloneInstallerPath, "utf8");

  assert.match(service, /^Environment="ALASIO_SERVICE_UNIT=alasio-standalone\.service"$/m);
  assert.match(service, /^Environment="ALASIO_RESTART_WRAPPER=\/home\/operator\/alasio\/restart-alasio-standalone\.sh"$/m);
  assert.match(sudoers, /^operator ALL=\(root\) NOPASSWD: \/usr\/bin\/systemctl restart alasio-standalone\.service$/m);
  assert.ok(installer.indexOf('sudo visudo -cf "$SUDOERS_SOURCE"') < installer.indexOf('sudo install -m 0440 "$SUDOERS_SOURCE" "$SUDOERS_TARGET"'));
});
