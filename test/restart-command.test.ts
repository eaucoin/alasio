import assert from "node:assert/strict";
import { test } from "node:test";

import { looksLikeSelfRestartCommand, looksLikeSelfRestartNearMiss } from "../src/policy/restart-command.ts";

const env = { ALASIO_DEPLOYMENT: "alasio" };

test("a rollout restart of alasio's own Deployment is a self-restart, however it is spelled", () => {
  for (const command of [
    "kubectl rollout restart deployment/alasio",
    "kubectl -n alasio rollout restart deployment/alasio",
    "kubectl rollout restart deploy/alasio --namespace alasio",
    "kubectl --namespace=alasio rollout restart deployment alasio",
    "/home/op/.local/bin/kubectl rollout restart deployments.apps/alasio",
    "bash -lc 'kubectl -n alasio rollout restart deployment/alasio'",
  ]) {
    assert.equal(looksLikeSelfRestartCommand(command, { env }), true, command);
    assert.equal(looksLikeSelfRestartNearMiss(command, { env }), false, command);
  }
});

test("a release's own Deployment name is the one that counts", () => {
  const named = { ALASIO_DEPLOYMENT: "bot-alasio" };
  assert.equal(looksLikeSelfRestartCommand("kubectl rollout restart deployment/bot-alasio", { env: named }), true);
  assert.equal(looksLikeSelfRestartCommand("kubectl rollout restart deployment/alasio", { env: named }), false);
});

test("other rollouts and other commands are not alasio restarting", () => {
  for (const command of [
    "kubectl rollout restart deployment/alasio-lake",
    "kubectl rollout status deployment/alasio",
    "kubectl delete pod alasio-abc",
    "systemctl restart alasio-standalone.service",
    "echo kubectl rollout restart deployment/alasio",
    "kubectl rollout restart deployment/alasio/x",
  ]) {
    assert.equal(looksLikeSelfRestartCommand(command, { env }), false, command);
  }
});

test("a rollout restart of alasio among others is a near miss, worth a warning", () => {
  for (const command of [
    "kubectl rollout restart deployment/alasio deployment/alasio-lake",
    "kubectl rollout restart deployment alasio extra",
  ]) {
    assert.equal(looksLikeSelfRestartCommand(command, { env }), false, command);
    assert.equal(looksLikeSelfRestartNearMiss(command, { env }), true, command);
  }
  assert.equal(looksLikeSelfRestartNearMiss("kubectl rollout restart deployment/alasio-lake", { env }), false);
});
