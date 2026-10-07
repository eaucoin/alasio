/**
 * Grafana's image (neon/grafana), as far as it is alasio's: what its start script
 * provisions Grafana with before Grafana starts.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

const IMAGE = new URL("../neon/grafana/", import.meta.url);

/** The image's start script, run with `env` up to where it starts Grafana: what it provisioned, or why it failed. */
function start(env: Readonly<Record<string, string>>): { provisioned: (path: string) => string; failed: string | null } {
  const root = mkdtempSync(join(tmpdir(), "alasio-grafana-"));
  try {
    cpSync(new URL("provisioning", IMAGE), join(root, "image", "provisioning"), { recursive: true });
    const script = readFileSync(new URL("start.sh", IMAGE), "utf8");
    const at = script.indexOf("exec /run.sh");
    assert.ok(at > 0, "it ends by starting Grafana");
    writeFileSync(join(root, "image", "start.sh"), script.slice(0, at), { mode: 0o755 });
    const provisioning = join(root, "provisioning");
    let failed: string | null = null;
    try {
      execFileSync(join(root, "image", "start.sh"), { env: { PATH: process.env["PATH"], GF_PATHS_PROVISIONING: provisioning, ...env }, stdio: "pipe" });
    } catch (error) {
      failed = (error as { stderr: Buffer }).stderr.toString("utf8").trim();
    }
    const files = new Map<string, string>();
    for (const path of ["alerting/contact-points.yaml", "alerting/rules.yaml", "datasources/lake.yaml"]) {
      try {
        files.set(path, readFileSync(join(provisioning, path), "utf8"));
      } catch {
        // Not provisioned.
      }
    }
    return { provisioned: (path) => files.get(path) ?? "", failed };
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

test("Grafana alerts each of the bot's allowed users, its token read from the environment, and is provisioned with the image's files", () => {
  const { provisioned, failed } = start({ TELEGRAM_ALLOWED_USER_IDS: "1001,1002", TELEGRAM_BOT_TOKEN: "123:secret" });
  assert.equal(failed, null);
  assert.equal(provisioned("alerting/contact-points.yaml"), [
    "apiVersion: 1",
    "contactPoints:",
    "  - orgId: 1",
    "    name: telegram",
    "    receivers:",
    ...["1001", "1002"].flatMap((user) => [`      - uid: telegram-${user}`, "        type: telegram", "        settings:", "          bottoken: $TELEGRAM_BOT_TOKEN", `          chatid: "${user}"`]),
    "",
  ].join("\n"));
  assert.match(provisioned("alerting/rules.yaml"), /uid: alasio-turns-failing/u);
  assert.match(provisioned("datasources/lake.yaml"), /bearerToken: \$LAKE_QUERY_TOKEN/u);
});

test("Grafana does not start without a user to alert, or with what is not a user's id", () => {
  assert.equal(start({ TELEGRAM_ALLOWED_USER_IDS: "", TELEGRAM_BOT_TOKEN: "123:secret" }).failed, "TELEGRAM_ALLOWED_USER_IDS names no user to alert");
  assert.equal(start({ TELEGRAM_ALLOWED_USER_IDS: "1001,x: y", TELEGRAM_BOT_TOKEN: "123:secret" }).failed, "TELEGRAM_ALLOWED_USER_IDS holds x:, which is not a Telegram user's id");
});
