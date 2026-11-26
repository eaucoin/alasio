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

/** The rule `uid` of the image's alert rules, as its text says it: its severity, how long it must hold, its threshold, and its query. */
function ruleOf(uid: string): { readonly severity: string | undefined; readonly for: string | undefined; readonly threshold: string | undefined; readonly query: string } {
  const rules = readFileSync(new URL("provisioning/alerting/rules.yaml", IMAGE), "utf8");
  const at = rules.indexOf(`      - uid: ${uid}\n`);
  assert.ok(at >= 0, `no rule ${uid}`);
  const next = rules.indexOf("\n      - uid: ", at + 1);
  const text = rules.slice(at, next === -1 ? undefined : next);
  return {
    severity: /labels: \{severity: (\w+)\}/u.exec(text)?.[1],
    for: /^ {8}for: (\S+)$/mu.exec(text)?.[1],
    threshold: /conditions: \[\{evaluator: \{type: (\w+), params: \[([^\]]*)\]\}\}\]/u.exec(text)?.slice(1).join(" "),
    query: text,
  };
}

test("a node agent's own memory warns at 448 MiB of anonymous memory for five minutes, and pages on any OOM of its cgroup; never on its whole nor on its cgroup reaching its limit", () => {
  const memory = ruleOf("alasio-node-agent-memory");
  assert.deepEqual([memory.severity, memory.for, memory.threshold], ["warning", "5m", "gt 469762047"]);
  assert.match(memory.query, /MetricName = 'alasio.node_agent.memory' and Attributes\['alasio.memory.part'\] = 'anon'/u);
  // In bytes as the gauge says them, not rounded: at 448 MiB it fires, at 447.5 MiB not.
  assert.match(memory.query, /, arg_max\(Value, TimeUnix\) as anon$/mu);
  const fires = (bytes: number) => bytes > Number(memory.threshold?.split(" ")[1]);
  assert.deepEqual([fires(448 * 1024 ** 2), fires(447.5 * 1024 ** 2), fires(448 * 1024 ** 2 - 1)], [true, false, false]);
  const oom = ruleOf("alasio-node-agent-oom");
  assert.deepEqual([oom.severity, oom.for, oom.threshold], ["critical", "0s", "gt 0"]);
  assert.match(oom.query, /MetricName = 'alasio.node_agent.memory.events' and Attributes\['alasio.memory.event'\] in \('oom', 'oom_kill'\)/u);
  // Neither reads the agent's whole, nor its cgroup's high or max events.
  for (const { query } of [memory, oom]) assert.doesNotMatch(query.replace(/^ *#.*$/gmu, ""), /'cgroup'|'high'|'max'/u);
});
