/**
 * 08's cut-over (gate/08/cutover.sh) and its rollback (gate/08/rollback.sh), rehearsed on
 * k3s on this machine as C1 meets the box: alasio 5.1.0, the box's release, installed by its
 * published package on the host target, with the stand-ins for Telegram (test/e2e/stand-ins.ts)
 * in place of Telegram, and in use: a session workspace, live, its files on JuiceFS; a branch
 * environment; rows in Neon, megabytes of them; thousands of versions and delete markers in
 * the store, beside what alasio, Neon, the lake, JuiceFS and the backups put there, and objects
 * of more than one chunk of the kinds and sizes the box's store holds, multipart dumps of
 * hundreds of megabytes among them; and, as each cut-over starts, CronJobs' Jobs running. Then
 *
 * - `forward`: the release's upgrade alone, which refuses while the store keeps its data on
 *   its one pod's volume; then the cut-over, on one OUT, failing after each step (5a and 6a
 *   within steps 5 and 6) and run again, going on from the step it reached; its upgrade (this
 *   checkout's command line and images, alasio's and Grafana's built here, the others the
 *   5.1.0 ones, which 08 leaves as they were) failing partway, the rollback refusing then, and
 *   the cut-over run again going on with it, twice; alasio, its session, its branch and its
 *   data checked as before, and no CronJob left suspended;
 * - `rollback <step>...`: for each step, on the same cluster, the cut-over with a failure
 *   injected once that step has run, run again and failing there again, its step recorded no
 *   less; the old store's pod, from step 2 to 5, reached by none of the stack's pods, as a
 *   probe proves; the rollback; the cut-over refusing the OUT rolled back; and alasio 5.1.0,
 *   its session, its branch and its data checked as before. The step `bytes` is a version
 *   read back as other bytes than before: the cut-over stopping at its checks, the rollback
 *   recording the old store's answer as differing, and bringing the stack back all the same.
 *   The steps `stop-claim`, `stop-volume`, `stop-maxid` and `stop-listing` (STOPS) are the
 *   rollback stopping, alasio still at none, where the old store would write or allocate
 *   wrongly, and going on once that is put right.
 *
 * Folder workspaces are on, as on the box: a Sandbox of their bayma's pod template, which no
 * cut-over may stop, is checked untouched through every failure.
 *
 * As the runner's user, who may sudo without a password: the scripts run as root, as on the
 * box. Everything is recorded under WORK.
 *
 *   WORK=<dir> IMAGES=<alasio's image>,<Grafana's image> node gate/08/rehearse.ts forward | rollback <step>...
 */
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { userInfo } from "node:os";
import { join } from "node:path";

import { standInObjects, STAND_INS, TELEGRAM, TELEGRAM_BRANCH, urlOf } from "../../test/e2e/stand-ins.ts";
import { type CallsListing, createTelegramStub, type RecordedCall } from "../../test/e2e/telegram-stub.ts";

const TREE = join(import.meta.dirname, "..", "..");
const WORK = process.env["WORK"] ?? fail("WORK names the rehearsal's directory");
const [ALASIO_IMAGE = "", GRAFANA_IMAGE = ""] = (process.env["IMAGES"] ?? fail("IMAGES names the release's images built here")).split(",");
const PREVIOUS_VERSION = "5.1.0";
const OPERATOR = 1001;
/** The chat, another user's, that is in a folder workspace. */
const FOLDER_CHAT = 1002;
const SESSIONS = "alasio-sessions";
const BRANCH = "rehearsal";

function fail(message: string): never {
  throw new Error(message);
}

const say = (line: string) => console.error(`# ${new Date().toISOString().slice(11, 19)} ${line}`);
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const configHome = join(WORK, "config");
const configFile = join(configHome, "alasio", "config.json");
const kubeconfig = join(configHome, "alasio", "kubeconfig");
const cliEnv: NodeJS.ProcessEnv = { ...process.env, XDG_CONFIG_HOME: configHome, XDG_DATA_HOME: join(WORK, "data") };
for (const name of ["KUBECONFIG", "TELEGRAM_BOT_TOKEN", "TELEGRAM_API_ROOT"]) delete cliEnv[name];

/** Runs `command`, which must succeed: what it printed. */
function run(command: string, args: readonly string[], { env = cliEnv, input }: { readonly env?: NodeJS.ProcessEnv; readonly input?: string } = {}): string {
  const ran = spawnSync(command, args, { env, input, encoding: "utf8", maxBuffer: 1 << 30, stdio: ["pipe", "pipe", "inherit"] });
  if (ran.status !== 0) throw new Error(`${command} ${args.join(" ")} exited with ${ran.status ?? ran.signal}`);
  return ran.stdout;
}
/** Runs `command`, which must succeed, leaving this process's event loop free, as for a server of its own the command talks to. */
function runFree(command: string, args: readonly string[], env: NodeJS.ProcessEnv): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env, stdio: ["ignore", "inherit", "inherit"] });
    child.on("error", reject);
    child.on("exit", (code, signal) => (code === 0 ? resolve() : reject(new Error(`${command} ${args.join(" ")} exited with ${code ?? signal}`))));
  });
}
/** Runs `command`, which may fail: its exit code, and what it said on stderr. */
function attempt(command: string, args: readonly string[], env: NodeJS.ProcessEnv): { readonly code: number; readonly stderr: string } {
  const ran = spawnSync(command, args, { env, encoding: "utf8", maxBuffer: 1 << 30, stdio: ["ignore", "inherit", "pipe"] });
  process.stderr.write(ran.stderr);
  return { code: ran.status ?? 1, stderr: ran.stderr };
}

const kubectl = (...args: string[]) => run("kubectl", ["--kubeconfig", kubeconfig, ...args]);
/** The previous release's command line, as the box has it, from its published package, installed under WORK. */
const PREVIOUS_CLI = join(WORK, "previous", "node_modules", ".bin", "alasio");
const previous = (...args: string[]) => run(PREVIOUS_CLI, args);
/** This checkout's command line, the release's. */
const release = (...args: string[]) => run("node", [join(TREE, "cli", "src", "main.ts"), ...args]);

/** The install configuration of the rehearsal: what both releases take, with requests small enough for a runner. */
const INSTALL = {
  alasio: { env: { TELEGRAM_API_ROOT: urlOf(TELEGRAM) }, resources: { requests: { cpu: "50m", memory: "256Mi" } } },
  sessions: { storage: { size: "1Gi" }, resources: { requests: { cpu: "50m", memory: "128Mi" }, limits: { cpu: "1", memory: "1Gi" } } },
  neon: {
    safekeepers: { storage: { size: "2Gi" }, resources: { requests: { cpu: "20m", memory: "64Mi" } } },
    pageserver: { storage: { size: "5Gi" }, resources: { requests: { cpu: "50m", memory: "128Mi" } } },
    storageController: { resources: { requests: { cpu: "10m", memory: "64Mi" } } },
    storageBroker: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
    controllerDb: { resources: { requests: { cpu: "10m", memory: "64Mi" } } },
    control: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
    compute: { resources: { requests: { cpu: "50m", memory: "256Mi" } } },
  },
  telemetry: { collector: { resources: { requests: { cpu: "10m", memory: "64Mi" } } } },
  lake: { resources: { requests: { cpu: "20m", memory: "256Mi" } }, query: { resources: { requests: { cpu: "10m", memory: "128Mi" } } } },
  grafana: { resources: { requests: { cpu: "20m", memory: "192Mi" } } },
  workspaceStorage: { backupInterval: "5m" },
  agentSandbox: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
};

/**
 * The release's install configuration: the previous one's, with the images built here for what
 * 08 changed, and the previous release's published ones for what it leaves as they were, as
 * this checkout's command line, of no release, names none of its own.
 */
function releaseInstall(): Record<string, unknown> {
  const image = (reference: string) => {
    const colon = reference.lastIndexOf(":");
    return { repository: reference.slice(0, colon), tag: reference.slice(colon + 1), digest: "" };
  };
  const published = (name: string) => ({ repository: `ghcr.io/eaucoin/${name}`, tag: PREVIOUS_VERSION, digest: "" });
  return {
    ...INSTALL,
    images: { alasio: image(ALASIO_IMAGE), grafana: image(GRAFANA_IMAGE), agent: published("alasio-agent"), lake: published("alasio-lake"), pullPolicy: "IfNotPresent" },
  };
}

/**
 * A command the scripts run as root, which runs the shell script `body` as this user, in the
 * rehearsal's configuration: the file it is written to, beside the script's own.
 */
function asUser(name: string, body: string): string {
  const file = join(WORK, `${name}.sh`);
  writeFileSync(`${file}.body`, `set -e\n${body}\n`, { mode: 0o644 });
  const env = ["HOME", "PATH", "XDG_CONFIG_HOME", "XDG_DATA_HOME"].map((key) => `'${key}=${cliEnv[key] ?? ""}'`).join(" ");
  writeFileSync(file, `#!/bin/sh\nexec sudo -u ${userInfo().username} env ${env} sh '${file}.body'\n`, { mode: 0o755 });
  return file;
}
/** A shell line that makes the configuration's install `install`, the release's or the previous one's. */
function configured(name: string, install: Record<string, unknown>): string {
  const file = join(WORK, `${name}-install.json`);
  writeFileSync(file, JSON.stringify(install));
  return `node -e 'const fs = require("fs"); const [file, install] = process.argv.slice(1); const config = JSON.parse(fs.readFileSync(file, "utf8")); config.install = JSON.parse(fs.readFileSync(install, "utf8")); fs.writeFileSync(file, JSON.stringify(config))' '${configFile}' '${file}'`;
}
const RELEASE_CLI = join(TREE, "cli", "src", "main.ts");
/**
 * The cut-over's upgrade, as on the box: the configuration made the release's, then its
 * command line's upgrade, given `timeout` to have alasio run.
 */
const UPGRADE = (timeout = "30m") => asUser(`upgrade-${timeout}`, [configured("release", releaseInstall()), `node '${RELEASE_CLI}' upgrade --timeout ${timeout}`].join("\n"));
/** That the upgrade can run, as the box's `npx alasio@<v> --version` proves. */
const UPGRADE_CHECK = () => asUser("upgrade-check", `node '${RELEASE_CLI}' --version`);
/** The rollback's previous release, the configuration made its again, applied again by its own command line. */
const PREVIOUS_CHECK = () => asUser("previous-check", `'${PREVIOUS_CLI}' --version`);
const PREVIOUS = () => asUser("previous", [configured("previous", INSTALL), `'${PREVIOUS_CLI}' upgrade --timeout 30m`].join("\n"));

// --- the stand-in for Telegram, as the operator talks to alasio through it ---

/** The port-forwards the rehearsal runs, ended as it ends. */
const forwards = new Set<ChildProcess>();

/**
 * The stand-in's Service, reached through a local port forwarded to it: its base URL, once
 * it answers. A forward ends with the connection it rides on, as when k3s restarts, so a
 * request that fails makes it anew, and tries again.
 */
function standIn(service: string, port: number, local: number) {
  const base = `http://127.0.0.1:${local}`;
  let child: ChildProcess | undefined;
  const open = async () => {
    if (child) {
      child.kill();
      forwards.delete(child);
    }
    child = spawn("kubectl", ["--kubeconfig", kubeconfig, "-n", STAND_INS, "port-forward", `svc/${service}`, `${local}:${port}`], { stdio: "ignore" });
    forwards.add(child);
    for (let tries = 0; tries < 60; tries += 1) {
      if (await fetch(`${base}/control/calls`).then((response) => response.ok, () => false)) return;
      await sleep(1000);
    }
    fail(`the stand-in ${service} is not reached through its forward`);
  };
  return {
    base,
    open,
    async request(path: string, init?: RequestInit): Promise<unknown> {
      for (let tries = 0; ; tries += 1) {
        try {
          return await (await fetch(`${base}${path}`, init)).json();
        } catch (error) {
          if (tries >= 5) throw error;
          await open();
        }
      }
    },
  };
}

function operator(telegram: ReturnType<typeof standIn>) {
  let seen = 0;
  const post = (path: string, body: unknown) => telegram.request(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  const calls = async (): Promise<readonly RecordedCall[]> => {
    const listed = (await telegram.request(`/control/calls?since=${seen}`)) as CallsListing;
    seen = listed.total;
    return listed.calls;
  };
  const buttons = (call: RecordedCall) => {
    const markup = call.payload.reply_markup;
    return (markup && "inline_keyboard" in markup ? markup.inline_keyboard : []).flat();
  };
  return {
    calls,
    say: (text: string, user?: number) => post("/control/message", { text, ...(user === undefined ? {} : { chatId: user, userId: user }) }),
    async waitFor(predicate: (call: RecordedCall) => boolean, what: string, timeoutMs = 600_000): Promise<RecordedCall> {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const hit = (await calls()).find(predicate);
        if (hit) return hit;
        await sleep(1000);
      }
      throw new Error(`alasio sent nothing that ${what}`);
    },
    async choose(label: RegExp) {
      const call = await this.waitFor((candidate) => buttons(candidate).some((button) => label.test(button.text)), `had a button ${label}`);
      const button = buttons(call).find((candidate) => label.test(candidate.text));
      await post("/control/callback", { data: button && "callback_data" in button ? button.callback_data : undefined });
    },
  };
}

// --- what the rehearsal checks is kept ---

interface Kept {
  readonly volumeId: string;
  /** The pod of the folder workspace's Sandbox, by its uid, which no cut-over stops. */
  readonly folder: string;
  readonly proof: string;
  readonly payload: string;
  readonly rows: string;
}

const onDatabase = (sql: string) => {
  const pod = kubectl("-n", "alasio", "get", "pods", "-l", "app.kubernetes.io/component=neon-compute", "--field-selector=status.phase=Running", "-o", "jsonpath={.items[0].metadata.name}");
  return kubectl("-n", "alasio", "exec", pod, "-c", "compute", "--", "psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", "alasio", "-Atc", sql).trim();
};
const inSession = (volumeId: string, code: string) => kubectl("-n", SESSIONS, "exec", volumeId, "-c", "bayma", "--", "node", "-e", code);
const inOldStore = (script: string) => kubectl("-n", "alasio", "exec", "alasio-seaweedfs-0", "-c", "lifecycle", "--", "sh", "-c", script);

/** Waits, at most `timeoutMs`, until `check` holds, saying `what` if it never does. */
async function until(what: string, check: () => boolean, timeoutMs = 900_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    let held = false;
    try {
      held = check();
    } catch {
      held = false;
    }
    if (held) return;
    if (Date.now() > deadline) throw new Error(`waited ${timeoutMs / 1000}s for ${what}`);
    await sleep(5000);
  }
}

/** alasio, as the operator finds it: its session workspace's panel, its rows, its session's files, its branch. */
async function check(tg: ReturnType<typeof operator>, kept: Kept, cli: (...args: string[]) => string): Promise<void> {
  await until("every workload of alasio's to be ready", () => {
    const workloads = cli("status").split("\n").filter((line) => /^ {2}(Deployment|StatefulSet) /u.test(line));
    return workloads.length > 0 && workloads.every((line) => line.endsWith(": ready"));
  });
  await tg.calls();
  await tg.say("/workspace");
  const panel = await tg.waitFor((call) => /^Session workspace: /mu.test(call.payload.text ?? ""), "showed the workspace's panel");
  if (!new RegExp(`^Session workspace: ${kept.volumeId}, no internet$`, "mu").test(panel.payload.text ?? "")) fail(`the panel says ${panel.payload.text}`);
  const rows = onDatabase("select count(*) || ' ' || md5(string_agg(payload, ',' order by id)) from rehearsal");
  if (rows !== kept.rows) fail(`the database holds ${rows}, not ${kept.rows}`);
  await until("the session's pod to run", () => kubectl("-n", SESSIONS, "get", "pod", kept.volumeId, "-o", "jsonpath={.status.phase}") === "Running");
  const files = inSession(kept.volumeId, 'const fs = require("fs"); console.log(fs.readFileSync("/workspace/proof", "utf8") + " " + require("crypto").createHash("sha256").update(fs.readFileSync("/workspace/payload")).digest("hex"))').trim();
  if (files !== `${kept.proof} ${kept.payload}`) fail(`the session's files read ${files}, not ${kept.proof} ${kept.payload}`);
  await until("the branch's alasio to be ready", () => new RegExp(`^${BRANCH}: a branch of main, .*its alasio ready`, "mu").test(cli("branch", "list")));
  say(`checked: the workspace's panel, ${rows.split(" ")[0]} rows as they were, the session's files, the branch`);
}

// --- the rehearsal ---

async function setUp(): Promise<{ readonly tg: ReturnType<typeof operator>; readonly kept: Kept }> {
  mkdirSync(join(configHome, "alasio"), { recursive: true, mode: 0o700 });
  writeFileSync(configFile, JSON.stringify({ target: { host: {} }, install: INSTALL }), { mode: 0o600 });
  const botToken = join(WORK, "bot-token");
  writeFileSync(botToken, "123:e2e", { mode: 0o600 });
  say(`alasio ${PREVIOUS_VERSION}, initialized by its published package, k3s with it`);
  run("npm", ["install", "--silent", "--no-save", "--prefix", join(WORK, "previous"), `alasio@${PREVIOUS_VERSION}`]);
  const stub = createTelegramStub();
  await new Promise<void>((resolve) => stub.server.listen(0, "127.0.0.1", resolve));
  try {
    const { port } = stub.server.address() as AddressInfo;
    // The stand-in answers in this process, which the command waits on without blocking it.
    const home = join(WORK, "home");
    mkdirSync(home, { recursive: true });
    chmodSync(home, 0o777);
    const owner = `${userInfo().uid}:${userInfo().gid}`;
    await runFree(PREVIOUS_CLI, ["init", "--non-interactive", "--bot-token-file", botToken, "--allowed-user-ids", `${OPERATOR},${FOLDER_CHAT}`, "--folder-workspaces", "--user", owner, "--home", home, "--no-up"], {
      ...cliEnv,
      TELEGRAM_API_ROOT: `http://127.0.0.1:${port}`,
    });
  } finally {
    await new Promise((resolve) => stub.server.close(resolve));
  }
  run("kubectl", ["--kubeconfig", kubeconfig, "apply", "-f", "-"], { input: JSON.stringify({ apiVersion: "v1", kind: "List", items: standInObjects() }) });
  for (const name of ["telegram-stub", "telegram-branch-stub", "otlp-sink"]) kubectl("-n", STAND_INS, "rollout", "status", `deployment/${name}`, "--timeout=300s");
  say(`alasio ${PREVIOUS_VERSION} up`);
  previous("up", "--timeout", "30m");

  say("the release's images, built here, given to k3s");
  for (const image of [ALASIO_IMAGE, GRAFANA_IMAGE]) {
    const saved = spawnSync("sh", ["-c", `docker save ${image} | sudo k3s ctr -n k8s.io images import -`], { stdio: "inherit" });
    if (saved.status !== 0) fail(`${image} was not imported`);
  }

  const telegram = standIn(TELEGRAM.service, TELEGRAM.port, 18081);
  await telegram.open();
  const tg = operator(telegram);
  await tg.waitFor((call) => call.method === "setMyCommands", "set its commands");
  say("a session workspace, its files on JuiceFS");
  await tg.say("/workspace");
  await tg.choose(/New empty workspace/u);
  await tg.choose(/No internet/u);
  const created = await tg.waitFor((call) => /Created and mounted session workspace/u.test(call.payload.text ?? ""), "made the workspace");
  const volumeId = /fs-[0-9a-f]+/u.exec(created.payload.text ?? "")?.[0] ?? fail(`alasio named no workspace: ${created.payload.text}`);
  const proof = `written before the cut-over, ${new Date().toISOString()}`;
  const payload = inSession(
    volumeId,
    `const fs = require("fs"); const data = require("crypto").randomBytes(8 << 20); fs.writeFileSync("/workspace/proof", ${JSON.stringify(proof)}); fs.writeFileSync("/workspace/payload", data); console.log(require("crypto").createHash("sha256").update(data).digest("hex"))`,
  ).trim();

  say("rows in Neon, megabytes of them");
  onDatabase("create table rehearsal (id int primary key, payload text)");
  onDatabase("insert into rehearsal select g, repeat(md5(g::text), 8) from generate_series(1, 100000) g");
  const rows = onDatabase("select count(*) || ' ' || md5(string_agg(payload, ',' order by id)) from rehearsal");

  say("thousands of versions and delete markers in Neon's versioned bucket, beside what is there");
  inOldStore([
    "set -e",
    "s3() { aws --endpoint-url http://127.0.0.1:8333 s3 \"$@\"; }",
    "mkdir -p /tmp/versions",
    "for round in 1 2 3; do",
    "  for i in $(seq 1 1000); do head -c $((512 + i)) /dev/urandom > /tmp/versions/object-$i; done",
    "  s3 cp --recursive --quiet /tmp/versions s3://neon/rehearsal/",
    "done",
    "for i in $(seq 1 200); do echo \"object-$i\"; done | while read -r key; do aws --endpoint-url http://127.0.0.1:8333 s3api delete-object --bucket neon --key \"rehearsal/$key\" >/dev/null; done",
    "rm -rf /tmp/versions",
  ].join("\n"));

  say("objects of the kinds and sizes the box's store holds: Neon's dumps, the lake's Parquet files, Neon's layers");
  boxObjects();

  say("a branch environment");
  const branchTelegram = standIn(TELEGRAM_BRANCH.service, TELEGRAM_BRANCH.port, 18082);
  await branchTelegram.open();
  const branchToken = join(WORK, "branch-token");
  writeFileSync(branchToken, "456:branch", { mode: 0o600 });
  const overrides = join(WORK, "overrides.json");
  writeFileSync(overrides, JSON.stringify({ alasio: { env: { TELEGRAM_API_ROOT: urlOf(TELEGRAM_BRANCH) } } }));
  run(PREVIOUS_CLI, ["branch", "create", BRANCH, "--bot-token-file", branchToken, "--overrides", overrides], {
    env: { ...cliEnv, TELEGRAM_API_ROOT: branchTelegram.base },
  });
  say("a folder workspace's Sandbox, as alasio makes one for the operator's own session on the machine");
  const folder = folderSandbox();
  const kept = { volumeId, folder, proof, payload, rows };
  writeFileSync(join(WORK, "kept.json"), JSON.stringify(kept, null, 2));
  await check(tg, kept, previous);
  return { tg, kept };
}

/**
 * Beside the many small objects, those the box's store holds that are of more than one chunk
 * (C1's inventory, program/progress/object-store.md), each as its writer writes it, from a pod
 * of the backup's upload container, which has the stack's S3 identity: Neon's dumps, the
 * smallest and the largest the box keeps, as the backup uploads them (`aws s3 cp`, parts of 8
 * MiB); the lake's Parquet files as DuckDB writes them, a single PUT of several chunks and a
 * multipart upload of parts of no whole number of chunks; Neon's largest layer, one PUT to its
 * versioned bucket; and a multipart object there, written twice, current and noncurrent. A
 * dump's ETags then differ as the box's did: listed by its 4 MiB chunks, read by its parts.
 */
function boxObjects(): void {
  const backup = JSON.parse(kubectl("-n", "alasio", "get", "cronjob", "alasio-neon-backup", "-o", "json")) as {
    spec: { jobTemplate: { spec: { template: { metadata: { labels: Record<string, string> }; spec: { containers: { name: string }[] } & Record<string, unknown> } } } };
  };
  const { metadata, spec } = backup.spec.jobTemplate.spec.template;
  const upload = spec.containers.find((container) => container.name === "upload") ?? fail("the backup has no upload container");
  const write = [
    "set -eu",
    's3() { aws s3 --endpoint-url "$S3_ENDPOINT" "$@"; }',
    's3api() { aws s3api --endpoint-url "$S3_ENDPOINT" "$@"; }',
    "object() { head -c \"$1\" /dev/urandom >/tmp/object; }",
    "printf '[default]\\ns3 =\\n  multipart_threshold = 80213616\\n  multipart_chunksize = 80213616\\n' >/tmp/duckdb-parts",
    'object 130170545; s3 cp --quiet /tmp/object "s3://$BUCKET/rehearsal/alasio-smallest.dump"',
    'object 275199644; s3 cp --quiet /tmp/object "s3://$BUCKET/rehearsal/alasio-largest.dump"',
    "object 18885630; s3api put-object --bucket lake --key rehearsal/one-put.parquet --body /tmp/object >/dev/null",
    "object 224329961; AWS_CONFIG_FILE=/tmp/duckdb-parts s3 cp --quiet /tmp/object s3://lake/rehearsal/multipart.parquet",
    "object 268460032; s3api put-object --bucket neon --key rehearsal/layer --body /tmp/object >/dev/null",
    "for round in 1 2; do object 20971520; s3 cp --quiet /tmp/object s3://neon/rehearsal/multipart; done",
    "rm /tmp/object",
  ].join("\n");
  const pod = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: "rehearsal-objects", namespace: "alasio", labels: metadata.labels },
    spec: {
      ...spec,
      initContainers: [],
      containers: [{ ...upload, name: "objects", command: ["/bin/sh", "-c", write], resources: { requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "1Gi" } } }],
    },
  };
  run("kubectl", ["--kubeconfig", kubeconfig, "apply", "-f", "-"], { input: JSON.stringify(pod) });
  try {
    kubectl("-n", "alasio", "wait", "pod/rehearsal-objects", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=1200s");
  } finally {
    process.stderr.write(kubectl("-n", "alasio", "logs", "rehearsal-objects"));
    kubectl("-n", "alasio", "delete", "pod", "rehearsal-objects", "--wait=false");
  }
  const s3api = "aws --endpoint-url http://127.0.0.1:8333 s3api";
  const listed = inOldStore(`${s3api} list-object-versions --bucket backups --prefix rehearsal/alasio-smallest.dump --query 'Versions[0].ETag' --output text`).trim();
  const read = inOldStore(`${s3api} head-object --bucket backups --key rehearsal/alasio-smallest.dump --query ETag --output text`).trim();
  if (!listed.endsWith('-32"') || !read.endsWith('-16"')) fail(`the smallest dump is listed as ${listed} and read as ${read}, not by its 32 chunks and its 16 parts`);
  say(`the smallest dump listed as ${listed}, read as ${read}`);
}

/** The folder workspaces' namespace, the host profile's, of the configuration's default. */
const HOST = "alasio-host";

/**
 * A Sandbox of the folder workspaces' bayma's pod template, as alasio makes one as a folder
 * conversation's turn starts (src/mcp/bayma.ts): its pod's labels, in their namespace; its
 * container a stand-in for bayma, which the rehearsal runs no turn for. The uid of its pod,
 * once it runs.
 */
function folderSandbox(): string {
  const labels = { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "folder-bayma" };
  const image = kubectl("-n", STAND_INS, "get", "deployment", "telegram-stub", "-o", "jsonpath={.spec.template.spec.containers[0].image}");
  run("kubectl", ["--kubeconfig", kubeconfig, "apply", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "agents.x-k8s.io/v1beta1",
      kind: "Sandbox",
      metadata: { name: "rehearsal-folder", namespace: HOST },
      spec: { podTemplate: { metadata: { labels }, spec: { containers: [{ name: "bayma", image, command: ["node", "-e", "setInterval(() => {}, 1 << 30)"] }] } } },
    }),
  });
  let uid = "";
  const running = () => {
    uid = kubectl("-n", HOST, "get", "pods", "-l", "app.kubernetes.io/component=folder-bayma", "--field-selector=status.phase=Running", "-o", "jsonpath={.items[*].metadata.uid}").trim();
    return uid !== "";
  };
  for (let tries = 0; tries < 120 && !running(); tries += 1) spawnSync("sleep", ["5"]);
  return uid || fail("the folder workspace's Sandbox runs no pod");
}

/** The folder workspace's Sandbox, as a cut-over leaves it, stopping writers: not suspended, its pod the one it ran. */
function folderUntouched(kept: Kept): void {
  const mode = kubectl("-n", HOST, "get", "sandbox", "rehearsal-folder", "-o", "jsonpath={.spec.operatingMode}");
  if (mode === "Suspended") fail("the cut-over suspended the folder workspace's Sandbox");
  const uid = kubectl("-n", HOST, "get", "pods", "-l", "app.kubernetes.io/component=folder-bayma", "--field-selector=status.phase=Running", "-o", "jsonpath={.items[*].metadata.uid}").trim();
  if (uid !== kept.folder) fail(`the folder workspace's pod is ${uid || "none"}, not the one it ran, ${kept.folder}`);
}

/** CronJobs' Jobs, running as a cut-over starts, as the box's may be. */
function runningJobs(round: string): void {
  for (const cronJob of ["alasio-neon-backup", "alasio-seaweedfs-lifecycle"]) {
    kubectl("-n", "alasio", "create", "job", `${cronJob}-${round}`, `--from=cronjob/${cronJob}`);
  }
}

/** The release's objects, as gate/08/render.ts renders them from the configuration the release is applied with. */
function render(out: string): { readonly release: string; readonly csi: string } {
  const file = join(out, "release-config.json");
  writeFileSync(file, JSON.stringify({ install: releaseInstall() }));
  const releaseFile = join(out, "release.json");
  writeFileSync(releaseFile, run("node", [join(TREE, "gate", "08", "render.ts"), file]));
  return { release: releaseFile, csi: run("node", [join(TREE, "gate", "08", "render.ts"), "--csi-namespace", file]).trim() };
}

/** Runs the script `name` as root, as on the box, with `env`: its exit code and what it said. */
function script(name: string, env: Readonly<Record<string, string>>): { readonly code: number; readonly stderr: string } {
  return attempt("sudo", ["env", `PATH=${process.env["PATH"]}`, `KUBECONFIG=${kubeconfig}`, ...Object.entries(env).map(([key, value]) => `${key}=${value}`), join(TREE, "gate", "08", name)], process.env);
}

/**
 * From step 2 to 5 the old store's pod is reached by the metadata's copier alone: a pod with
 * the stack's labels is refused at its S3 port, and one with the copier's, beside it, reaches
 * its filer, so the refusal is the seal's.
 */
function sealed(): void {
  const address = kubectl("-n", "alasio", "get", "pod", "alasio-seaweedfs-0", "-o", "jsonpath={.status.podIP}");
  const image = kubectl("-n", "alasio", "get", "pod", "alasio-seaweedfs-0", "-o", "jsonpath={.spec.containers[0].image}");
  /** What a pod of `labels` finds at the old store's `port`: open where any HTTP answers, refused otherwise. */
  const probe = (name: string, labels: string, port: number) => {
    kubectl("-n", "alasio", "run", name, "--restart=Never", `--image=${image}`, `--labels=${labels}`, "--command", "--", "sh", "-c",
      // Past the moment a new pod is refused by any, before its NetworkPolicies apply to it.
      `sleep 20; if wget -S -q -T 5 -O /dev/null http://${address}:${port}/ 2>&1 | grep -q HTTP/; then echo open; else echo refused; fi`);
    try {
      kubectl("-n", "alasio", "wait", `pod/${name}`, "--for=jsonpath={.status.phase}=Succeeded", "--timeout=120s");
      return kubectl("-n", "alasio", "logs", name);
    } finally {
      kubectl("-n", "alasio", "delete", "pod", name, "--wait=false");
    }
  };
  const stack = probe("cutover-probe-stack", "app.kubernetes.io/instance=alasio,alasio.dev/stack=neon,app.kubernetes.io/component=neon-probe", 8333);
  if (!/^refused$/mu.test(stack)) fail(`a pod of the stack reaches the old store's sealed pod: ${stack}`);
  const copier = probe("cutover-probe-copier", "alasio.dev/cutover=meta-backup", 8888);
  if (!/^open$/mu.test(copier)) fail(`the copier does not reach the old store's filer: ${copier}`);
  say("the old store's pod refuses a pod of the stack, and takes the copier");
}

/** The step a cut-over recorded in `out`, which root's scripts keep to root. */
const recorded = (out: string) => run("sudo", ["cat", join(out, "step")], { env: process.env }).trim();

/** Runs `command` as root, as the scripts do: what it printed. */
const asRoot = (command: string) => run("sudo", ["sh", "-c", command], { env: process.env });

/**
 * What the rollback stops at, the old store's writers stopped and it sealed, each made on a
 * cut-over that failed after step 6 (the volume on the release's claim, the old store
 * stopped), with what it says, and what puts it right again, after which the rollback goes
 * on: the volume held by another claim; a volume the old store had missing from it; a volume
 * id on the disk above any the old master would make; the old filer's metadata gone, its
 * versions with it.
 */
const STOPS = {
  "stop-claim": {
    says: "is held by 'rehearsal-other'",
    break: ({ pv }: Volume) => {
      kubectl("-n", "alasio", "delete", "pvc", "data-alasio-seaweedfs-volume-0", "--wait=true");
      const volume = JSON.parse(kubectl("get", "pv", pv, "-o", "json")) as { spec: { storageClassName: string; capacity: { storage: string }; accessModes: string[] } };
      kubectl("patch", "pv", pv, "--type", "merge", "-p", JSON.stringify({ spec: { claimRef: { apiVersion: "v1", kind: "PersistentVolumeClaim", namespace: "alasio", name: "rehearsal-other", uid: null, resourceVersion: null } } }));
      run("kubectl", ["--kubeconfig", kubeconfig, "apply", "-f", "-"], {
        input: JSON.stringify({
          apiVersion: "v1", kind: "PersistentVolumeClaim", metadata: { name: "rehearsal-other", namespace: "alasio" },
          spec: { volumeName: pv, storageClassName: volume.spec.storageClassName, accessModes: volume.spec.accessModes, resources: { requests: { storage: volume.spec.capacity.storage } } },
        }),
      });
      kubectl("-n", "alasio", "wait", "pvc/rehearsal-other", "--for=jsonpath={.status.phase}=Bound", "--timeout=120s");
    },
    mend: () => kubectl("-n", "alasio", "delete", "pvc", "rehearsal-other", "--wait=true"),
  },
  "stop-volume": {
    says: "lacks the volume",
    break: ({ dir }: Volume) => asRoot(`f=$(ls ${dir} | grep -E '^[a-z]+_[0-9]+\\.dat$' | head -n 1); mv "${dir}/$f" '${WORK}/missing.dat'; echo "$f" > '${WORK}/missing.name'`),
    mend: ({ dir }: Volume) => asRoot(`mv '${WORK}/missing.dat' "${dir}/$(cat '${WORK}/missing.name')"`),
  },
  "stop-maxid": {
    says: "the old master would make volume",
    break: ({ dir }: Volume) => asRoot(`: > '${dir}/9999.dat'`),
    mend: ({ dir }: Volume) => asRoot(`rm -f '${dir}/9999.dat'`),
  },
  "stop-listing": {
    says: "fewer versions than before the cut-over",
    break: ({ dir }: Volume) => asRoot(`mv '${dir}/filerldb2' '${WORK}/filerldb2.aside'`),
    // The old store, started on the metadata it made anew, stopped, its own put back, and started again.
    mend: ({ dir }: Volume) => {
      kubectl("-n", "alasio", "scale", "statefulset/alasio-seaweedfs", "--replicas=0");
      kubectl("-n", "alasio", "wait", "pod/alasio-seaweedfs-0", "--for=delete", "--timeout=300s");
      asRoot(`rm -rf '${dir}/filerldb2' && mv '${WORK}/filerldb2.aside' '${dir}/filerldb2'`);
      kubectl("-n", "alasio", "scale", "statefulset/alasio-seaweedfs", "--replicas=1");
    },
  },
} as const;

/** The volume a cut-over recorded: its PV and its directory on this machine. */
interface Volume {
  readonly pv: string;
  readonly dir: string;
}

async function stops(name: keyof typeof STOPS, tg: ReturnType<typeof operator>, kept: Kept, cutover: Readonly<Record<string, string>>): Promise<void> {
  const out = cutover["OUT"] ?? fail("the cut-over has no OUT");
  const stop = STOPS[name];
  runningJobs(`at-${name}`);
  say(`the cut-over, failing after step 6, then the store broken for ${name}`);
  const cut = script("cutover.sh", { ...cutover, FAIL_AT: "6" });
  if (cut.code === 0 || !cut.stderr.includes("a failure injected after step 6")) fail("the cut-over did not fail as injected after step 6");
  const [pv = "", dir = ""] = run("sudo", ["cat", join(out, "volume.tsv")], { env: process.env }).trim().split("\t");
  stop.break({ pv, dir });
  const rollback = { RELEASE: cutover["RELEASE"] ?? "", OUT: out, PREVIOUS: PREVIOUS(), PREVIOUS_CHECK: PREVIOUS_CHECK() };
  const stopped = script("rollback.sh", rollback);
  if (stopped.code !== 1 || !stopped.stderr.includes(stop.says)) fail(`the rollback exited ${stopped.code}, not 1 saying "${stop.says}"`);
  const writers = kubectl("-n", "alasio", "get", "deployment", "alasio", "-o", "jsonpath={.spec.replicas}");
  if (writers !== "0") fail(`the rollback stopped with alasio at ${writers} replicas, not none`);
  say(`the rollback stopped (${stop.says}), alasio still at none; put right, and run again`);
  stop.mend({ pv, dir });
  const back = script("rollback.sh", rollback);
  if (back.code !== 0) fail(`the rollback, run again once ${name} was put right, exited ${back.code}`);
  await check(tg, kept, previous);
  folderUntouched(kept);
  say(`rolled back once ${name} was put right`);
}

/**
 * A version read back whose bytes differ from what the old store read before: the cut-over,
 * stopped after its inventory, has the first digest it recorded made another, as a store or
 * a check reading wrongly would, and run again stops at its checks; the rollback, finding the
 * old store's answer differ from the recorded one too, records it, and brings the stack back
 * all the same.
 */
async function bytesDiffer(tg: ReturnType<typeof operator>, kept: Kept, cutover: Readonly<Record<string, string>>): Promise<void> {
  const out = cutover["OUT"] ?? fail("the cut-over has no OUT");
  runningJobs("at-bytes");
  say("the cut-over, stopped after its inventory, the first digest it recorded made another");
  const cut = script("cutover.sh", { ...cutover, FAIL_AT: "4" });
  if (cut.code === 0 || !cut.stderr.includes("a failure injected after step 4")) fail("the cut-over did not fail as injected after step 4");
  run("sudo", ["sed", "-i", `1s/[0-9a-f]\\{64\\}$/${"0".repeat(64)}/`, join(out, "contents-before.tsv")], { env: process.env });
  say("run again, it stops at its checks");
  const again = script("cutover.sh", cutover);
  if (again.code === 0 || !again.stderr.includes("hold other bytes than the old store's")) fail("the cut-over went on past a version read back as other bytes");
  if (recorded(out) !== "8") fail(`the cut-over recorded step ${recorded(out)}, not 8`);
  say("the rollback records the old store's differing answer, and brings the stack back");
  const back = script("rollback.sh", { RELEASE: cutover["RELEASE"] ?? "", OUT: out, PREVIOUS: PREVIOUS(), PREVIOUS_CHECK: PREVIOUS_CHECK() });
  if (back.code !== 3 || !back.stderr.includes("DIFFERS: versions read back hold other bytes than before the cut-over")) fail(`the rollback exited ${back.code}, not 3 with the bytes recorded as differing`);
  const differing = run("sudo", ["cat", join(out, "discrepancies.txt")], { env: process.env }).trim().split("\n");
  if (differing.length !== 2 || !differing[0]?.startsWith("# the rollback of ")) fail(`the rollback recorded ${differing.length - 1} discrepancies, not the one: ${differing.join("; ")}`);
  if (kubectl("-n", "alasio", "get", "cronjob", "alasio-seaweedfs-lifecycle", "-o", "jsonpath={.spec.suspend}") !== "true") fail("the old store's lifecycle CronJob runs, with what differs not looked into");
  kubectl("-n", "alasio", "patch", "cronjob", "alasio-seaweedfs-lifecycle", "-p", JSON.stringify({ spec: { suspend: false } }));
  await check(tg, kept, previous);
  folderUntouched(kept);
  say("rolled back from step 8, the discrepancy recorded");
}

async function main(): Promise<void> {
  const [mode, ...steps] = process.argv.slice(2);
  if (mode !== "forward" && mode !== "rollback") fail("usage: rehearse.ts forward | rollback <step>...");
  const { tg, kept } = await setUp();
  const settings = { MIN_FREE_GB: "5", READ_BACK: "100" };

  if (mode === "rollback") {
    for (const step of steps) {
      const out = join(WORK, `cutover-${step}`);
      mkdirSync(out, { recursive: true });
      const { release: releaseFile, csi } = render(out);
      if (step === "bytes") {
        await bytesDiffer(tg, kept, { RELEASE: releaseFile, CSI_NAMESPACE: csi, OUT: out, ...settings });
        continue;
      }
      if (step in STOPS) {
        await stops(step as keyof typeof STOPS, tg, kept, { RELEASE: releaseFile, CSI_NAMESPACE: csi, OUT: out, ...settings });
        continue;
      }
      const cutover = { RELEASE: releaseFile, CSI_NAMESPACE: csi, OUT: out, FAIL_AT: step, ...settings };
      // The step a failure after step `step` leaves recorded: 5a and 6a are within 5 and 6.
      const within = step.replace(/[ab]$/u, () => "");
      runningJobs(`at-${step}`);
      say(`the cut-over, a failure injected after step ${step}`);
      const cut = script("cutover.sh", cutover);
      if (cut.code === 0 || !cut.stderr.includes(`a failure injected after step ${step}`)) fail(`the cut-over did not fail as injected after step ${step}`);
      say(`run again, going on from step ${within}, and failing there again`);
      const again = script("cutover.sh", cutover);
      if (again.code === 0 || !again.stderr.includes(`a failure injected after step ${step}`)) fail(`the cut-over run again did not fail as injected after step ${step}`);
      if (recorded(out) !== within) fail(`the cut-over run again recorded step ${recorded(out)}, not ${within}`);
      if (Number(within) >= 2) folderUntouched(kept);
      if (Number(within) >= 2 && Number(within) <= 5) sealed();
      say(`rolled back from step ${step}`);
      if (step === "6") {
        say("a rollback that fails as it applies the previous release, which the cut-over then refuses to go on from, run again");
        const broken = script("rollback.sh", { RELEASE: releaseFile, OUT: out, PREVIOUS: asUser("previous-failing", "exit 1"), PREVIOUS_CHECK: PREVIOUS_CHECK(), READ_BACK: "100" });
        if (broken.code === 0 || !broken.stderr.includes("the previous release was not applied again")) fail("the rollback did not fail with its previous release");
        const refused = script("cutover.sh", cutover);
        if (refused.code === 0 || !refused.stderr.includes("a rollback of the cut-over recorded")) fail("the cut-over went on from an OUT a rollback began on");
      }
      const back = script("rollback.sh", { RELEASE: releaseFile, OUT: out, PREVIOUS: PREVIOUS(), PREVIOUS_CHECK: PREVIOUS_CHECK(), READ_BACK: "100" });
      if (back.code !== 0) fail(`the rollback from step ${step} failed`);
      const twice = script("cutover.sh", { RELEASE: releaseFile, CSI_NAMESPACE: csi, OUT: out, ...settings });
      if (twice.code === 0 || !twice.stderr.includes("was rolled back")) fail("the cut-over took an OUT that was rolled back");
      await check(tg, kept, previous);
      folderUntouched(kept);
    }
    say(`rehearsed: the cut-over failed after steps ${steps.join(", ")}, each run again and rolled back`);
    return;
  }

  say("a plain upgrade, without the cut-over, refuses to start the parts on an empty store");
  const plain = attempt("sh", [UPGRADE()], process.env);
  if (plain.code === 0 || !plain.stderr.includes("keeps its data on the volume of its one pod")) fail("the release's upgrade did not refuse to run without the cut-over");
  run("sh", ["-c", configured("previous", INSTALL)]);
  await check(tg, kept, previous);

  const out = join(WORK, "cutover");
  mkdirSync(out, { recursive: true });
  const { release: releaseFile, csi } = render(out);
  const cutover = { RELEASE: releaseFile, CSI_NAMESPACE: csi, OUT: out, ...settings };
  say("a release's command line that does not run is refused before anything changes");
  const unrunnable = script("cutover.sh", { ...cutover, UPGRADE: UPGRADE(), UPGRADE_CHECK: asUser("upgrade-check-failing", "exit 1") });
  if (unrunnable.code === 0 || !unrunnable.stderr.includes("does not run as the configuration's owner")) fail("the cut-over did not refuse an unrunnable release");
  if (spawnSync("sudo", ["test", "-e", join(out, "step")]).status === 0) fail("the cut-over refused late: it recorded a step");
  say("an OUT another run holds is refused");
  // --close: flock holds the lock itself, not the sleep it runs, so ending flock frees it.
  const holder = spawn("sudo", ["flock", "--close", join(out, "lock"), "sleep", "600"], { stdio: "ignore" });
  await sleep(2000);
  const held = script("cutover.sh", cutover);
  holder.kill();
  spawnSync("sudo", ["pkill", "-f", `flock --close ${join(out, "lock")}`]);
  await until("the lock to be free again", () => spawnSync("sudo", ["flock", "--nonblock", join(out, "lock"), "true"]).status === 0, 30_000);
  if (held.code === 0 || !held.stderr.includes("another cut-over or rollback holds")) fail("the cut-over went on in an OUT another run holds");
  runningJobs("forward");
  for (const step of ["1", "2", "3", "4", "5a", "5", "6a", "6b", "6", "7", "8"]) {
    say(`the cut-over, run again each time, a failure injected after step ${step}`);
    const cut = script("cutover.sh", { ...cutover, FAIL_AT: step });
    if (cut.code === 0 || !cut.stderr.includes(`a failure injected after step ${step}`)) fail(`the cut-over did not fail as injected after step ${step}`);
    if (recorded(out) !== step.replace(/[ab]$/u, () => "")) fail(`the cut-over recorded step ${recorded(out)} after a failure after step ${step}`);
    if (step !== "1") folderUntouched(kept);
  }
  say("the upgrade fails partway, alasio not running within 5 s");
  const failed = script("cutover.sh", { ...cutover, UPGRADE: UPGRADE("5s"), UPGRADE_CHECK: UPGRADE_CHECK() });
  if (failed.code === 0 || !failed.stderr.includes("the upgrade failed")) fail("the cut-over did not fail with its upgrade");
  say("the rollback, past the point of no return, refuses and says to roll forward");
  const back = script("rollback.sh", { RELEASE: releaseFile, OUT: out, PREVIOUS: PREVIOUS(), PREVIOUS_CHECK: PREVIOUS_CHECK() });
  if (back.code === 0 || !back.stderr.includes("roll it forward")) fail("the rollback did not refuse past the point of no return");
  say("the cut-over run again, going on with the upgrade, a failure injected once it has run");
  const upgraded = script("cutover.sh", { ...cutover, UPGRADE: UPGRADE(), UPGRADE_CHECK: UPGRADE_CHECK(), FAIL_AT: "9" });
  if (upgraded.code === 0 || !upgraded.stderr.includes("a failure injected after step 9")) fail("the cut-over did not go on with the upgrade");
  say("run again once more, going on with the upgrade, which changes nothing now");
  const done = script("cutover.sh", { ...cutover, UPGRADE: UPGRADE(), UPGRADE_CHECK: UPGRADE_CHECK() });
  if (done.code !== 0) fail("the cut-over run again past its upgrade failed");
  await check(tg, kept, release);
  for (const cronJob of ["alasio-seaweedfs-lifecycle", "alasio-seaweedfs-filer-store-backup", "alasio-neon-backup"]) {
    if (kubectl("-n", "alasio", "get", "cronjob", cronJob, "-o", "jsonpath={.spec.suspend}") === "true") fail(`the CronJob ${cronJob} is left suspended`);
  }
  say(`the release's store: ${release("status").split("\n").filter((line) => /object store|seaweedfs/u.test(line)).join("; ")}`);
  say("rehearsed: the cut-over failed after each step and run again, its upgrade failed and run again, rolled forward");
}

await main().then(
  () => {
    for (const child of forwards) child.kill();
  },
  (error: unknown) => {
    for (const child of forwards) child.kill();
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  },
);
