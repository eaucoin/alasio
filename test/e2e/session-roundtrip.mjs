/**
 * Run inside alasio's pod (kubectl exec deploy/alasio -- node <this> <volumeId>): drives one
 * session through alasio's own Kubernetes driver and its ServiceAccount's permissions.
 * bayma answers over MCP with the session's token, writes a file in the workspace, which
 * alasio reads back through exec; the session is suspended, resumed, and the file is
 * still there. Prints one JSON line of what it saw.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

import { loadKubeTemplates } from "/opt/alasio/src/kube/config.js";
import { createKubeClient } from "/opt/alasio/src/kube/client.js";
import { createSandboxes } from "/opt/alasio/src/kube/sandboxes.js";
import { createKubernetesSandbox } from "/opt/alasio/src/sandbox/kubernetes/index.js";

const volumeId = process.argv[2];
const templates = loadKubeTemplates();
const kube = createKubeClient();
const sandbox = createKubernetesSandbox({ templates, stateDir: "/tmp/roundtrip", kube, env: {}, createForwarder: async () => null });
const seen = {};

async function exec(bayma, code) {
  const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
  await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }));
  try {
    const created = await client.callTool({ name: "session.create", arguments: { runtime: "bun", title: "e2e", cwd: "/workspace" } });
    const sessionId = JSON.parse(created.content[0].text).session.session_id;
    const result = await client.callTool({ name: "exec", arguments: { session_id: sessionId, code } });
    return result.structuredContent ?? JSON.parse(result.content.find((part) => part.type === "text").text);
  } finally {
    await client.close();
  }
}

let { bayma } = await sandbox.ensureSession(volumeId);
const wrote = await exec(bayma, 'await Bun.write("/workspace/out/hello.txt", "hello from " + require("os").release()); "written"');
seen.exec = wrote.result_text || JSON.stringify(wrote).slice(0, 300);
seen.read = (await sandbox.readFile(volumeId, "out/hello.txt", 1024)).bytes?.toString("utf8");
seen.missing = (await sandbox.readFile(volumeId, "out/nope.txt", 1024)).note;
seen.outside = (await sandbox.readFile(volumeId, "/etc/passwd", 4096)).bytes ? "read /etc/passwd of the sandbox, not the host" : "refused";

const sandboxes = createSandboxes({ kube, namespace: templates.sessions.namespace, port: templates.sessions.port });
await sandboxes.suspend(volumeId);
for (let i = 0; i < 60; i++) {
  if (!(await kube.read("v1", "Pod", templates.sessions.namespace, volumeId))) break;
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
seen.suspendedPod = (await kube.read("v1", "Pod", templates.sessions.namespace, volumeId)) ? "still there" : "gone";
seen.whileSuspended = (await sandbox.readFile(volumeId, "out/hello.txt", 1024)).note;
const resumedAt = Date.now();
({ bayma } = await sandbox.ensureSession(volumeId));
seen.resumeMs = Date.now() - resumedAt;
seen.afterResume = (await sandbox.readFile(volumeId, "out/hello.txt", 1024)).bytes?.toString("utf8");
const again = await exec(bayma, 'require("fs").readFileSync("/workspace/out/hello.txt", "utf8")');
seen.execAfterResume = again.result_text;
console.log(JSON.stringify(seen));
await sandbox.close();
