/**
 * Run inside alasio's pod (kubectl exec deploy/alasio -- node <this>): a folder
 * conversation's bayma through alasio's own code, as the host profile makes it, with a
 * file written through it to the operator's home. Prints one JSON line of what it saw.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

import { folderBaymaServer } from "/opt/alasio/src/mcp/bayma.js";

const bayma = await folderBaymaServer({ harness: "claude", threadKey: "e2e:folder" });
const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }));
try {
  const created = await client.callTool({ name: "session.create", arguments: { runtime: "bun", title: "e2e", cwd: process.env.HOME } });
  const sessionId = (created.structuredContent ?? JSON.parse(created.content[0].text)).session.session_id;
  const code = 'await Bun.write(process.env.HOME + "/e2e-folder-proof", "written by " + process.getuid()); JSON.stringify({ uid: process.getuid(), home: process.env.HOME })';
  const result = await client.callTool({ name: "exec", arguments: { session_id: sessionId, code } });
  const done = result.structuredContent ?? JSON.parse(result.content.find((part) => part.type === "text").text);
  console.log(JSON.stringify({ url: bayma.url, seen: JSON.parse(JSON.parse(done.result_text)) }));
} finally {
  await client.close();
}
