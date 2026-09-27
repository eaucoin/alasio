/**
 * pgrag's model files, served where Neon's compute image fetches them.
 *
 * Neon builds pgrag's embedding and reranking extensions to download their
 * ONNX models on first use from a host inside Neon's own cluster. On the
 * stack's private network this service answers to that host's name (see
 * compose.yml) with the files src/neon/models.js keeps under
 * <state>/neon/models: GET or HEAD /pgrag-data/<file>, read-only. Its health
 * does not depend on them: without them, only embedding and reranking wait.
 */
import { createReadStream, statSync } from "node:fs";
import { createServer } from "node:http";

const MODELS = "/models";
const FILES = new Set(["bge_small_en_v15.onnx", "jina_reranker_v1_tiny_en.onnx"]);
const PREFIX = "/pgrag-data/";

const server = createServer((request, response) => {
  const { pathname } = new URL(request.url, "http://models");
  if (pathname === "/healthz") {
    response.writeHead(200).end();
    return;
  }
  const name = pathname.startsWith(PREFIX) ? pathname.slice(PREFIX.length) : null;
  if ((request.method !== "GET" && request.method !== "HEAD") || !FILES.has(name)) {
    response.writeHead(404).end();
    return;
  }
  const path = `${MODELS}/${name}`;
  let size;
  try {
    size = statSync(path).size;
  } catch {
    response.writeHead(404).end();
    return;
  }
  response.writeHead(200, { "content-type": "application/octet-stream", "content-length": size });
  if (request.method === "HEAD") response.end();
  else createReadStream(path).pipe(response);
});

server.listen(80);
process.on("SIGTERM", () => server.close(() => process.exit(0)));
