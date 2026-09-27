/**
 * pgrag's model files, which Neon's compute image fetches on first use from a
 * host inside Neon's own cluster, and which the stack's pgrag-models service
 * serves in its place (neon/models/serve.js).
 *
 * They come from the release of pgrag that Neon's compute image is built
 * from, checked against the checksum Neon's build pins, and each model
 * against its own. They are kept under <state>/neon/models, fetched once.
 */
import { createHash } from "node:crypto";
import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";

/** pgrag v0.1.2, as neon's compute/compute-node.Dockerfile fetches and checks it. */
export const PGRAG_RELEASE = {
  url: "https://github.com/neondatabase-labs/pgrag/archive/refs/tags/v0.1.2.tar.gz",
  sha256: "7361654ea24f08cbb9db13c2ee1c0fe008f6114076401bb871619690dafc5225",
};

/** Each model: the file served, the archive in the release holding it, and its digest. */
export const PGRAG_MODELS = [
  {
    file: "bge_small_en_v15.onnx",
    archive: "pgrag-0.1.2/lib/bge_small_en_v15/model.onnx.tar.gz",
    sha256: "828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35",
  },
  {
    file: "jina_reranker_v1_tiny_en.onnx",
    archive: "pgrag-0.1.2/lib/jina_reranker_v1_tiny_en/model.onnx.tar.gz",
    sha256: "e0e743251c7566e2b1e4f5ad091c681a700d7d7a3d85541ea56ca3acf43d1afa",
  },
];

const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");

/** A regular file's bytes in a tar archive, found by name. */
export function tarMember(tar, name) {
  for (let offset = 0; offset + 512 <= tar.length; ) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) break;
    const field = (start, length) => header.subarray(start, start + length).toString("latin1").replace(/\0.*$/su, "");
    const prefix = field(345, 155);
    const path = prefix ? `${prefix}/${field(0, 100)}` : field(0, 100);
    const size = Number.parseInt(field(124, 12).trim() || "0", 8);
    const type = field(156, 1);
    const data = offset + 512;
    if (path === name && (type === "0" || type === "")) return tar.subarray(data, data + size);
    offset = data + Math.ceil(size / 512) * 512;
  }
  throw new Error(`${name} is not in the archive`);
}

function present(dir, model) {
  const path = join(dir, model.file);
  return existsSync(path) && sha256(readFileSync(path)) === model.sha256;
}

/**
 * Makes sure every model is in `dir`, fetching pgrag's release for any that
 * is missing or not what it should be. Throws if they cannot be had.
 */
export async function ensurePgragModels(dir, { fetchRelease = defaultFetch } = {}) {
  const missing = PGRAG_MODELS.filter((model) => !present(dir, model));
  if (missing.length === 0) return false;
  const release = await fetchRelease(PGRAG_RELEASE.url);
  if (sha256(release) !== PGRAG_RELEASE.sha256) {
    throw new Error(`pgrag's release from ${PGRAG_RELEASE.url} is not the one Neon's compute is built from`);
  }
  const tar = gunzipSync(release);
  for (const model of missing) {
    const bytes = tarMember(gunzipSync(tarMember(tar, model.archive)), "model.onnx");
    if (sha256(bytes) !== model.sha256) throw new Error(`${model.file} in pgrag's release has an unexpected digest`);
    const path = join(dir, model.file);
    writeFileSync(`${path}.partial`, bytes, { mode: 0o644 });
    renameSync(`${path}.partial`, path);
  }
  return true;
}

async function defaultFetch(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url} answered HTTP ${response.status}`);
  return Buffer.from(await response.arrayBuffer());
}
