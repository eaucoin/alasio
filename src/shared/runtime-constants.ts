/**
 * Runtime constants shared by alasio source modules.
 */
/** Sessions shown per page in !sessions and !rewind commands */
export const SESSIONS_PER_PAGE = 5;
/** Default HTTP server port for receiving hook notifications from Codex; ALASIO_HOOK_PORT overrides it */
export const HOOK_SERVER_PORT = 8765;

export function resolveHookPort(env: NodeJS.ProcessEnv = process.env): number {
  const raw = Number.parseInt(env["ALASIO_HOOK_PORT"] ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : HOOK_SERVER_PORT;
}
