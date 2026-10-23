// `alasio codex-login`'s entry point (./relay.ts), run from alasio's image. Telemetry starts
// before the relay's modules load, as alasio's does (../index.ts), so the modules it
// instruments (pg, http) are patched as they are imported.
import { startTelemetry } from "../telemetry/start.ts";

await startTelemetry();
const [{ runAlasio }, { relay }] = await Promise.all([import("../shared/run.ts"), import("./relay.ts")]);
runAlasio(relay);
