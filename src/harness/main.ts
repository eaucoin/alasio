// `alasio harness`'s entry point (./program.ts harness), run from alasio's image. Telemetry
// starts before the host's modules load, as alasio's does (../index.ts), so the modules it
// instruments (pg, http) are patched as they are imported.
import { startTelemetry } from "../telemetry/start.ts";

await startTelemetry();
const [{ runAlasio }, { harness }] = await Promise.all([import("../shared/run.ts"), import("./program.ts")]);
runAlasio(harness);
