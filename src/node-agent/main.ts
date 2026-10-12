// The node agent's entry point (./server.ts nodeAgent), run from alasio's image on every
// node sessions run on. Telemetry starts before the agent's modules load, as alasio's
// does (../index.ts), so the modules it instruments (http) are patched as they are
// imported.
import { startTelemetry } from "../telemetry/start.ts";

await startTelemetry();
const [{ runAlasio }, { nodeAgent }] = await Promise.all([import("../shared/run.ts"), import("./server.ts")]);
runAlasio(nodeAgent);
