// The sandbox gateway's entry point (./gateway.ts gateway), run from alasio's image on the
// floor. Telemetry starts before the gateway's modules load, as alasio's does (../index.ts),
// so the modules it instruments (http) are patched as they are imported.
import { startTelemetry } from "../telemetry/start.ts";

await startTelemetry();
const [{ runAlasio }, { gateway }] = await Promise.all([import("../shared/run.ts"), import("./gateway.ts")]);
runAlasio(gateway);
