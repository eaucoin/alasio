// The session telemetry receiver's entry point (./receiver.ts receiver), run from alasio's
// image on the floor. Telemetry starts before the receiver's modules load, as alasio's does
// (../index.ts), so the modules it instruments (http) are patched as they are imported.
import { startTelemetry } from "../telemetry/start.ts";

await startTelemetry();
const [{ runAlasio }, { receiver }] = await Promise.all([import("../shared/run.ts"), import("./receiver.ts")]);
runAlasio(receiver);
