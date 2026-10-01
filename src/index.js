// alasio's entry point. Telemetry starts before the service's modules load, so the
// modules it instruments (pg, http) are patched as the service imports them; a static
// import would have them loaded first.
import "dotenv/config";
import { startTelemetry } from "./telemetry/start.js";

await startTelemetry();
await import("./main.js");
