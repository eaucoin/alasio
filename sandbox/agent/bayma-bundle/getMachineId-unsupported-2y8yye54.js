#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  __commonJS,
  require_src
} from "./main-qc1nvyfk.js";

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/machine-id/getMachineId-unsupported.js
var require_getMachineId_unsupported = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getMachineId = undefined;
  var api_1 = require_src();
  async function getMachineId() {
    api_1.diag.debug("could not read machine-id: unsupported platform");
    return;
  }
  exports.getMachineId = getMachineId;
});
export default require_getMachineId_unsupported();
