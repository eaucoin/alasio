#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  require_execAsync
} from "./main-nyqxb2d3.js";
import {
  __commonJS,
  __require,
  require_src
} from "./main-qc1nvyfk.js";

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/machine-id/getMachineId-bsd.js
var require_getMachineId_bsd = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getMachineId = undefined;
  var fs_1 = __require("fs");
  var execAsync_1 = require_execAsync();
  var api_1 = require_src();
  async function getMachineId() {
    try {
      const result = await fs_1.promises.readFile("/etc/hostid", { encoding: "utf8" });
      return result.trim();
    } catch (e) {
      api_1.diag.debug(`error reading machine id: ${e}`);
    }
    try {
      const result = await (0, execAsync_1.execAsync)("kenv -q smbios.system.uuid");
      return result.stdout.trim();
    } catch (e) {
      api_1.diag.debug(`error reading machine id: ${e}`);
    }
    return;
  }
  exports.getMachineId = getMachineId;
});
export default require_getMachineId_bsd();
