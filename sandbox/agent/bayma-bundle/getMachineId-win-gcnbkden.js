#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  require_execAsync
} from "./main-vw7d0zyb.js";
import {
  __commonJS,
  __require,
  require_src
} from "./main-4vabrjre.js";

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/machine-id/getMachineId-win.js
var require_getMachineId_win = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getMachineId = undefined;
  var process = __require("process");
  var execAsync_1 = require_execAsync();
  var api_1 = require_src();
  async function getMachineId() {
    const args = "QUERY HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography /v MachineGuid";
    let command = "%windir%\\System32\\REG.exe";
    if (process.arch === "ia32" && "PROCESSOR_ARCHITEW6432" in process.env) {
      command = "%windir%\\sysnative\\cmd.exe /c " + command;
    }
    try {
      const result = await (0, execAsync_1.execAsync)(`${command} ${args}`);
      const parts = result.stdout.split("REG_SZ");
      if (parts.length === 2) {
        return parts[1].trim();
      }
    } catch (e) {
      api_1.diag.debug(`error reading machine id: ${e}`);
    }
    return;
  }
  exports.getMachineId = getMachineId;
});
export default require_getMachineId_win();
