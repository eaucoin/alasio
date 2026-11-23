#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  __commonJS,
  __require
} from "./main-c0bag0nq.js";

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/machine-id/execAsync.js
var require_execAsync = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.execAsync = undefined;
  var child_process = __require("child_process");
  var util = __require("util");
  exports.execAsync = util.promisify(child_process.exec);
});

export { require_execAsync };
