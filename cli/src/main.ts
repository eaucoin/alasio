#!/usr/bin/env node
/**
 * alasio's command line: `npx alasio <command>`, which installs alasio, runs it, and
 * looks after it, on a cluster it makes on this machine or one a kubeconfig reaches.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { Command } from "effect/cli";

import { alasio } from "./commands.ts";
import { VERSION } from "./release.ts";

Command.run(alasio, { version: VERSION }).pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain);
