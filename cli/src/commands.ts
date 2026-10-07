/** The `alasio` command and its subcommands, each in a module of its own (./commands/). */
import { Command } from "effect/cli";

import { asRoot } from "./commands/as-root.ts";
import { branch } from "./commands/branch.ts";
import { down } from "./commands/down.ts";
import { grafana } from "./commands/grafana.ts";
import { init } from "./commands/init.ts";
import { lake } from "./commands/lake.ts";
import { login } from "./commands/login.ts";
import { logs } from "./commands/logs.ts";
import { restart } from "./commands/restart.ts";
import { status } from "./commands/status.ts";
import { uninstall } from "./commands/uninstall.ts";
import { up } from "./commands/up.ts";
import { upgrade } from "./commands/upgrade.ts";
import { ConfigFlag } from "./config.ts";

export const alasio = Command.make("alasio").pipe(
  Command.withDescription(
    "Coding agents you talk to from Telegram: Claude Code and Codex, each conversation with a workspace of its own. " +
      "alasio init sets it up, alasio up starts it, in a cluster alasio makes on this machine or one a kubeconfig reaches.",
  ),
  Command.withSubcommands([init, up, status, logs, restart, upgrade, login, lake, grafana, branch, down, uninstall, asRoot]),
  Command.withGlobalFlags([ConfigFlag]),
);
