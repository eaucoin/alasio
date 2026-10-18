/** `alasio folder`: of the folder conversations' instances, the sandbox gateway's (../folders.ts). */
import { Console, Effect } from "effect";
import { Argument, Command } from "effect/cli";

import { restartFolder } from "../folders.ts";
import { onCluster } from "./common.ts";

const conversation = Argument.String("conversation").pipe(Argument.withDescription("The conversation, as alasio names it, such as telegram:42"));

const restart = Command.make("restart", { conversation }, ({ conversation }) =>
  onCluster(() =>
    Effect.gen(function*() {
      for (const line of yield* restartFolder(conversation)) yield* Console.log(line);
    })
  )).pipe(
    Command.withShortDescription("Restart a folder conversation's bayma"),
    Command.withDescription(
      "Lets the folder instances of the conversation go, Claude Code's and Codex's: each pod's bayma stops, saving its REPL sessions " +
        "whole into the conversation's state directory, and the conversation's next message makes it anew, which restores them. " +
        "A folder whose binds failed three times in a row is bound again then, as Retry now binds it from Telegram.",
    ),
  );

export const folder = Command.make("folder").pipe(
  Command.withShortDescription("Of the folder conversations' bayma"),
  Command.withSubcommands([restart]),
);
