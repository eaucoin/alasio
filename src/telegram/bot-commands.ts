/**
 * The bot's commands, as Telegram's menu offers them: set by alasio's command line as it
 * registers each environment's bot (cli/src/webhook.ts), not by alasio as it starts, which
 * would ask Telegram the same of every start, and an environment that sleeps starts often.
 * Free of anything but itself, so the command line takes it without the server.
 */

/** A command of the bot's menu: its name and what the menu says of it. */
export interface BotCommand {
  readonly command: string;
  readonly description: string;
}

/** The bot's commands, in the menu's order. */
export const BOT_COMMANDS: readonly BotCommand[] = [
  { command: "service", description: "Switch between Codex and Claude" },
  { command: "model", description: "Choose the model and effort" },
  { command: "workspace", description: "Choose, create or fork the workspace to work in" },
  { command: "session", description: "Manage the mounted agent session" },
  { command: "sessions", description: "Browse and mount agent sessions" },
  { command: "goal", description: "View or set the mounted session goal (Codex)" },
  { command: "stop", description: "Interrupt the active turn" },
];
