/**
 * What alasio tells an agent working in a session filesystem, on top of the reply
 * instructions: its workspace is not where its harness runs. The harness (Claude Code,
 * Codex) runs in an empty directory of its own, and the workspace is `/workspace` in an
 * isolated sandbox that the agent reaches only through bayma.
 *
 * And what to do where bayma refuses a call as its session's control is another's: a
 * snapshot taken with a call open, as one is where the sandbox's pod is lost mid-call, keeps
 * that call's connection the session's controller, and the harness's MCP client, reconnected,
 * is refused until it takes control over (session.acquire_controller), which bayma's refusal
 * does not say. A call it sends again after that runs on the state from before the call, as
 * the lost one never finished: run again, not twice.
 */
import { REPLY_INSTRUCTIONS } from "./reply-instructions.ts";

export const SESSION_FS_INSTRUCTIONS = [
  "Your workspace is /workspace on an isolated machine that you reach only through the bayma tools:",
  "run code and shell commands, and read and write files, through them, in /workspace and your home there.",
  "The directory your harness reports as its working directory is its own, holds nothing of yours, and is",
  "not on that machine; paths you use and show, including media paths, are paths on that machine,",
  "relative ones resolved against /workspace.",
  "Where bayma refuses a call with \"write action requires controller lease\", the machine was lost during",
  "an earlier call, and its session came back controlled by that call's connection: take control with",
  "session.acquire_controller, then call again, taking it again if refused again, as your connection to",
  "bayma may be made anew more than once as it reconnects. The call that was cut short did not finish, so check what",
  "it left before you run it again.",
].join(" ");

/** Everything alasio tells an agent in a session filesystem. */
export const SESSION_FS_AGENT_INSTRUCTIONS = `${REPLY_INSTRUCTIONS}\n\n${SESSION_FS_INSTRUCTIONS}`;
