/**
 * What alasio tells an agent working in a session filesystem, on top of the reply
 * instructions: its workspace is not where its harness runs. The harness (Claude Code,
 * Codex) runs in an empty directory of its own, and the workspace is `/workspace` in an
 * isolated sandbox that the agent reaches only through bayma.
 *
 * And what to tell any agent with bayma, a session filesystem's or a folder's, of a call bayma
 * refuses as its session's control is another's (BAYMA_CONTROL_INSTRUCTIONS): a session saved
 * and restored, as a sandbox's is where its pod is lost mid-call, or a folder instance's as it
 * is made anew once released, keeps the connection that last held it its controller, and the
 * harness's MCP client, whose connection is a new one, is refused until it takes control over
 * (session.acquire_controller), which bayma's refusal does not say. A call cut short as it was
 * saved runs again on the state from before it, as it never finished: run again, not twice.
 */
import { REPLY_INSTRUCTIONS } from "./reply-instructions.ts";

/** What an agent with bayma is told of a call bayma refuses as its session's control is another's, as the module says. */
export const BAYMA_CONTROL_INSTRUCTIONS = [
  "Where bayma refuses a call with \"write action requires controller lease\", bayma was lost or started anew since",
  "your last call, and its session came back controlled by the connection that last held it: take control with",
  "session.acquire_controller, then call again, taking it again if refused again, as your connection to",
  "bayma may be made anew more than once as it reconnects. A call that was cut short did not finish, so check what",
  "it left before you run it again.",
].join(" ");

export const SESSION_FS_INSTRUCTIONS = [
  "Your workspace is /workspace on an isolated machine that you reach only through the bayma tools:",
  "run code and shell commands, and read and write files, through them, in /workspace and your home there.",
  "The directory your harness reports as its working directory is its own, holds nothing of yours, and is",
  "not on that machine; paths you use and show, including media paths, are paths on that machine,",
  "relative ones resolved against /workspace.",
  BAYMA_CONTROL_INSTRUCTIONS,
].join(" ");

/** Everything alasio tells an agent in a session filesystem. */
export const SESSION_FS_AGENT_INSTRUCTIONS = `${REPLY_INSTRUCTIONS}\n\n${SESSION_FS_INSTRUCTIONS}`;

/** Everything alasio tells an agent in a folder workspace with its bayma, on top of the operator's own. */
export const FOLDER_BAYMA_AGENT_INSTRUCTIONS = `${REPLY_INSTRUCTIONS}\n\n${BAYMA_CONTROL_INSTRUCTIONS}`;
