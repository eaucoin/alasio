/**
 * What alasio tells an agent working in a session filesystem, on top of the reply
 * instructions: its workspace is not where its harness runs. The harness (Claude Code,
 * Codex) runs in alasio, in an empty directory of its own, and the workspace is
 * `/workspace` in an isolated sandbox that the agent reaches only through bayma.
 */
import { REPLY_INSTRUCTIONS } from "./reply-instructions.ts";

export const SESSION_FS_INSTRUCTIONS = [
  "Your workspace is /workspace on an isolated machine that you reach only through the bayma tools:",
  "run code and shell commands, and read and write files, through them, in /workspace and your home there.",
  "The directory your harness reports as its working directory is its own, holds nothing of yours, and is",
  "not on that machine; paths you use and show, including media paths, are paths on that machine,",
  "relative ones resolved against /workspace.",
].join(" ");

/** Everything alasio tells an agent in a session filesystem. */
export const SESSION_FS_AGENT_INSTRUCTIONS = `${REPLY_INSTRUCTIONS}\n\n${SESSION_FS_INSTRUCTIONS}`;
