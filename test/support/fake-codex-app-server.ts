/**
 * A stand-in for the Codex app-server process, which alasio spawns in place of the codex
 * binary (ALASIO_CODEX_BIN, through the wrapper script test/support/codex.ts writes).
 * It holds no logic of its own: it connects to the test over a Unix socket, reports
 * how it was started, and relays each line alasio writes to its stdin to the test and
 * each line the test sends back to its stdout, so the test scripts exactly what Codex
 * does. It exits when the test tells it to, and when alasio or the test lets go of it.
 *
 *   node fake-codex-app-server.ts <socket> app-server <arguments...>
 */
import { connect } from "node:net";
import { createInterface } from "node:readline";

/** What the stand-in tells the test: how it was started, then each line alasio wrote. */
export type FakeCodexReport =
  | { readonly spawned: { readonly argv: readonly string[]; readonly cwd: string; readonly pid: number } }
  | { readonly stdin: string };

/** What the test tells the stand-in: write a line for alasio to read, or exit. */
export type FakeCodexCommand =
  | { readonly stdout: string }
  | { readonly exit: number };

if (import.meta.main) {
  const [socketPath, ...argv] = process.argv.slice(2);
  if (socketPath === undefined) throw new Error("usage: fake-codex-app-server.ts <socket> <arguments...>");
  const socket = connect(socketPath);
  const report = (frame: FakeCodexReport) => socket.write(`${JSON.stringify(frame)}\n`);
  report({ spawned: { argv, cwd: process.cwd(), pid: process.pid } });
  createInterface({ input: process.stdin, crlfDelay: Infinity }).on("line", (line) => report({ stdin: line }));
  createInterface({ input: socket, crlfDelay: Infinity }).on("line", (line) => {
    // The test writes these commands, one per line.
    const command = JSON.parse(line) as FakeCodexCommand;
    if ("exit" in command) process.exit(command.exit);
    process.stdout.write(`${command.stdout}\n`);
  });
  socket.on("close", () => process.exit(0));
  process.stdin.on("end", () => process.exit(0));
}
