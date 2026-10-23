/**
 * What every Codex alasio runs is run without: plugins, and apps, the login's ChatGPT
 * connectors, which would reach services past what alasio gives a turn (its workspace and
 * its bayma), as Claude Code's claude.ai connectors are turned off
 * (../harness/claude/env.ts CLAUDEAI_CONNECTORS). The app-server is started with each
 * disabled (./app-server/process.ts).
 */
export const CODEX_DISABLED_FEATURES = ["plugins", "apps"] as const;
