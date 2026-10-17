# alasio's image with the end-to-end run's stand-ins for its harnesses in it, Codex's
# (./codex-stand-in.ts) and Claude Code's (./claude-stand-in.ts), which the run's install
# has alasio run as Codex and as Claude Code (test/e2e/harness.ts). Built from test/, on
# the alasio image the run built: --build-arg ALASIO=<that image>. The stand-ins are where
# alasio's own tests are in the repository, so they import what they do, and alasio's
# packages, as they do there.
ARG ALASIO
FROM ${ALASIO}
COPY e2e/codex-stand-in.ts e2e/claude-stand-in.ts /opt/alasio/test/e2e/
COPY support/codex-protocol.ts /opt/alasio/test/support/
