# alasio's image with the end-to-end run's stand-in for Codex (./codex-stand-in.ts) in it,
# which the run's install has alasio run as Codex (test/e2e/harness.ts). Built from test/,
# on the alasio image the run built: --build-arg ALASIO=<that image>. The stand-in is
# where alasio's own tests are in the repository, so it imports what they do, and alasio's
# packages, as it does there.
ARG ALASIO
FROM ${ALASIO}
COPY e2e/codex-stand-in.ts /opt/alasio/test/e2e/
COPY support/codex-protocol.ts /opt/alasio/test/support/
