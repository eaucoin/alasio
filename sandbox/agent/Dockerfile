# The image session filesystems run: bayma's, with every runtime bayma serves, run as
# the unprivileged user `agent`. No harness runs here: Claude Code and Codex run in alasio
# and reach a session only through its bayma. /workspace and /home/agent are the
# session's volume, mounted by its Sandbox (cli/src/manifests/alasio.ts, sessionsProfile).
ARG BAYMA_IMAGE=ghcr.io/eaucoin/bayma:0.11.0@sha256:91f78d334302fced5c5aecb5bdb1cb99757a8b05d0c9fda5d3b9855ebc6d1236
FROM ${BAYMA_IMAGE}
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's session sandboxes: bayma, run as the unprivileged agent"
RUN usermod --login agent --home /home/agent --move-home ubuntu \
 && groupmod --new-name agent ubuntu \
 && mkdir -p /workspace /home/agent \
 && chown agent:agent /workspace /home/agent
# bayma installs its toolbelt (its own fixed set of tools, ~25k files) under
# $XDG_DATA_HOME before it serves. Onto every new session's volume that copy would take
# minutes, so it is installed here once, by bayma's own installer (doctor prepares the
# payload before it runs), and each session's ~/.local/share/bayma/toolbelt is a link to
# it, made as its pod starts. bayma finds its version current there and copies nothing.
# The doctor's own verdict does not matter here, only the installed toolbelt.
RUN mkdir -p /tmp/bayma-build \
 && (XDG_DATA_HOME=/opt/bayma-data HOME=/tmp/bayma-build BAYMA_PAYLOAD_DIR=/opt/bayma/payload \
       node /opt/bayma/bayma.js doctor --runtime bun --state-dir /tmp/bayma-build/state >/dev/null 2>&1 || true) \
 && test -f /opt/bayma-data/bayma/toolbelt/.bayma-toolbelt-version \
 && chown -R agent:agent /opt/bayma-data \
 && rm -rf /tmp/bayma-build
