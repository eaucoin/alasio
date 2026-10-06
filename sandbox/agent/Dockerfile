# The image session filesystems run: bayma's, with every runtime bayma serves, run as
# the unprivileged user `agent`. No harness runs here: Claude Code and Codex run in alasio
# and reach a session only through its bayma. /workspace and /home/agent are the
# session's volume, mounted by its Sandbox (cli/src/manifests/alasio.ts, sessionsProfile).
ARG BAYMA_IMAGE=ghcr.io/eaucoin/bayma:0.12.2@sha256:654750dd6a1ddcc36aaf46bf5676c58a3438fab3545da454c05aeb296f42a4e6
FROM ${BAYMA_IMAGE}
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's session sandboxes: bayma, run as the unprivileged agent"
RUN usermod --login agent --home /home/agent --move-home ubuntu \
 && groupmod --new-name agent ubuntu \
 && mkdir -p /workspace /home/agent \
 && chown agent:agent /workspace /home/agent
# bayma installs its toolbelt (its own fixed set of tools, ~25k files) under
# $XDG_DATA_HOME, where a server that finds none of its version installs it in the
# background. Onto every new session's volume that copy would take minutes, so it is
# installed here once, by bayma's own installer, and each session's
# ~/.local/share/bayma/toolbelt is a link to it, made as its pod starts. bayma finds its
# version current there and copies nothing.
RUN mkdir -p /tmp/bayma-build \
 && XDG_DATA_HOME=/opt/bayma-data HOME=/tmp/bayma-build BAYMA_PAYLOAD_DIR=/opt/bayma/payload \
      node /opt/bayma/bayma.js install-toolbelt \
 && chown -R agent:agent /opt/bayma-data \
 && rm -rf /tmp/bayma-build
