# The image session filesystems run: bayma's, with every runtime bayma serves, run as
# the unprivileged user `agent`. No harness runs here: Claude Code and Codex run in alasio
# and reach a session only through its bayma. A pod of a sandbox pool runs it, its
# workspace attached at /attach/ws once it is bound, by the node agent of its node
# (src/node-agent/workspaces.ts): /workspace and /home/agent are links to the workspace's
# own, and its bayma starts once its binding's token is beside it (start.sh).
ARG BAYMA_IMAGE=ghcr.io/eaucoin/bayma:0.14.2@sha256:5a9a731371d083cae815c6f27c5eb0147e1e77fac400544285f8416047ba4109
FROM ${BAYMA_IMAGE}
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's session sandboxes: bayma, run as the unprivileged agent"
RUN usermod --login agent --home /home/agent ubuntu \
 && groupmod --new-name agent ubuntu \
 && rm -rf /home/ubuntu \
 && mkdir -p /attach \
 && ln -s /attach/ws/workspace /workspace \
 && ln -s /attach/ws/home /home/agent
COPY --chmod=0755 start.sh /usr/local/bin/alasio-start
# bayma installs its toolbelt (its own fixed set of tools, ~25k files) under
# $XDG_DATA_HOME, where a server that finds none of its version installs it in the
# background. Onto every new session's workspace that copy would take minutes, so it is
# installed here once, by bayma's own installer, and each session's
# ~/.local/share/bayma/toolbelt is a link to it, made as its bayma starts (start.sh).
# bayma finds its version current there and copies nothing.
RUN mkdir -p /tmp/bayma-build \
 && XDG_DATA_HOME=/opt/bayma-data HOME=/tmp/bayma-build BAYMA_PAYLOAD_DIR=/opt/bayma/payload \
      node /opt/bayma/bayma.js install-toolbelt \
 && chown -R agent:agent /opt/bayma-data \
 && rm -rf /tmp/bayma-build
