# The agent's world: bayma's image (every runtime bayma serves) plus the Claude Code
# and Codex CLIs. Mounted read-only as the gVisor sandbox's root at /agent-root, so
# every session shares it. The agent runs as the unprivileged user `agent`; /workspace
# and /home/agent are bound from the session's JuiceFS volume at run time.
ARG BAYMA_IMAGE=ghcr.io/eaucoin/bayma:0.7.0
FROM ${BAYMA_IMAGE}
# Copied into the build context by build.sh from alasio's node_modules.
COPY claude /usr/local/bin/claude
COPY codex /usr/local/bin/codex
COPY codex-code-mode-host /usr/local/bin/codex-code-mode-host
RUN usermod --login agent --home /home/agent --move-home ubuntu \
 && groupmod --new-name agent ubuntu \
 && mkdir -p /workspace /home/agent \
 && chown agent:agent /workspace /home/agent \
 && chmod +x /usr/local/bin/claude /usr/local/bin/codex /usr/local/bin/codex-code-mode-host
