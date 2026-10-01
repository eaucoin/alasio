# The agent's world: bayma's image, with every runtime bayma serves. Mounted read-only
# as the gVisor sandbox's root at /agent-root, so every session shares it. No harness
# runs here: Claude Code and Codex run in alasio and reach the sandbox only through
# bayma. The agent runs as the unprivileged user `agent`; /workspace and /home/agent are
# bound from the session's JuiceFS volume at run time.
ARG BAYMA_IMAGE=ghcr.io/eaucoin/bayma:0.9.0@sha256:97c6546fbb9c303d51f1010ea89eae413228ba4625a25a30bd25eac332178a35
FROM ${BAYMA_IMAGE}
RUN usermod --login agent --home /home/agent --move-home ubuntu \
 && groupmod --new-name agent ubuntu \
 && mkdir -p /workspace /home/agent \
 && chown agent:agent /workspace /home/agent
# bayma installs its toolbelt (its own fixed set of tools, ~25k files) under
# $XDG_DATA_HOME before it serves. Onto a session's JuiceFS volume that copy takes
# minutes, so it is installed here once, by bayma's own installer (doctor prepares the
# payload before it runs), and each session's ~/.local/share/bayma/toolbelt is a link to
# it (session-host/entrypoint.sh). bayma finds its version current there and copies
# nothing. The doctor's own verdict does not matter here, only the installed toolbelt.
RUN mkdir -p /tmp/bayma-build \
 && (XDG_DATA_HOME=/opt/bayma-data HOME=/tmp/bayma-build BAYMA_PAYLOAD_DIR=/opt/bayma/payload \
       node /opt/bayma/bayma.js doctor --runtime bun --state-dir /tmp/bayma-build/state >/dev/null 2>&1 || true) \
 && test -f /opt/bayma-data/bayma/toolbelt/.bayma-toolbelt-version \
 && chown -R agent:agent /opt/bayma-data \
 && rm -rf /tmp/bayma-build
