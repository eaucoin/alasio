#!/bin/bash
# Runs a command as the agent inside the running sandbox, stdio passed through:
#   docker exec -i <session host> agent-exec <command> [args...]
# The command sees only the agent's own HOME, USER and PATH. Nothing of this container's
# environment is carried in: it holds the volume's storage keys and metadata URL, which
# the agent must never see.
set -euo pipefail
exec /opt/gvisor/runsc --root /run/runsc exec \
  --user 1000:1000 --cwd /workspace \
  --env HOME=/home/agent --env USER=agent \
  --env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  session "$@"
