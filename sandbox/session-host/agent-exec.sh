#!/bin/bash
# Runs a command as the agent inside the running sandbox, stdio passed through, for the
# harness alasio starts:  docker exec -i <session host> agent-exec <command> [args...]
# Extra environment comes from AGENT_ENV in /run/agent-env (KEY=VALUE per line), which
# alasio writes before starting a harness (the gateway bearer, CODEX_HOME, ...).
set -euo pipefail
env_args=()
if [ -f /run/agent-env ]; then
  while IFS= read -r line; do [ -n "$line" ] && env_args+=(--env "$line"); done < /run/agent-env
fi
exec /opt/gvisor/runsc --root /run/runsc exec \
  --user 1000:1000 --cwd /workspace \
  --env HOME=/home/agent --env USER=agent \
  --env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  "${env_args[@]}" session "$@"
