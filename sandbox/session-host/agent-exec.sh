#!/bin/bash
# Runs a command as the agent inside the running sandbox, stdio passed through, for the
# harness alasio starts:
#   docker exec -i [-e NAME=VALUE ... -e AGENT_EXEC_VARS=NAME,...] <session host> agent-exec <command> [args...]
# The command sees only the agent's own HOME/USER/PATH, the session's base env from
# /run/agent-env, and the variables AGENT_EXEC_VARS names. Nothing else of this
# container's environment is carried in: it holds the volume's storage keys and
# metadata URL, which the agent must never see.
set -euo pipefail
env_args=()
# The session's base env (the gateway bearer, base URLs, CODEX_HOME), written by alasio
# when it starts a harness. Persists across execs.
if [ -f /run/agent-env ]; then
  while IFS= read -r line; do [ -n "$line" ] && env_args+=(--env "$line"); done < /run/agent-env
fi
# The per-spawn env alasio passed with `docker exec -e`, by name only.
IFS=, read -r -a forward <<< "${AGENT_EXEC_VARS:-}"
for name in "${forward[@]}"; do
  case "$name" in
    "" | HOME | USER | PATH) continue ;;
    *[!A-Za-z0-9_]* | [0-9]*) echo "agent-exec: invalid variable name: $name" >&2; exit 2 ;;
  esac
  [ -n "${!name+set}" ] && env_args+=(--env "$name=${!name}")
done
exec /opt/gvisor/runsc --root /run/runsc exec \
  --user 1000:1000 --cwd /workspace \
  --env HOME=/home/agent --env USER=agent \
  --env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  "${env_args[@]}" session "$@"
