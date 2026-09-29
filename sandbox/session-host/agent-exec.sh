#!/bin/bash
# Runs a command as the agent inside the running sandbox, stdio passed through, for the
# harness alasio starts:  docker exec -i <session host> agent-exec <command> [args...]
# Extra environment comes from AGENT_ENV in /run/agent-env (KEY=VALUE per line), which
# alasio writes before starting a harness (the gateway bearer, CODEX_HOME, ...).
set -euo pipefail
env_args=()
# The session's base env (the gateway bearer, base URLs, CODEX_HOME), written by alasio
# when it starts a harness. Persists across execs.
if [ -f /run/agent-env ]; then
  while IFS= read -r line; do [ -n "$line" ] && env_args+=(--env "$line"); done < /run/agent-env
fi
# Any env this invocation was given (docker exec -e), minus what agent-exec sets itself,
# so a harness's per-spawn env reaches inside without carrying the host's PATH/HOME.
for name in $(env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p'); do
  case "$name" in HOME|USER|PATH|PWD|SHLVL|_|HOSTNAME|TERM) continue ;; esac
  env_args+=(--env "$name=$(printenv "$name")")
done
exec /opt/gvisor/runsc --root /run/runsc exec \
  --user 1000:1000 --cwd /workspace \
  --env HOME=/home/agent --env USER=agent \
  --env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  "${env_args[@]}" session "$@"
