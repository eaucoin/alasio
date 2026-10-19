#!/bin/sh
# What a sandbox pool's pod runs before bayma, and before bayma's hook (src/gateway/sessions.ts):
# it waits until its workspace is attached and its binding's token is beside it, which the
# node agent writes last (src/node-agent/workspaces.ts); takes the variables written with
# the token, where bayma exports its telemetry among them; links bayma's toolbelt, which the
# image installs, into the workspace's home; then runs what it is given in the workspace.
# bayma's server runs once in a pod, as a pool's pod serves one binding: its container started
# again, as a pool's pods' are once a save has ended their sandbox, or once bayma has ended,
# runs nothing more of the workspace, which the gateway releases; bayma's hook runs as it is
# asked.
# Its directories are where a pool's pod mounts them, and its hook's bound for a save 30 s,
# but for tests.
set -eu
attach="${ALASIO_ATTACH:-/attach}"
workspace="${ALASIO_WORKSPACE:-/workspace}"
until [ -s "$attach/token" ]; do sleep 0.1; done
case " $* " in
*" mcp-http "*)
  if [ -e "$attach/served" ]; then
    echo "bayma has served this pod's binding already, and is not started again in it" >&2
    exit 1
  fi
  : > "$attach/served"
  ;;
esac
set -a
. "$attach/env"
set +a
mkdir -p "$HOME/.local/share/bayma"
ln -sfn /opt/bayma-data/bayma/toolbelt "$HOME/.local/share/bayma/toolbelt"
cd "$workspace"
# bayma's hook, as gVisor runs it to ready the sandbox for a save, or to go on after one
# taken live, has 30 s (src/node-agent/agent.ts SAVE_HOOK_SECONDS), past which it is killed
# and the save fails. runsc's own bound is a restore's, as long as a restore's start may
# take while its pages are read lazily, which runsc keeps with the sandbox from its save.
case "${GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE:-}" in
save | resume) exec timeout --kill-after=1 "${ALASIO_SAVE_HOOK_SECONDS:-30}" "$@" ;;
esac
exec "$@"
