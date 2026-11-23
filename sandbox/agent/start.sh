#!/bin/sh
# What a sandbox pool's pod runs before bayma, and before bayma's hook (src/gateway/sessions.ts):
# it waits until its workspace is attached and its binding's token is beside it, which the
# node agent writes last (src/node-agent/workspaces.ts); runs bayma's hook then, as it is asked;
# else takes the variables written with the token, where bayma exports its telemetry among
# them; links bayma's toolbelt, which the image installs, into the workspace's home; then runs
# what it is given in the workspace. bayma's server runs once in a pod, as a pool's pod serves
# one binding: its container started again, as a pool's pods' are once a save has ended their
# sandbox, or once bayma has ended, runs nothing more of the workspace, which the gateway
# releases.
# Its directories are where a pool's pod mounts them, its hook bayma's image's, and its hook's
# bound for a save 30 s, but for tests.
set -eu
attach="${ALASIO_ATTACH:-/attach}"
workspace="${ALASIO_WORKSPACE:-/workspace}"
# The binaries a restore of this pod's sandbox reads first, bayma's node and the runtimes' (or
# ALASIO_WARM_FILES's, for tests), opened as bayma's server starts in a pool's pod, before it waits
# for its binding: where the node serves the image through FUSE passthrough (cli/src/cluster/stargz.ts),
# a file's first open merges it into its backing file, which is paid here, not under a session's bind
# or restore; elsewhere it reads a byte of each, which costs nothing.
case " $* " in
*" mcp-http "*)
  for file in ${ALASIO_WARM_FILES:-$(command -v node) $(find /opt/bayma/payload -maxdepth 4 -type f -perm -u+x \( -name bun -o -name 'python3.[0-9]*' \) 2>/dev/null)}; do
    head -c 1 "$file" >/dev/null 2>&1 || true
  done
  ;;
esac
until [ -s "$attach/token" ]; do sleep 0.1; done
# bayma's hook is the one bayma's image carries without Node (bayma-sandbox-hook), given what
# follows `sandbox-hook`: a Node process's start, in a sandbox just restored, is most of the
# hook's time on a restore's path, and the hook needs nothing of the setup below. As gVisor
# runs it to ready the sandbox for a save, or to go on after one taken live, it has 30 s
# (src/node-agent/names.ts SAVE_HOOK_SECONDS), past which it is killed and the save fails.
# runsc's own bound is a restore's, as long as a restore's start may take while its pages are
# read lazily, which runsc keeps with the sandbox from its save.
case " $* " in
*" sandbox-hook "*)
  for arg; do
    shift
    if [ "$arg" = sandbox-hook ]; then break; fi
  done
  hook="${ALASIO_BAYMA_HOOK:-/usr/local/bin/bayma-sandbox-hook}"
  case "${GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE:-}" in
  save | resume) exec timeout --kill-after=1 "${ALASIO_SAVE_HOOK_SECONDS:-30}" "$hook" "$@" ;;
  esac
  exec "$hook" "$@"
  ;;
esac
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
exec "$@"
