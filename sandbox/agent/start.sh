#!/bin/sh
# What a sandbox pool's pod runs before bayma, and before bayma's hook (src/gateway/sessions.ts):
# it waits until its workspace is attached and its binding's token is beside it, which the
# node agent writes last (src/node-agent/workspaces.ts); takes the variables written with
# the token, where bayma exports its telemetry among them; links bayma's toolbelt, which the
# image installs, into the workspace's home; then runs what it is given in the workspace.
set -eu
until [ -s /attach/token ]; do sleep 0.1; done
set -a
. /attach/env
set +a
mkdir -p "$HOME/.local/share/bayma"
ln -sfn /opt/bayma-data/bayma/toolbelt "$HOME/.local/share/bayma/toolbelt"
cd /workspace
exec "$@"
