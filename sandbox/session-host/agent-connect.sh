#!/bin/bash
# Connects stdin and stdout to a TCP port on the sandbox's own loopback, which nothing
# outside the sandbox reaches otherwise:
#   docker exec -i <session host> agent-connect <port>
# alasio pipes each harness connection to bayma through it (src/sandbox/bayma-forward.js).
# The connection is made from inside, as the agent, so it needs no listener or firewall
# opening anywhere.
set -euo pipefail
port=${1:?usage: agent-connect <port>}
case "$port" in
  "" | *[!0-9]*) echo "agent-connect: invalid port: $port" >&2; exit 2 ;;
esac
exec agent-exec node -e '
const socket = require("node:net").connect(Number(process.argv[1]), "127.0.0.1");
process.stdin.pipe(socket);
socket.pipe(process.stdout);
socket.on("error", () => process.exit(1));
socket.on("close", () => process.exit(0));
' "$port"
