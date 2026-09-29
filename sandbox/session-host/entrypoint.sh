#!/bin/bash
# The session host's entrypoint. It mounts the session's JuiceFS volume, builds the
# agent a network namespace with the chosen internet mode, and runs the agent, its
# harness, and bayma inside one gVisor sandbox whose root is the agent image.
#
# The boundary is gVisor: an escape from it lands in this privileged container, exactly
# where an escape from a host-installed gVisor would land, so nesting costs no strength
# and needs nothing installed on the host. See sandbox/README.md.
#
# Configuration, all by environment (alasio sets these; see src/sandbox/session-host.js):
#   JFS_META            JuiceFS metadata URL, no password (META_PASSWORD_FILE supplies it)
#   JFS_FORMAT          1 to `juicefs format` first (JFS_NAME, JFS_STORAGE, JFS_BUCKET,
#                       and ACCESS_KEY/SECRET_KEY for the object store)
#   JFS_CACHE_MB        JuiceFS data cache bound (default 1024)
#   NET_MODE            none (the gateway only) or full (the public internet)
#   GATEWAY_IP, GATEWAY_PORT   the one address the agent may always reach
#   HOST_PUBLIC_IP      the host's own public address, always dropped in full mode
#   AGENT_IMAGE_ROOT    where the read-only agent image is mounted (default /agent-root)
#   BAYMA_HTTP_PORT     the port bayma serves inside the sandbox (default 7290)
#   RESTORE             1 to `runsc restore` a prior checkpoint from CHECKPOINT_DIR
#   CHECKPOINT_DIR      where a checkpoint image is read and written (default /checkpoint)
# It prints `session-host ready <ms>` once the sandbox runs, then waits; SIGTERM tears
# down, checkpointing first when CHECKPOINT_ON_STOP=1.
set -euo pipefail

T0=$(date +%s%3N)
stamp() { echo "session-host $1 $(( $(date +%s%3N) - T0 ))ms" >&2; }
AGENT_ROOT=${AGENT_IMAGE_ROOT:-/agent-root}
BAYMA_PORT=${BAYMA_HTTP_PORT:-7290}
CKPT=${CHECKPOINT_DIR:-/checkpoint}
RUNSC=(/opt/gvisor/runsc --root /run/runsc)

# --- 1. The volume ------------------------------------------------------------
mkdir -p /mnt/session
if [ "${JFS_FORMAT:-0}" = 1 ]; then
  META_PASSWORD="$(cat "${META_PASSWORD_FILE:?}")" \
  juicefs format --trash-days 0 --storage "$JFS_STORAGE" --bucket "$JFS_BUCKET" \
    "$JFS_META" "$JFS_NAME" >/tmp/juicefs-format.log 2>&1 || { cat /tmp/juicefs-format.log >&2; exit 1; }
  stamp formatted
fi
META_PASSWORD="$(cat "${META_PASSWORD_FILE:?}")" GOMEMLIMIT=256MiB \
  juicefs mount -d --no-agent --hide-internal --enable-xattr -o allow_other \
  --cache-size "${JFS_CACHE_MB:-1024}" --buffer-size 100 \
  --log /tmp/juicefs.log "$JFS_META" /mnt/session
mkdir -p /mnt/session/workspace /mnt/session/home
chown 1000:1000 /mnt/session/workspace /mnt/session/home
stamp mounted

# --- 2. The agent's network ---------------------------------------------------
# A namespace of the agent's own, joined to this host by a veth pair; this host's
# firewall (outside gVisor) decides what the agent reaches. The rules are E7's.
ip netns add agent
ip link add host0 type veth peer name agent0
ip link set agent0 netns agent
ip addr add 10.200.0.1/30 dev host0 && ip link set host0 up
ip netns exec agent ip addr add 10.200.0.2/30 dev agent0
ip netns exec agent ip link set agent0 up
ip netns exec agent ip link set lo up
ip netns exec agent ip route add default via 10.200.0.1
ip netns exec agent sysctl -qw net.ipv6.conf.all.disable_ipv6=1 || true
sysctl -qw net.ipv4.ip_forward=1
# Clamp MSS so a smaller uplink MTU does not stall downloads (E7).
iptables -t mangle -A FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu
iptables -t nat -A POSTROUTING -s 10.200.0.2 -o eth0 -j MASQUERADE
iptables -P FORWARD DROP
iptables -A FORWARD -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A FORWARD -s 10.200.0.2 -m conntrack --ctstate NEW -m connlimit --connlimit-above 200 -j REJECT
# The agent never reaches this host's own listeners (JuiceFS's DB and store live past them).
iptables -P INPUT DROP
iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A INPUT -i lo -j ACCEPT
ip6tables -P FORWARD DROP 2>/dev/null || true
ip6tables -P INPUT DROP 2>/dev/null || true
if [ -n "${GATEWAY_IP:-}" ]; then
  iptables -A FORWARD -s 10.200.0.2 -d "$GATEWAY_IP" -p tcp --dport "${GATEWAY_PORT:?}" -j ACCEPT
fi
if [ "${NET_MODE:-none}" = full ]; then
  for net in 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10 169.254.0.0/16 \
             127.0.0.0/8 224.0.0.0/4 0.0.0.0/8 ${HOST_PUBLIC_IP:+$HOST_PUBLIC_IP/32}; do
    iptables -A FORWARD -s 10.200.0.2 -d "$net" -j DROP
  done
  iptables -A FORWARD -s 10.200.0.2 -j ACCEPT
  printf 'nameserver 1.1.1.1\nnameserver 8.8.8.8\n' > /run/agent-resolv.conf
else
  : > /run/agent-resolv.conf
fi
stamp network

# --- 3. The sandbox -----------------------------------------------------------
mkdir -p /bundle "$CKPT" && cd /bundle
# The sandbox's init is tini running bayma's HTTP MCP server, so it reaps orphans and
# survives checkpoint/restore (E6). The harness (Claude Code, Codex) is added later
# with `runsc exec`. bayma serves on loopback inside the agent's namespace.
/opt/gvisor/runsc spec
jq --arg root "$AGENT_ROOT" --arg port "$BAYMA_PORT" '
    .root = {path: $root, readonly: true}
  | .process.terminal = false
  | .process.noNewPrivileges = true
  | .process.user = {uid: 1000, gid: 1000}
  | .process.cwd = "/home/agent"
  | .process.args = ["/usr/bin/tini", "-s", "--", "/bin/sh", "-c",
      "exec node /opt/bayma/bayma.js mcp-http --host 127.0.0.1 --port " + $port + " --state-dir /home/agent/.bayma"]
  | .process.env = [
      "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      "HOME=/home/agent", "USER=agent", "BAYMA_PAYLOAD_DIR=/opt/bayma/payload"]
  | .hostname = "session"
  | .linux.namespaces = ([.linux.namespaces[] | select(.type != "network")]
      + [{type: "network", path: "/run/netns/agent"}])
  | .mounts += [
      {destination: "/workspace", type: "bind", source: "/mnt/session/workspace", options: ["rbind","rw","nosuid","nodev"]},
      {destination: "/home/agent", type: "bind", source: "/mnt/session/home", options: ["rbind","rw","nosuid","nodev"]},
      {destination: "/tmp", type: "tmpfs", source: "tmpfs", options: ["nosuid","nodev","size=1g"]},
      {destination: "/etc/resolv.conf", type: "bind", source: "/run/agent-resolv.conf", options: ["rbind","ro"]}
    ]' config.json > config.tmp && mv config.tmp config.json

restore_network() {
  # runsc moves agent0's address/route into its netstack and does not return them; a
  # restore into the same namespace needs them re-added first (E6).
  ip netns exec agent ip addr add 10.200.0.2/30 dev agent0 2>/dev/null || true
  ip netns exec agent ip route add default via 10.200.0.1 2>/dev/null || true
}
if [ "${RESTORE:-0}" = 1 ] && [ -f "$CKPT/checkpoint.img" ]; then
  restore_network
  "${RUNSC[@]}" --network=sandbox --ignore-cgroups restore --image-path "$CKPT" session
else
  "${RUNSC[@]}" --network=sandbox --ignore-cgroups run --detach session
fi
stamp sandbox
echo "session-host ready $(( $(date +%s%3N) - T0 ))"

teardown() {
  if [ "${CHECKPOINT_ON_STOP:-0}" = 1 ]; then
    "${RUNSC[@]}" checkpoint --image-path "$CKPT" --compression flate-best-speed session 2>>/tmp/checkpoint.log \
      && echo "session-host checkpointed" >&2 || echo "session-host checkpoint failed (see /tmp/checkpoint.log)" >&2
  fi
  "${RUNSC[@]}" kill session KILL 2>/dev/null || true
  "${RUNSC[@]}" delete --force session 2>/dev/null || true
  juicefs umount /mnt/session 2>/dev/null || true
  exit 0
}
trap teardown TERM INT
while :; do sleep 3600 & wait $!; done
