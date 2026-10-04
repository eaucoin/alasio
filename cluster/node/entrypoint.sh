#!/bin/sh
# Readies the node's container for k3s, then runs k3s with the container's arguments
# (`server ...` or `agent ...`). It runs at every start of the container: what it changes
# lives in the container's namespaces, which a restart makes anew.
set -eu

# cgroup v2, in the container's own cgroup namespace: a cgroup that holds processes
# cannot hand controllers down to cgroups below it, as kubelet's need, so the
# container's processes move out of the root into /init, and the root hands every
# controller down, as Docker-in-Docker does.
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
  mkdir -p /sys/fs/cgroup/init
  xargs -rn1 </sys/fs/cgroup/cgroup.procs >/sys/fs/cgroup/init/cgroup.procs || :
  sed -e 's/ / +/g' -e 's/^/+/' </sys/fs/cgroup/cgroup.controllers >/sys/fs/cgroup/cgroup.subtree_control
fi

# DNS: on its network the container resolves through Docker's embedded server at
# 127.0.0.11, which Docker reaches with NAT rules in the container's network namespace and
# which pods, in namespaces of their own, cannot reach. The rules are made to answer at
# the network's gateway instead, for pods as well (PREROUTING), and the container resolves
# there, so CoreDNS, forwarding to the node's resolv.conf, resolves through Docker too.
gateway=$(ip -4 route show default | sed -n 's/^default via \([0-9.]*\).*/\1/p')
iptables-save \
  | sed -e "s/-d 127\.0\.0\.11/-d $gateway/g" \
    -e 's/-A OUTPUT \(.*\) -j DOCKER_OUTPUT/\0\n-A PREROUTING \1 -j DOCKER_OUTPUT/' \
    -e "s/--to-source :53/--to-source $gateway:53/g" \
  | iptables-restore
# resolv.conf is a file Docker mounts, so it is written in place.
if grep -q '127\.0\.0\.11' /etc/resolv.conf; then
  resolv=$(sed "s/127\.0\.0\.11/$gateway/g" /etc/resolv.conf)
  printf '%s\n' "$resolv" >/etc/resolv.conf
fi

# The root's mounts shared, so mounts kubelet makes for pods propagate as they ask.
mount --make-rshared /

exec /bin/k3s "$@"
