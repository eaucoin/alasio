#!/usr/bin/env bash
# Spike, not for main: runs a command as root (sudo, as Debian's cloud user) in a Debian 13 VM on this runner, under KVM, on
# Debian's generic 6.12 kernel (the box's kernel flavour, linux-image-amd64 as trixie ships it),
# for the experiments that need Linux 6.12 beside the runner's 7.0. The files given are copied
# into the VM's /val first; what the command prints is its output here.
#
#   bench/val/vm612.sh <memory MiB> <command> [file...]
set -euo pipefail
memory=$1
command=$2
shift 2
work=${VAL_VM:-/mnt/vm612}
image=https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-amd64.qcow2
port=2222
ssh_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -i "$work/key" -p "$port")

start() {
  qemu-system-x86_64 -enable-kvm -cpu host -smp "$(nproc)" -m "$memory" -nographic \
    -drive "file=$work/disk.qcow2,if=virtio" -drive "file=$work/seed.img,if=virtio,format=raw" \
    -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:$port-:22" -device virtio-net-pci,netdev=net0 \
    >"$work/console.log" 2>&1 &
  echo $! >"$work/qemu.pid"
  for _ in $(seq 300); do
    ssh "${ssh_opts[@]}" debian@127.0.0.1 true 2>/dev/null && return 0
    sleep 2
  done
  tail -n 50 "$work/console.log"
  return 1
}

stop() {
  ssh "${ssh_opts[@]}" debian@127.0.0.1 sudo poweroff 2>/dev/null || true
  for _ in $(seq 60); do kill -0 "$(cat "$work/qemu.pid")" 2>/dev/null || return 0; sleep 1; done
  kill "$(cat "$work/qemu.pid")" || true
}

if [ ! -f "$work/ready" ]; then
  mkdir -p "$work"
  command -v qemu-system-x86_64 >/dev/null || { sudo apt-get update -q && sudo apt-get install -yq qemu-system-x86 qemu-utils cloud-image-utils; }
  sudo chmod 666 /dev/kvm
  curl -fsSL -o "$work/base.qcow2" "$image"
  qemu-img create -f qcow2 -F qcow2 -b "$work/base.qcow2" "$work/disk.qcow2" 20G
  ssh-keygen -q -t ed25519 -N "" -f "$work/key"
  cat >"$work/user-data" <<EOF
#cloud-config
ssh_authorized_keys: ["$(cat "$work/key.pub")"]
EOF
  cloud-localds "$work/seed.img" "$work/user-data"
  start
  # The generic kernel, as the box runs (the cloud image boots Debian's cloud flavour), and a reboot into it.
  ssh "${ssh_opts[@]}" debian@127.0.0.1 'sudo sh -c "DEBIAN_FRONTEND=noninteractive apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -yq linux-image-amd64 >/dev/null && rm -f /boot/vmlinuz-*cloud* /boot/initrd.img-*cloud* && update-grub >/dev/null 2>&1"'
  stop
  start
  touch "$work/ready"
else
  start
fi
ssh "${ssh_opts[@]}" debian@127.0.0.1 'uname -r; sudo mkdir -p /val; sudo chown debian /val' >&2
for file in "$@"; do scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -i "$work/key" -P "$port" "$file" debian@127.0.0.1:/val/; done
status=0
ssh "${ssh_opts[@]}" debian@127.0.0.1 "cd /val && sudo sh -c '$command'" || status=$?
stop
exit "$status"
