#!/usr/bin/env bash
# alasio-standalone.service's ExecStart: runs the bot in the alasio-standalone
# image with what it has on the host itself, so moving it into a container
# changes nothing it or its agents can do:
#   - the same user, /home (at the same path), and /tmp, so files,
#     tools, tmux sockets, and state are the host's
#   - the host's network, processes, and IPC, so ports, localhost services,
#     and process signals are as before
#   - the host's Docker, Tailscale, Stripe, and Google Cloud CLIs and daemons
#   - the host's system D-Bus, so restart-alasio-standalone.sh's
#     `systemctl restart alasio-standalone.service` reaches the host's systemd,
#     which a polkit rule lets this user do for this unit alone
#   - the environment systemd gives this unit, passed through by name
set -euo pipefail

IMAGE="alasio-standalone"
NAME="alasio-standalone"

environment=()
while IFS= read -r variable; do
  environment+=(--env "$variable")
done < <(compgen -e)

exec /usr/bin/docker run --rm --init --name "$NAME" \
  --user "$(id -u):$(id -g)" \
  --group-add "$(getent group docker | cut -d: -f3)" \
  --network host --pid host --ipc host \
  --security-opt seccomp=unconfined --security-opt apparmor=unconfined \
  --log-driver none \
  --volume /etc/passwd:/etc/passwd:ro \
  --volume /etc/group:/etc/group:ro \
  --volume /etc/localtime:/etc/localtime:ro \
  --volume /home:/home \
  --volume /tmp:/tmp \
  --volume /var/run/docker.sock:/var/run/docker.sock \
  --volume /usr/bin/docker:/usr/bin/docker:ro \
  --volume /usr/libexec/docker/cli-plugins:/usr/libexec/docker/cli-plugins:ro \
  --volume /var/run/tailscale:/var/run/tailscale \
  --volume /usr/bin/tailscale:/usr/bin/tailscale:ro \
  --volume /usr/bin/stripe:/usr/bin/stripe:ro \
  --volume /usr/lib/google-cloud-sdk:/usr/lib/google-cloud-sdk:ro \
  --volume /run/dbus/system_bus_socket:/run/dbus/system_bus_socket \
  --volume /run/systemd:/run/systemd:ro \
  --workdir "$PWD" \
  "${environment[@]}" \
  "$IMAGE" "$@"
