# alasio's cluster node, ghcr.io/eaucoin/alasio-node: k3s with gVisor as a containerd
# runtime, `runsc`, so pods with the `gvisor` RuntimeClass run sandboxed, and an entrypoint
# that readies the container for k3s (entrypoint.sh). What alasio's cluster in Docker runs
# on, on a single machine and in CI. The k3s image, and the gVisor release with its sha512,
# are pins.json's, whose k3s and gVisor the host target installs on a machine itself, with
# the same containerd template: every build passes them as build arguments, as
# tooling/node-image.ts says them.
ARG K3S_IMAGE
FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 AS gvisor
ARG GVISOR_RELEASE
ARG GVISOR_SHA512
ADD https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE}/x86_64/gvisor.tar.zstd /tmp/gvisor.tar.zstd
RUN echo "${GVISOR_SHA512}  /tmp/gvisor.tar.zstd" | sha512sum -c - \
 && apk add --no-cache zstd \
 && mkdir /gvisor \
 && zstd -dc /tmp/gvisor.tar.zstd | tar -x -C /gvisor \
 && test -x /gvisor/runsc -a -x /gvisor/containerd-shim-runsc-v1 -a -x /gvisor/gvisor-bin/gvisor_sentry

FROM ${K3S_IMAGE}
# runsc and its shim, and the Sentry, which current gVisor runs as a sidecar binary.
COPY --from=gvisor /gvisor/runsc /gvisor/containerd-shim-runsc-v1 /bin/
COPY --from=gvisor /gvisor/gvisor-bin /bin/gvisor-bin
COPY config-v3.toml.tmpl /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl
COPY entrypoint.sh /bin/alasio-node-entrypoint
ENTRYPOINT ["/bin/alasio-node-entrypoint"]
