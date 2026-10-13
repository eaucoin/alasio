# alasio's cluster node, ghcr.io/eaucoin/alasio-node: k3s with the gVisor releases alasio
# keeps, each in /usr/local/lib/alasio/gvisor/RELEASE, stargz-snapshotter's daemon, which
# pulls eStargz images lazily, and an entrypoint that readies the container for k3s and
# starts the daemon (entrypoint.sh). What alasio's cluster in Docker runs on, on a single
# machine and in CI; alasio puts the files that make the daemon containerd's snapshotter
# and each release a runtime of containerd's, so pods with the `gvisor` RuntimeClass run
# sandboxed, into each node's container itself (cli/src/cluster/docker.ts). The k3s image,
# the gVisor releases with their sha512s and stargz-snapshotter's with its sha256 are
# pins.json's, whose k3s, gVisor and stargz-snapshotter the host target installs on a
# machine itself, at the same paths: every build passes them as build arguments, as
# tooling/node-image.ts says them, GVISOR_RELEASES as RELEASE:SHA512 pairs, separated by
# spaces. gVisor is alasio's fork of it, github.com/eaucoin/gvisor.
ARG K3S_IMAGE
FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 AS gvisor
ARG GVISOR_RELEASES
# Of each release's archive, what the host target installs of it too: runsc and its shim,
# and gvisor-bin/, the Sentry and the binaries runsc runs beside it.
RUN set -eu; \
    apk add --no-cache zstd; \
    test -n "${GVISOR_RELEASES}"; \
    for pair in ${GVISOR_RELEASES}; do \
      release="${pair%%:*}"; \
      wget -q -O /tmp/gvisor.tar.zstd "https://github.com/eaucoin/gvisor/releases/download/release-${release}/gvisor-x86_64.tar.zstd"; \
      echo "${pair#*:}  /tmp/gvisor.tar.zstd" | sha512sum -c -; \
      mkdir -p /tmp/gvisor "/gvisor/${release}"; \
      zstd -dc /tmp/gvisor.tar.zstd | tar -x -C /tmp/gvisor; \
      mv /tmp/gvisor/runsc /tmp/gvisor/containerd-shim-runsc-v1 /tmp/gvisor/gvisor-bin "/gvisor/${release}/"; \
      test -x "/gvisor/${release}/runsc" -a -x "/gvisor/${release}/containerd-shim-runsc-v1" -a -x "/gvisor/${release}/gvisor-bin/gvisor_sentry"; \
      rm -r /tmp/gvisor /tmp/gvisor.tar.zstd; \
    done

FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 AS stargz
ARG STARGZ_VERSION
ARG STARGZ_SHA256
ADD https://github.com/containerd/stargz-snapshotter/releases/download/${STARGZ_VERSION}/stargz-snapshotter-${STARGZ_VERSION}-linux-amd64.tar.gz /tmp/stargz.tar.gz
RUN echo "${STARGZ_SHA256}  /tmp/stargz.tar.gz" | sha256sum -c - \
 && mkdir /stargz \
 && tar -xzf /tmp/stargz.tar.gz -C /stargz containerd-stargz-grpc stargz-fuse-manager

FROM ${K3S_IMAGE}
COPY --from=gvisor /gvisor /usr/local/lib/alasio/gvisor
# stargz-snapshotter's daemon and its FUSE manager, where its configuration names them.
COPY --from=stargz /stargz/containerd-stargz-grpc /stargz/stargz-fuse-manager /usr/local/bin/
COPY entrypoint.sh /bin/alasio-node-entrypoint
ENTRYPOINT ["/bin/alasio-node-entrypoint"]
