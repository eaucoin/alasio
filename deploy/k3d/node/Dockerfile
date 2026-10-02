# A k3s node for k3d with gVisor as a containerd runtime, `runsc`, so pods with the
# `gvisor` RuntimeClass run sandboxed: what alasio's session filesystems run under on a
# single machine and in CI. gVisor is pinned by release and verified by its sha512.
FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 AS gvisor
ARG GVISOR_RELEASE=20260921.0
ARG GVISOR_SHA512=66a5b17354690ec2e4037b54080eeabde55a1503779ceb823d73856283a6836953e105493d7e38fb2dcf85e5c86e0f48c66a9abb0244a78bd612df938702475c
ADD https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE}/x86_64/gvisor.tar.zstd /tmp/gvisor.tar.zstd
RUN echo "${GVISOR_SHA512}  /tmp/gvisor.tar.zstd" | sha512sum -c - \
 && apk add --no-cache zstd \
 && mkdir /gvisor \
 && zstd -dc /tmp/gvisor.tar.zstd | tar -x -C /gvisor \
 && test -x /gvisor/runsc -a -x /gvisor/containerd-shim-runsc-v1 -a -x /gvisor/gvisor-bin/gvisor_sentry

FROM rancher/k3s:v1.37.1-k3s1@sha256:ca7f37d993d82ef0dcdcfecb2e0e2618ea541dbaffc620c8cedebe01a82acd0d
# runsc and its shim, and the Sentry, which current gVisor runs as a sidecar binary.
COPY --from=gvisor /gvisor/runsc /gvisor/containerd-shim-runsc-v1 /bin/
COPY --from=gvisor /gvisor/gvisor-bin /bin/gvisor-bin
COPY config-v3.toml.tmpl /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl
