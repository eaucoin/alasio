# alasio's image: the bot, neon-control, and the Neon setup job, on Node; and the node agent,
# which sets workspaces' quotas with JuiceFS's own command, of the release of JuiceFS the
# nodes mount workspace storage with (cli/src/manifests/config.ts mountImage).
#
# The base is a full Ubuntu userland rather than a slim one because, with the host
# profile, the harnesses work on the operator's own machine from inside this
# container: Codex runs its shell commands here, so the commands an agent runs should
# behave as they do on an ordinary Ubuntu host. Published as ghcr.io/eaucoin/alasio by
# .github/workflows/release.yml.
FROM ubuntu:24.04@sha256:a853f94d226358a79c740cfc7bce0c289748f3fe3488d921d038ccd752c61b60
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio: the bot, neon-control and the Neon setup job"

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      acl bash-completion bind9-dnsutils build-essential ca-certificates cpio curl \
      diffutils dirmngr ed file findutils git gnupg htop iproute2 iputils-ping jq less \
      locales lsb-release lsof nano net-tools netbase netcat-openbsd openssh-client openssl \
      patch procps psmisc python3 rsync screen strace sysstat time tini tmux tzdata \
      unzip uuid-runtime vim wget xz-utils zstd \
 && sed -i 's/^# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
 && locale-gen \
 && rm -rf /var/lib/apt/lists/*
ENV LANG=en_US.UTF-8

# Node, verified against the release's published checksum.
ARG NODE_VERSION=24.14.0
ARG NODE_SHA256=41cd79bb7877c81605a9e68ec4c91547774f46a40c67a17e34d7179ef11729df
RUN curl -fsSLo /tmp/node.tar.xz "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
 && echo "${NODE_SHA256}  /tmp/node.tar.xz" | sha256sum -c - \
 && tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1 --no-same-owner \
 && rm /tmp/node.tar.xz \
 && node --version

# JuiceFS's command, its own release's static build, of the mount image's version, verified
# against the release's published checksum, and checked to be that version.
ARG JUICEFS_VERSION=1.4.1
ARG JUICEFS_SHA256=01ee09a21a9351a465e09906f113845e1c6a19bea70f530e5e2b2125b2dd3b82
RUN curl -fsSLo /tmp/juicefs.tar.gz "https://github.com/juicedata/juicefs/releases/download/v${JUICEFS_VERSION}/juicefs-${JUICEFS_VERSION}-linux-amd64.tar.gz" \
 && echo "${JUICEFS_SHA256}  /tmp/juicefs.tar.gz" | sha256sum -c - \
 && tar -xzf /tmp/juicefs.tar.gz -C /usr/local/bin --no-same-owner juicefs \
 && rm /tmp/juicefs.tar.gz \
 && HOME=/tmp juicefs version | grep -F "juicefs version ${JUICEFS_VERSION}+"

WORKDIR /opt/alasio
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY src src
COPY neon/control neon/control

ENV NODE_ENV=production
# The commit the image was built from, which alasio's telemetry says (src/telemetry/start.ts):
# none where the build is given none.
ARG REVISION=
ENV ALASIO_REVISION=${REVISION}
# tini reaps what the harnesses leave behind and passes SIGTERM on, so alasio shuts
# down cleanly when its pod stops.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["node", "/opt/alasio/src/index.ts"]
