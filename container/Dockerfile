# The image alasio-standalone.service runs the bot in. It supplies only the
# operating system: alasio, Node, and everything else the bot and its agents use
# come from the host's /home/operator, which container/run.sh mounts at the same
# path. The packages are this host's own userland (Ubuntu 24.04), so commands
# an agent runs behave as they do on the host.
FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      acl bash-completion bind9-dnsutils build-essential busybox-static byobu \
      ca-certificates cpio curl diffutils dirmngr ed file findutils gnupg git \
      htop inetutils-telnet iproute2 iputils-ping iputils-tracepath jq less \
      locales lsb-release lsof mtr-tiny nano net-tools netbase netcat-openbsd \
      numactl openssh-client patch pciutils procps psmisc python3 rsync screen \
      sensible-utils strace sysstat systemd time tmux tzdata uuid-runtime vim \
      wget xvfb xz-utils zstd \
      fonts-freefont-ttf fonts-ipafont-gothic fonts-liberation \
      fonts-noto-color-emoji fonts-tlwg-loma-otf fonts-unifont fonts-wqy-zenhei \
      xfonts-cyrillic xfonts-scalable \
      libasound2t64 libatk-bridge2.0-0t64 libatk1.0-0t64 libatspi2.0-0t64 \
      libcairo2 libcups2t64 libdbus-1-3 libdrm2 libfontconfig1 libfreetype6 \
      libgbm1 libglib2.0-0t64 libicu74 libnspr4 libnss3 libpango-1.0-0 \
      libssl3t64 libx11-6 libxcb1 libxcomposite1 libxdamage1 libxext6 \
      libxfixes3 libxkbcommon0 libxrandr2 \
 && sed -i 's/^# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
 && locale-gen \
 && rm -rf /var/lib/apt/lists/*

# The host's Google Cloud CLI, which run.sh mounts at /usr/lib/google-cloud-sdk.
RUN ln -s ../lib/google-cloud-sdk/bin/gcloud /usr/bin/gcloud \
 && ln -s ../lib/google-cloud-sdk/bin/gsutil /usr/bin/gsutil \
 && ln -s ../lib/google-cloud-sdk/bin/bq /usr/bin/bq
