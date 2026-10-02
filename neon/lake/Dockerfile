# The lake service's image (see README.md): Node, DuckDB's Node API, and the DuckDB
# extensions the lake loads, installed here so none is ever fetched at run time. The
# base is glibc Debian because DuckDB's Node bindings are built for glibc. Built by
# alasio as alasio-neon-lake:<revision of this directory> (src/neon/stack.js).
FROM node:24.14.0-bookworm-slim@sha256:d8e448a56fc63242f70026718378bd4b00f8c82e78d20eefb199224a4d8e33d8
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's analytics lake: DuckLake loaded from alasio's Neon"

WORKDIR /opt/lake
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

# The extensions before the rest of the code, so a code change rebuilds only the last layer.
COPY src/extensions.js src/install-extensions.js src/
RUN node src/install-extensions.js /opt/lake/extensions && chmod -R a+rX /opt/lake/extensions

COPY src src
ENV LAKE_EXTENSION_DIRECTORY=/opt/lake/extensions NODE_ENV=production
CMD ["node", "src/service.js"]
