# The lake service's image (src/service.ts): Node, DuckDB's Node API, and the DuckDB
# extensions the lake loads, installed here so none is ever fetched at run time. The
# base is glibc Debian because DuckDB's Node bindings are built for glibc. Published as
# ghcr.io/eaucoin/alasio-lake by .github/workflows/release.yml.
FROM node:24.14.0-bookworm-slim@sha256:d8e448a56fc63242f70026718378bd4b00f8c82e78d20eefb199224a4d8e33d8
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's analytics lake: DuckLake loaded from alasio's Neon"

WORKDIR /opt/lake
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

# The extensions before the rest of the code, so a code change rebuilds only the last layer.
COPY src/extensions.ts src/install-extensions.ts src/
RUN node src/install-extensions.ts /opt/lake/extensions && chmod -R a+rX /opt/lake/extensions

COPY src src
# DuckDB's allocator, the jemalloc it is built with, gives back the pages it frees at once
# rather than over its default second: most of what a load, a pass or a query allocates is
# outside DuckDB's buffer manager, and so its memory limit, and the pages each batch freed
# were kept for the next. Measured in CI on a box-shaped lake's reload (its 468 MiB file of
# 160 641 entries lost), the process held 791-830 MiB at its peak, and 780-873 MiB reloading
# one twice the size; with this, 304-342 MiB, at a fifth to a third more CPU. A decay of
# 10, 100 or 250 ms bounds almost nothing (698-817 MiB): jemalloc purges decaying pages
# lazily, as it next allocates, and the next batch takes them first; so 0. A reader's dashboard
# queries hold 133-187 MiB rather than 179-242, at an eighth to a quarter more CPU and about
# the same time.
ENV LAKE_EXTENSION_DIRECTORY=/opt/lake/extensions NODE_ENV=production \
    DUCKDB_JE_MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0
CMD ["node", "src/service.ts"]
