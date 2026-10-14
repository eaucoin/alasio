# The webhook intake's image (src/server.ts): Node and the one Kafka client, on a
# distroless base, as it is what the internet reaches: no shell, no package manager, not
# root. Published as ghcr.io/eaucoin/alasio-webhook by .github/workflows/release.yml.
FROM node:24.14.0-bookworm-slim@sha256:d8e448a56fc63242f70026718378bd4b00f8c82e78d20eefb199224a4d8e33d8 AS build
WORKDIR /opt/webhook
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY src src

FROM gcr.io/distroless/nodejs24-debian12:nonroot@sha256:14d42e2511532589a7c7e01a753667a74fcc96266e137e8125006b87b0c32d0a
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's webhook intake: Telegram's updates, checked, into the log"
WORKDIR /opt/webhook
COPY --from=build /opt/webhook /opt/webhook
ENV NODE_ENV=production
CMD ["src/server.ts"]
