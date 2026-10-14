# The stack's collector as alasio runs it (cli/src/manifests/collector.ts): the OpenTelemetry
# Collector, built by its builder of the components alasio configures alone (builder.yaml), on
# franz-go v1.22.2, which the contrib release of these components does not have yet. An
# idempotent producer of franz-go v1.22.1 and before answers a record as written that it has
# not written, where it failed the batch before, its context done, after the broker had
# appended it (franz-go #1484): the gateway would answer as taken telemetry the log never
# holds. Its test (acked_test.go) proves the build of that, on Kafka as franz-go fakes it,
# before the build is kept; and the build fails once the components resolve the fix of their
# own, to say that builder.yaml's replaces, and this image, are to go for contrib's. Nothing is
# fetched at run time. Published as ghcr.io/eaucoin/alasio-collector by
# .github/workflows/release.yml.
#
# alasio owns this image's rebuilds for its CVEs, as its base images' and its modules' come,
# as it does its other images'.
#
# Its build is what builder.yaml and these digests pin: Go's module graph is its minimal
# versions, a function of the versions named, each module checked against Go's checksum
# database as it is fetched, so no go.sum of its own is kept.
FROM golang:1.26.9-trixie@sha256:f06b7122f888680048afb247c4e5f49c0069eb6c4c1ccae0f3f4834527a8d22c AS build
WORKDIR /build
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.161.0
COPY builder.yaml acked_test.go ./
RUN builder --config builder.yaml --skip-compilation
# What franz-go the components resolve without builder.yaml's replaces: past v1.22.1, they
# have the fix themselves.
RUN cp -r otelcol unreplaced \
 && cd unreplaced \
 && go mod edit -dropreplace=github.com/twmb/franz-go -dropreplace=github.com/twmb/franz-go/pkg/kmsg \
 && resolved=$(GOFLAGS=-mod=mod go list -m -f '{{.Version}}' github.com/twmb/franz-go) \
 && if [ "$(printf '%s\n' v1.22.2 "$resolved" | sort -V | head -n 1)" = v1.22.2 ]; then \
      echo "the components resolve franz-go $resolved, which has the fix of franz-go #1484: drop builder.yaml's replaces, and this build, for contrib's image" >&2; exit 1; \
    fi
# The test on a copy of the build's module, which it alone adds Kafka's fake to.
RUN cp -r otelcol test && cp acked_test.go test/ \
 && cd test \
 && go get github.com/twmb/franz-go/pkg/kfake@v0.0.0-20261013022759-7b6cac30a57a go.opentelemetry.io/collector/exporter/exportertest@v0.161.0 \
 && go test -count=1 -run TestAnExportAnsweredAsTakenIsInTheTopic . \
 && cd ../otelcol \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /otelcol .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's collector: the OpenTelemetry Collector, carrying the stack's telemetry to its log"
COPY --from=build /otelcol /otelcol
ENTRYPOINT ["/otelcol"]
