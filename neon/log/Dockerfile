# The log as alasio runs it (cli/src/manifests/log.ts): Apache Kafka, and Prometheus's JMX
# exporter, the Java agent its metrics are served in Prometheus's format by, which the
# log's ConfigMap configures. Nothing is fetched at run time. Published as
# ghcr.io/eaucoin/alasio-log by .github/workflows/release.yml.
FROM apache/kafka:4.2.2@sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's log: Apache Kafka, carrying the stack's telemetry to its lake"

# The JMX exporter 1.7.0, as its checksum says it is.
ADD --chmod=644 --checksum=sha256:0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9 \
    https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/jmx_prometheus_javaagent-1.7.0.jar \
    /opt/jmx-exporter/jmx_prometheus_javaagent.jar
