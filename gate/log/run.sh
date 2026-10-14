#!/usr/bin/env bash
# The log gate, 03 and 04's: AutoMQ 1.7.4 on SeaweedFS 4.48 in Docker on the runner, with
# SCRAM-SHA-512 principals made at format and ACLs by Kafka's own tools, as 03's setup will
# make them; then @platformatic/kafka, the one client of alasio, the lake and the webhook
# intake, against it (gate/log/client.ts); and the collector's Kafka exporter, its export
# answered only once the log has it, and refused while the log is down.
#
#   OUT=<dir> gate/log/run.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
: "${OUT:?where to record}"
mkdir -p "$OUT/kraft" && chmod 777 "$OUT/kraft"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log"; }
AUTOMQ=automqinc/automq:1.7.4
SEAWEEDFS=chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d
COLLECTOR=otel/opentelemetry-collector-contrib:0.161.0@sha256:fd328de2552466ad78385e1b1289c3f2402b1c45f265b252aab1955b42845ac1
NODE=node:24.14.0-bookworm-slim

docker network create log >/dev/null
cat >"$OUT/s3.json" <<'EOF'
{"identities": [{"name": "log", "credentials": [{"accessKey": "log", "secretKey": "log-secret"}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
EOF
chmod 644 "$OUT/s3.json"
docker run -d --name s3 --network log -p 127.0.0.1:18333:8333 -v "$OUT/s3.json:/etc/s3.json:ro" "$SEAWEEDFS" \
  server -dir=/data -s3 -s3.config=/etc/s3.json -master.volumeSizeLimitMB=64 -volume.max=100 >/dev/null
export AWS_ACCESS_KEY_ID=log AWS_SECRET_ACCESS_KEY=log-secret AWS_DEFAULT_REGION=us-east-1
until aws --endpoint-url http://127.0.0.1:18333 s3 mb s3://log >/dev/null 2>&1; do sleep 2; done
log "SeaweedFS serves the log's bucket"

# Restricted, as alasio runs it: not root, its root file system read-only, /tmp and its volume alone written.
kafka_env=(--read-only --tmpfs /tmp:rw,exec,size=512m -e KAFKA_S3_ACCESS_KEY=log -e KAFKA_S3_SECRET_KEY=log-secret -e LOG_DIR=/tmp/logs
  -e KAFKA_HEAP_OPTS="-Xms1g -Xmx1g -XX:MetaspaceSize=96m -XX:MaxDirectMemorySize=1500m")
mounts=(-v "$OUT/kraft:/data" -v "$here/server.properties:/etc/kafka/overrides.properties:ro")
# AutoMQ's own configuration, ours over it (a later key wins).
config='cat /opt/automq/kafka/config/kraft/server.properties /etc/kafka/overrides.properties >/tmp/server.properties'
scram=()
for user in admin gateway intake scaler webhook alasio-main alasio-branch; do scram+=(--add-scram "SCRAM-SHA-512=[name=$user,password=$user-secret]"); done
docker run --rm --user 1000:1000 "${kafka_env[@]}" "${mounts[@]}" "$AUTOMQ" \
  bash -c "$config && exec /opt/automq/kafka/bin/kafka-storage.sh format -t R2F0ZUxvZ0dhdGUwMzA0 -c /tmp/server.properties \"\$@\"" format "${scram[@]}" | tee "$OUT/format.txt"
start_automq() {
  docker run -d --name automq --hostname automq --network log --user 1000:1000 --memory 3g "${kafka_env[@]}" "${mounts[@]}" "$AUTOMQ" \
    bash -c "$config && exec /opt/automq/kafka/bin/kafka-server-start.sh /tmp/server.properties" >/dev/null
}
cat >"$OUT/admin.properties" <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
EOF
chmod 644 "$OUT/admin.properties"
admin() { docker run --rm --network log -e KAFKA_HEAP_OPTS=-Xmx256m -e LOG_DIR=/tmp -v "$OUT/admin.properties:/etc/admin.properties:ro" "$AUTOMQ" "$@"; }
serving() { admin /opt/automq/kafka/bin/kafka-topics.sh --bootstrap-server automq:9092 --command-config /etc/admin.properties --list >/dev/null 2>&1; }
started=$(date +%s)
start_automq
until serving; do sleep 3; done
log "AutoMQ serves, SCRAM principals made at format, after $(($(date +%s) - started)) s"
for _ in $(seq 20); do
  ! docker run --rm --network log curlimages/curl:8.17.0 -fsS -m 5 http://automq:9464/metrics >"$OUT/metrics.txt" 2>/dev/null || break
  sleep 3
done
log "its metrics: $(grep -vc '^#' "$OUT/metrics.txt") samples, of $(grep '^# TYPE' "$OUT/metrics.txt" | awk '{print $3}' | sed 's/_[a-z]*$//' | sort -u | head -12 | tr '\n' ' ')"

topics() {
  for topic in otel.spans otel.logs otel.metrics telegram.main telegram.branch; do
    admin /opt/automq/kafka/bin/kafka-topics.sh --bootstrap-server automq:9092 --command-config /etc/admin.properties \
      --create --if-not-exists --topic "$topic" --partitions 4
  done
  acl() { admin /opt/automq/kafka/bin/kafka-acls.sh --bootstrap-server automq:9092 --command-config /etc/admin.properties --add "$@" >/dev/null; }
  acl --allow-principal User:gateway --operation Write --operation Describe --topic otel. --resource-pattern-type prefixed
  acl --allow-principal User:intake --operation Read --operation Describe --topic otel. --resource-pattern-type prefixed
  acl --allow-principal User:intake --operation Read --group lake-intake
  # KEDA's, which reads the intake's lag: its committed offsets, and the topics' ends.
  acl --allow-principal User:scaler --operation Describe --topic otel. --resource-pattern-type prefixed
  acl --allow-principal User:scaler --operation Describe --group lake-intake
  acl --allow-principal User:webhook --operation Write --operation Describe --topic telegram. --resource-pattern-type prefixed
  acl --allow-principal User:alasio-main --operation Read --operation Describe --topic telegram.main
  acl --allow-principal User:alasio-main --operation Read --group alasio.main
  # A principal made after format, as a branch environment's is.
  admin /opt/automq/kafka/bin/kafka-configs.sh --bootstrap-server automq:9092 --command-config /etc/admin.properties \
    --alter --add-config 'SCRAM-SHA-512=[password=branch-two-secret]' --entity-type users --entity-name alasio-branch-two
  acl --allow-principal User:alasio-branch-two --operation Read --operation Describe --topic telegram.branch
  acl --allow-principal User:alasio-branch-two --operation Read --group alasio.branch
}
topics
admin /opt/automq/kafka/bin/kafka-acls.sh --bootstrap-server automq:9092 --command-config /etc/admin.properties --list | tee "$OUT/acls.txt"
log "topics and ACLs made with Kafka's tools, and a principal after format"

# The client, against it.
docker run --rm --network log --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$repo:$repo" -w "$repo" "$NODE" node gate/log/client.ts | tee "$OUT/client.txt"

# The collector's gateway.
docker run -d --name collector --network log -p 127.0.0.1:14318:4318 -v "$here/collector.yaml:/etc/otelcol/config.yaml:ro" "$COLLECTOR" --config /etc/otelcol/config.yaml >/dev/null
span() { # <trace id>: the HTTP status of one span's export
  curl -s -o /dev/null -w '%{http_code}' -m 30 -H 'content-type: application/json' http://127.0.0.1:14318/v1/traces --data \
    "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"gate\"}}]},\"scopeSpans\":[{\"spans\":[{\"traceId\":\"$1\",\"spanId\":\"0102030405060708\",\"name\":\"gate\",\"kind\":1,\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}]}]}]}" || true
}
until [ "$(span 00000000000000000000000000000001)" = 200 ]; do sleep 2; done
log "the collector exports a span to otel.spans, answered 200"
records() { # <topic>: how many records it holds
  admin /opt/automq/kafka/bin/kafka-get-offsets.sh --bootstrap-server automq:9092 --command-config /etc/admin.properties --topic "$1" |
    awk -F: '{ sum += $3 } END { print sum + 0 }'
}
log "otel.spans holds $(records otel.spans) record(s)"
docker logs collector >"$OUT/collector-start.log" 2>&1

log "the log stopped: an export is refused, not answered 200"
docker stop -t 30 automq >/dev/null
docker rm automq >/dev/null
began=$(date +%s)
down=$(span 00000000000000000000000000000002)
log "export while the log is down: HTTP $down after $(($(date +%s) - began)) s"
[ "$down" != 200 ] || { log "FAILED: an export was answered 200 with the log down"; exit 1; }
start_automq
until serving; do sleep 3; done
log "AutoMQ serves again after $(($(date +%s) - began)) s"
until [ "$(span 00000000000000000000000000000003)" = 200 ]; do sleep 2; done
log "exports answered 200 again; otel.spans holds $(records otel.spans) record(s)"
docker logs collector >"$OUT/collector.log" 2>&1
docker logs automq >"$OUT/automq.log" 2>&1
log "passed"
