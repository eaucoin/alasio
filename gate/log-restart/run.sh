#!/usr/bin/env bash
# The log's restart gate, 03's: which log keeps every record it acknowledged through its
# broker's restarts, as the collector's gateway produces to it. One broker in Docker on the
# runner, BROKER's (AutoMQ 1.7.4 on SeaweedFS 4.48; Apache Kafka 4.2.2 in KRaft, one node,
# on a volume of its own; or that Kafka with its segments tiered to SeaweedFS 4.48, and
# then the filer store put back under it and the log made anew, gate/log-restart/restore.ts),
# the collector's gateway producing to it with PRODUCER's
# producer (idempotent, acks=all; or plain, acks=1, which franz-go runs without
# idempotence); then ROUNDS rounds (gate/log-restart/rounds.ts), each its spans exported
# one at a time, the broker stopped one of three ways in turn (with its grace, as a pod
# deleted is; cut short after 2 s, as a pod deleted at once is; killed, as a process
# crashing is) and started again, and the exports gone on until 20 are taken again; then
# the topic read whole and counted: each span taken and not there, lost; each there twice,
# a duplicate. The broker's memory is sampled throughout.
#
#   BROKER=automq|kafka|kafka-tiered PRODUCER=idempotent|plain [ROUNDS=18] OUT=<dir> gate/log-restart/run.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
: "${BROKER:?automq or kafka}" "${PRODUCER:?idempotent or plain}" "${OUT:?where to record}"
ROUNDS=${ROUNDS:-18}
mkdir -p "$OUT/data" && chmod 777 "$OUT/data"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log"; }
AUTOMQ=automqinc/automq:1.7.4
KAFKA=apache/kafka:4.2.2@sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69
SEAWEEDFS=chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d
COLLECTOR=otel/opentelemetry-collector-contrib:0.161.0@sha256:fd328de2552466ad78385e1b1289c3f2402b1c45f265b252aab1955b42845ac1

TIERED_STORAGE=https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download/v1.1.1
TIERED_CORE=053480aa4ea5d3b667df824b4c4425f469243df3c89fa8c50edbf9ec88300e4f
TIERED_S3=3e0ea6ce17033c1fcf74e0f6f292411cb3e2afcc1798b51125bb15e5b7500017
export AWS_ACCESS_KEY_ID=log AWS_SECRET_ACCESS_KEY=log-secret AWS_DEFAULT_REGION=us-east-1
s3() { aws --endpoint-url http://127.0.0.1:18333 "$@"; }

# SeaweedFS, as alasio bundles it, its bucket `bucket` made.
object_store() {
  cat >"$OUT/s3.json" <<'EOF'
{"identities": [{"name": "log", "credentials": [{"accessKey": "log", "secretKey": "log-secret"}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
EOF
  chmod 644 "$OUT/s3.json"
  docker run -d --name s3 --network log -p 127.0.0.1:18333:8333 -v "$OUT/s3.json:/etc/s3.json:ro" "$SEAWEEDFS" \
    server -dir=/data -s3 -s3.config=/etc/s3.json -master.volumeSizeLimitMB=64 -volume.max=100 >/dev/null
  until s3 s3 mb "s3://$1" >/dev/null 2>&1; do sleep 2; done
}

# Apache Kafka, one node, combined, its log on a volume of its own; every record acked by
# the one replica there is; with `extra` of its settings.
kafka_broker() {
  docker run -d --name broker --hostname broker --network log -p 127.0.0.1:19092:19092 --memory 3g -v "$OUT/data:/var/lib/kafka/data" \
    -e CLUSTER_ID=R2F0ZUxvZ1Jlc3RhcnQwMw -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=INTERNAL://:9092,EXTERNAL://:19092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://broker:9092,EXTERNAL://localhost:19092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@broker:9093 -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_MIN_INSYNC_REPLICAS=1 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false -e KAFKA_HEAP_OPTS="-Xms512m -Xmx512m" "$@" "$KAFKA" >/dev/null
}

topic_configs=""
docker network create log >/dev/null
case "$BROKER" in
  automq)
    object_store log
    # As 03 runs it: not root, its root file system read-only, /tmp and its volume alone written; the tier's heap and direct memory.
    broker=(--read-only --tmpfs /tmp:rw,exec,size=512m --user 1000:1000 -e KAFKA_S3_ACCESS_KEY=log -e KAFKA_S3_SECRET_KEY=log-secret -e LOG_DIR=/tmp/logs
      -e KAFKA_HEAP_OPTS="-Xms1g -Xmx1g -XX:MetaspaceSize=96m -XX:MaxDirectMemorySize=1500m" --memory 3g
      -v "$OUT/data:/data" -v "$here/automq.properties:/etc/kafka/overrides.properties:ro" "$AUTOMQ")
    config='cat /opt/automq/kafka/config/kraft/server.properties /etc/kafka/overrides.properties >/tmp/server.properties'
    docker run --rm "${broker[@]}" bash -c "$config && exec /opt/automq/kafka/bin/kafka-storage.sh format -t R2F0ZUxvZ1Jlc3RhcnQwMw -c /tmp/server.properties" >"$OUT/format.txt"
    docker run -d --name broker --hostname broker --network log -p 127.0.0.1:19092:19092 "${broker[@]}" \
      bash -c "$config && exec /opt/automq/kafka/bin/kafka-server-start.sh /tmp/server.properties" >/dev/null
    tools=/opt/automq/kafka/bin
    ;;
  kafka)
    kafka_broker
    tools=/opt/kafka/bin
    ;;
  kafka-tiered)
    # Kafka's tiered storage (KIP-405), Aiven's RemoteStorageManager for S3 writing the
    # segments it rolls to the bundled SeaweedFS; a segment rolled each 10 s, and gone
    # from the volume 15 s after, once in the tier, so the rounds' are read from it.
    object_store tier
    mkdir -p "$OUT/../tiered-storage"
    for part in core:$TIERED_CORE s3:$TIERED_S3; do
      name=${part%%:*}
      curl -fsSL -o "$OUT/../tiered-storage/$name.tgz" "$TIERED_STORAGE/$name-1.1.1.tgz"
      echo "${part#*:}  $OUT/../tiered-storage/$name.tgz" | sha256sum --check --quiet
      tar -xzf "$OUT/../tiered-storage/$name.tgz" -C "$OUT/../tiered-storage"
    done
    kafka_broker -v "$(cd "$OUT/../tiered-storage" && pwd):/opt/tiered-storage:ro" \
      -e KAFKA_REMOTE_LOG_STORAGE_SYSTEM_ENABLE=true -e KAFKA_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS=5000 -e KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=5000 \
      -e KAFKA_REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME=INTERNAL \
      -e KAFKA_RLMM_CONFIG_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_RLMM_CONFIG_REMOTE_LOG_METADATA_TOPIC_NUM_PARTITIONS=1 \
      -e KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_NAME=io.aiven.kafka.tieredstorage.RemoteStorageManager \
      -e 'KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH=/opt/tiered-storage/core-1.1.1/*:/opt/tiered-storage/s3-1.1.1/*' \
      -e KAFKA_RSM_CONFIG_CHUNK_SIZE=4194304 \
      -e KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_CLASS=io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache -e KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_SIZE=67108864 \
      -e KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS=io.aiven.kafka.tieredstorage.storage.s3.S3Storage \
      -e KAFKA_RSM_CONFIG_STORAGE_S3_ENDPOINT_URL=http://s3:8333 -e KAFKA_RSM_CONFIG_STORAGE_S3_BUCKET_NAME=tier -e KAFKA_RSM_CONFIG_STORAGE_S3_REGION=us-east-1 \
      -e KAFKA_RSM_CONFIG_STORAGE_S3_PATH_STYLE_ACCESS_ENABLED=true \
      -e KAFKA_RSM_CONFIG_STORAGE_AWS_ACCESS_KEY_ID=log -e KAFKA_RSM_CONFIG_STORAGE_AWS_SECRET_ACCESS_KEY=log-secret
    tools=/opt/kafka/bin
    topic_configs="remote.storage.enable=true local.retention.ms=15000 segment.ms=10000"
    ;;
  *) log "BROKER is automq, kafka or kafka-tiered, not $BROKER"; exit 1 ;;
esac
kafka() { docker exec broker env KAFKA_HEAP_OPTS=-Xmx256m LOG_DIR=/tmp "$tools/$1" --bootstrap-server broker:9092 "${@:2}"; }
until kafka kafka-topics.sh --list >/dev/null 2>&1; do sleep 3; done
configs=()
for config in $topic_configs; do configs+=(--config "$config"); done
kafka kafka-topics.sh --create --if-not-exists --topic otel.spans --partitions 2 "${configs[@]}" >/dev/null
log "$BROKER serves otel.spans${topic_configs:+, with $topic_configs}"

# The broker's memory, every 5 s: what its cgroup charges, and of that its anonymous memory, resident and not of files.
(
  while sleep 5; do
    used=$(docker stats --no-stream --format '{{.MemUsage}}' broker 2>/dev/null | cut -d/ -f1 | tr -d ' ') || continue
    anon=$(docker exec broker sh -c "awk '\$1 == \"anon\" {print \$2}' /sys/fs/cgroup/memory.stat" 2>/dev/null) || continue
    printf '%s %s %s\n' "$(date -u +%H:%M:%S)" "$used" "$anon" >>"$OUT/memory.txt"
  done
) &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

case "$PRODUCER" in
  idempotent) acks=-1 ;;
  plain) acks=1 ;;
  *) log "PRODUCER is idempotent or plain, not $PRODUCER"; exit 1 ;;
esac
sed "s/@ACKS@/$acks/" "$here/collector.yaml" >"$OUT/collector.yaml"
chmod 644 "$OUT/collector.yaml"
docker run -d --name collector --network log -p 127.0.0.1:14318:4318 -v "$OUT/collector.yaml:/etc/otelcol/config.yaml:ro" "$COLLECTOR" --config /etc/otelcol/config.yaml >/dev/null
log "the collector's gateway produces with acks $acks"

ROUNDS=$ROUNDS OUT=$OUT TIERED=$([ "$BROKER" = kafka-tiered ] && echo 1 || echo 0) node "$repo/gate/log-restart/rounds.ts"
if [ "$BROKER" = kafka-tiered ]; then
  OUT=$OUT KAFKA_IMAGE=$KAFKA TOPIC_CONFIGS=$topic_configs node "$repo/gate/log-restart/restore.ts"
fi
docker logs collector >"$OUT/collector.log" 2>&1
docker logs broker >"$OUT/broker.log" 2>&1
log "memory: the broker's cgroup at most $(awk '{print $2}' "$OUT/memory.txt" | sort -h | tail -n 1), its anonymous memory at most $(awk '{print $3}' "$OUT/memory.txt" | sort -n | tail -n 1 | numfmt --to=iec-i)"
