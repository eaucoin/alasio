#!/usr/bin/env bash
# Spike: whether per-object presigned GET URLs serve a restore's reads: GET, ranged GET, HEAD, If-Match and expiry,
# on SeaweedFS's S3 gateway and on another S3 store, for direct reads (M7 step 2).
#   bench/presigned-urls.sh seaweedfs VERSION | minio
set -uo pipefail
kind=$1; v=${2:-}
here=$(cd "$(dirname "$0")" && pwd)
docker rm -f s3 >/dev/null 2>&1
data=$(mktemp -d)
if [ "$kind" = seaweedfs ]; then
  cat >"$data/s3.json" <<'EOF'
{"identities":[{"name":"admin","credentials":[{"accessKey":"ADMIN","secretKey":"adminsecret"}],"actions":["Admin","Read","Write","List","Tagging"]}]}
EOF
  mkdir -p "$data/vol"
  docker run -d --name s3 -p 8333:8333 -v "$data:/cfg" -v "$data/vol:/data" --entrypoint weed "chrislusf/seaweedfs:$v" server -dir=/data -s3 -s3.port=8333 -ip.bind=0.0.0.0 -s3.config=/cfg/s3.json >/dev/null
  end=http://127.0.0.1:8333
else
  docker run -d --name s3 -p 8333:9000 -e MINIO_ROOT_USER=ADMIN -e MINIO_ROOT_PASSWORD=adminsecret quay.io/minio/minio server /data >/dev/null
  end=http://127.0.0.1:8333
fi
for _ in $(seq 120); do [ "$(curl -s -o /dev/null -w '%{http_code}' $end/)" != 000 ] && break; sleep 1; done
sleep 3
echo "=== $kind $v: answers $(curl -s -o /dev/null -w '%{http_code}' $end/)"
signed() { curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 aws:amz:us-east-1:s3 --user ADMIN:adminsecret "$@"; }
echo "make bucket $(signed -X PUT $end/snapshots)"
head -c 1048576 /dev/urandom >"$data/obj"
echo "put $(signed -X PUT --data-binary @"$data/obj" $end/snapshots/main/ws1/layers/a/pages.img)"
etag=$(curl -s -D - -o /dev/null --aws-sigv4 aws:amz:us-east-1:s3 --user ADMIN:adminsecret -I $end/snapshots/main/ws1/layers/a/pages.img | sed -n 's/^[Ee][Tt]ag: *//p' | tr -d '\r')
echo "etag $etag"
url=$("$here/presign.py" GET "$end/snapshots/main/ws1/layers/a/pages.img" ADMIN adminsecret 60)
get() { curl -s -o "$data/out" -w '%{http_code} %{size_download}' "$@"; }
echo "presigned GET: $(get "$url") bytes, same as put: $(cmp -s "$data/out" "$data/obj" && echo yes || echo no)"
echo "presigned ranged GET 4096-8191: $(get -H 'Range: bytes=4096-8191' "$url")"
echo "presigned GET If-Match right: $(get -H "If-Match: $etag" "$url")"
echo "presigned GET If-Match wrong: $(get -H 'If-Match: \"0123\"' "$url")"
echo "presigned GET If-Match right, ranged: $(get -H "If-Match: $etag" -H 'Range: bytes=0-4095' "$url")"
echo "HEAD with the GET URL: $(curl -s -o /dev/null -w '%{http_code}' -I "$url")"
hurl=$("$here/presign.py" HEAD "$end/snapshots/main/ws1/layers/a/pages.img" ADMIN adminsecret 60)
echo "HEAD with a HEAD URL: $(curl -s -o /dev/null -w '%{http_code}' -I "$hurl")"
other=${url/layers\/a\/pages.img/layers\/b\/pages.img}
echo "the URL's signature on another key: $(get "$other")"
echo "PUT with the GET URL: $(curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary @"$data/obj" "$url")"
short=$("$here/presign.py" GET "$end/snapshots/main/ws1/layers/a/pages.img" ADMIN adminsecret 2)
echo "2 s URL at once: $(get "$short")"
sleep 4
echo "2 s URL after 4 s: $(get "$short")"
# A GET under way as its URL expires: 64 MiB, read slowly.
head -c 67108864 /dev/urandom >"$data/big"
signed -X PUT --data-binary @"$data/big" $end/snapshots/main/ws1/layers/a/big.img >/dev/null
slow=$("$here/presign.py" GET "$end/snapshots/main/ws1/layers/a/big.img" ADMIN adminsecret 3)
echo "a GET slower than its URL's 3 s: $(curl -s -o "$data/out" -w '%{http_code} %{size_download} in %{time_total}s' --limit-rate 8M "$slow")"
docker rm -f s3 >/dev/null 2>&1
