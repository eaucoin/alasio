#!/usr/bin/env bash
# Spike: whether SeaweedFS's S3 gateway takes identities made and removed at runtime that read one key prefix of a
# bucket alone, and how soon each change holds, for direct reads of a restore's images (M7 step 2).
#   bench/seaweed-identities.sh VERSION
set -uo pipefail
v=$1
net=sw-$v
docker network create "$net" >/dev/null
data=$(mktemp -d)
cat >"$data/s3.json" <<'EOF'
{"identities":[{"name":"admin","credentials":[{"accessKey":"ADMIN","secretKey":"adminsecret"}],"actions":["Admin","Read","Write","List","Tagging"]}]}
EOF
for mode in static filer; do
  echo "=== $v, admin in a static -s3.config: $([ $mode = static ] && echo yes || echo no)"
  docker rm -f sw >/dev/null 2>&1
  args=(server -dir=/tmp/data -s3 -s3.port=8333 -ip=sw -ip.bind=0.0.0.0)
  [ $mode = static ] && args+=(-s3.config=/cfg/s3.json)
  docker run -d --name sw --network "$net" -p 8333:8333 -v "$data:/cfg" --entrypoint weed "chrislusf/seaweedfs:$v" "${args[@]}" >/dev/null
  for _ in $(seq 120); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8333/)" != 000 ] && break; sleep 1; done
  sleep 5
  echo "gateway answers $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8333/)"; docker logs sw 2>&1 | tail -n 5
  shell() { printf '%s\n' "$1" | docker run --rm -i --network "$net" --entrypoint weed "chrislusf/seaweedfs:$v" shell -master=sw:9333 -filer=sw:8888 2>&1 | tail -n 8; }
  [ $mode = filer ] && shell 's3.configure -user=admin -access_key=ADMIN -secret_key=adminsecret -actions=Admin,Read,Write,List,Tagging -apply'
  sleep 2
  req() { # user secret method key [range]
    local extra=(); [ -n "${5:-}" ] && extra=(-H "Range: $5")
    curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" --user "$1:$2" -X "$3" "${extra[@]}" "http://127.0.0.1:8333/snapshots/$4" ${6:+--data-binary @$6}
  }
  head -c 1048576 /dev/urandom >"$data/obj"
  echo "admin: make bucket $(curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 aws:amz:us-east-1:s3 --user ADMIN:adminsecret -X PUT http://127.0.0.1:8333/snapshots)"
  for k in main/ws1/layers/a/pages.img main/ws2/layers/b/pages.img main/ws1/manifests/m.json; do
    echo "admin: put $k $(curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 aws:amz:us-east-1:s3 --user ADMIN:adminsecret -X PUT --data-binary @"$data/obj" "http://127.0.0.1:8333/snapshots/$k")"
  done
  for form in 'Read:snapshots/main/ws1/layers/*' 'Read:snapshots/main/ws1/layers/' 'Read:snapshots/main/ws1/layers'; do
    echo "--- identity r1, actions $form"
    t0=$(date +%s.%N)
    shell "s3.configure -user=r1 -access_key=R1KEY -secret_key=r1secret -actions=$form -apply"
    for _ in $(seq 100); do [ "$(req R1KEY r1secret GET main/ws1/layers/a/pages.img)" = 200 ] && break; sleep 0.1; done
    echo "first 200 after $(echo "$(date +%s.%N) - $t0" | bc) s"
    echo "GET own prefix $(req R1KEY r1secret GET main/ws1/layers/a/pages.img)"
    echo "ranged GET own prefix $(req R1KEY r1secret GET main/ws1/layers/a/pages.img 'bytes=4096-8191')"
    echo "HEAD own prefix $(req R1KEY r1secret HEAD main/ws1/layers/a/pages.img)"
    echo "GET other workspace $(req R1KEY r1secret GET main/ws2/layers/b/pages.img)"
    echo "GET own workspace outside layers/ $(req R1KEY r1secret GET main/ws1/manifests/m.json)"
    echo "PUT own prefix $(req R1KEY r1secret PUT main/ws1/layers/a/x "" "$data/obj")"
    echo "DELETE own prefix $(req R1KEY r1secret DELETE main/ws1/layers/a/pages.img)"
    echo "list bucket $(curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 aws:amz:us-east-1:s3 --user R1KEY:r1secret 'http://127.0.0.1:8333/snapshots?list-type=2&prefix=main/ws1/')"
    t0=$(date +%s.%N)
    shell "s3.configure -user=r1 -delete -apply"
    for _ in $(seq 100); do [ "$(req R1KEY r1secret GET main/ws1/layers/a/pages.img)" != 200 ] && break; sleep 0.1; done
    echo "refused ($(req R1KEY r1secret GET main/ws1/layers/a/pages.img)) after $(echo "$(date +%s.%N) - $t0" | bc) s of its removal"
  done
  echo "--- the identities file the filer holds"
  shell 's3.configure' | tail -n 30
  docker logs sw 2>&1 | grep -iE 'identit|iam|config' | tail -n 10
done
docker rm -f sw >/dev/null 2>&1
