#!/usr/bin/env bash
# What the bundled SeaweedFS answers to the copies AutoMQ's compaction makes
# (s3stream's MultiPartWriter.copyWrite: UploadPartCopy of ranges of at least 5 MiB, then
# CompleteMultipartUpload with each part's ETag as the copy answered it,
# CompletedPart.eTag(copyPartResult.eTag())): the ETags it writes, which S3 quotes and
# seaweedfs#11622 reports bare; and whether an object assembled from copied parts, its
# upload completed with those ETags as answered and then quoted as S3 writes them, reads
# back byte for byte. With the gate's identity, on its bucket; what it saw goes to
# $OUT/etag.txt.
#
#   OUT=/tmp/gate-03 gate/03/etag.sh
set -euo pipefail

OUT=${OUT:-$RUNNER_TEMP/gate-03}
mkdir -p "$OUT"
k() { kubectl -n alasio "$@"; }
AWS_ACCESS_KEY_ID=$(k get secret gate-s3 -o jsonpath='{.data.accessKey}' | base64 -d)
AWS_SECRET_ACCESS_KEY=$(k get secret gate-s3 -o jsonpath='{.data.secretKey}' | base64 -d)
BUCKET=$(k get secret gate-s3 -o jsonpath='{.data.bucket}' | base64 -d)
S3=http://$(k get svc alasio-seaweedfs -o jsonpath='{.spec.clusterIP}'):8333
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=us-east-1
s3api() { aws --endpoint-url "$S3" s3api "$@"; }
say() { echo "$*" | tee -a "$OUT/etag.txt"; }

head -c 12000000 /dev/urandom >"$OUT/etag-source.bin"
s3api put-object --bucket "$BUCKET" --key etag/source --body "$OUT/etag-source.bin" >/dev/null

# The raw XML of a copy's answer: the CLI's parsed value loses the quotes.
raw_etag() { grep -o '<ETag>[^<]*</ETag>' | head -1 | sed -e 's/<ETag>//' -e 's/<\/ETag>//' -e 's/&#34;/"/g' -e 's/&quot;/"/g'; }

assemble() { # <key> <quote: yes|no>: an object of two copied parts, completed with their ETags
  local upload first second parts how="as answered"
  upload=$(s3api create-multipart-upload --bucket "$BUCKET" --key "$1" --query UploadId --output text)
  first=$(s3api upload-part-copy --bucket "$BUCKET" --key "$1" --upload-id "$upload" --part-number 1 \
    --copy-source "$BUCKET/etag/source" --copy-source-range bytes=0-5999999 --debug 2>&1 | raw_etag)
  second=$(s3api upload-part-copy --bucket "$BUCKET" --key "$1" --upload-id "$upload" --part-number 2 \
    --copy-source "$BUCKET/etag/source" --copy-source-range bytes=6000000-11999999 --debug 2>&1 | raw_etag)
  say "$1: UploadPartCopy answered ETags $first and $second (part 1's MD5: $(head -c 6000000 "$OUT/etag-source.bin" | md5sum | cut -c 1-32))"
  if [ "$2" = yes ]; then first="\"${first//\"/}\""; second="\"${second//\"/}\""; how=quoted; fi
  parts=$(jq -cn --arg a "$first" --arg b "$second" '{Parts: [{PartNumber: 1, ETag: $a}, {PartNumber: 2, ETag: $b}]}')
  if s3api complete-multipart-upload --bucket "$BUCKET" --key "$1" --upload-id "$upload" --multipart-upload "$parts" >"$OUT/etag-complete.json" 2>&1; then
    s3api get-object --bucket "$BUCKET" --key "$1" "$OUT/etag-read.bin" >/dev/null
    if cmp -s "$OUT/etag-source.bin" "$OUT/etag-read.bin"; then
      say "$1: completed with the ETags $how, reads back byte for byte"
    else
      say "$1: completed with the ETags $how, but reads back CHANGED"
    fi
  else
    say "$1: CompleteMultipartUpload with the ETags $how refused: $(tr '\n' ' ' <"$OUT/etag-complete.json")"
  fi
}
assemble etag/answered no
assemble etag/quoted yes
copied=$(s3api copy-object --bucket "$BUCKET" --key etag/copy --copy-source "$BUCKET/etag/source" --debug 2>&1 | raw_etag)
say "CopyObject answered ETag $copied (the source's MD5: $(md5sum <"$OUT/etag-source.bin" | cut -c 1-32))"
s3api delete-objects --bucket "$BUCKET" --delete '{"Objects":[{"Key":"etag/source"},{"Key":"etag/copy"},{"Key":"etag/answered"},{"Key":"etag/quoted"}]}' >/dev/null
