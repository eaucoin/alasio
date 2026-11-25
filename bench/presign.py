#!/usr/bin/env python3
"""Spike: a SigV4 presigned URL, as S3 signs one for a query string: presign.py METHOD URL ACCESS SECRET EXPIRES [REGION]."""
import datetime, hashlib, hmac, sys, urllib.parse

def presign(method, url, access, secret, expires, region="us-east-1"):
    u = urllib.parse.urlsplit(url)
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    scope = f"{day}/{region}/s3/aws4_request"
    q = {"X-Amz-Algorithm": "AWS4-HMAC-SHA256", "X-Amz-Credential": f"{access}/{scope}", "X-Amz-Date": amz, "X-Amz-Expires": str(expires), "X-Amz-SignedHeaders": "host"}
    query = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted(q.items()))
    path = urllib.parse.quote(u.path, safe="/-_.~")
    canonical = "\n".join([method, path, query, f"host:{u.netloc}\n", "host", "UNSIGNED-PAYLOAD"])
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    k = ("AWS4" + secret).encode()
    for part in (day, region, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    return f"{u.scheme}://{u.netloc}{path}?{query}&X-Amz-Signature={sig}"

if __name__ == "__main__":
    m, url, a, s, e = sys.argv[1:6]
    print(presign(m, url, a, s, int(e), *(sys.argv[6:7] or [])))
