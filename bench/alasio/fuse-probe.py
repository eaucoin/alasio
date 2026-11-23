#!/usr/bin/env python3
"""Spike: the latency of open(2) and close(2), as the gVisor gofer's directfs makes them (read-only,
non-blocking, no symlinks followed), of files on the containers' root filesystems (overlays, whose
lower layers stargz-snapshotter may serve through FUSE) beside those of the runner's own disk."""
import json, os, statistics, sys, time

def mounts():
    out = []
    for line in open("/proc/self/mounts"):
        dev, point, fstype, opts = line.split()[:4]
        out.append((point.replace("\\040", " "), fstype, opts))
    return out

MOUNTS = mounts()

def fstype_of(path):
    best = ("", "?")
    for point, fstype, _ in MOUNTS:
        if (path == point or path.startswith(point.rstrip("/") + "/")) and len(point) > len(best[0]):
            best = (point, fstype)
    return best[1]

def files_under(root, n):
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for name in sorted(filenames):
            p = os.path.join(dirpath, name)
            if os.path.isfile(p) and not os.path.islink(p):
                found.append(p)
                if len(found) >= n:
                    return found
    return found

def probe(root, n=200):
    files = files_under(root, n)
    opens, closes = [], []
    for p in files:
        t0 = time.perf_counter()
        try:
            fd = os.open(p, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
        except OSError:
            continue
        t1 = time.perf_counter()
        os.close(fd)
        t2 = time.perf_counter()
        opens.append((t1 - t0) * 1e6)
        closes.append((t2 - t1) * 1e6)
    def s(xs):
        return {"n": len(xs), "p50_us": round(statistics.median(xs), 1), "max_us": round(max(xs), 1)} if xs else None
    return {"open": s(opens), "close": s(closes)}

result = {"overlays": [], "fuse": [], "disk": {}}
for point, fstype, opts in MOUNTS:
    if fstype == "overlay" and point.endswith("/rootfs"):
        lowers = [l for o in opts.split(",") if o.startswith("lowerdir=") for l in o[len("lowerdir="):].split(":")]
        entry = {"rootfs": point, "lower_fstypes": sorted({fstype_of(l) for l in lowers})}
        entry["cold"] = probe(point)
        entry["warm"] = probe(point)
        result["overlays"].append(entry)
for point, fstype, _ in MOUNTS:
    if fstype.startswith("fuse") and len(result["fuse"]) < 3:
        result["fuse"].append({"mount": point, "fstype": fstype, "cold": probe(point), "warm": probe(point)})
result["disk"] = {"root": "/usr/lib", "fstype": fstype_of("/usr/lib"), "warm": probe("/usr/lib")}
json.dump(result, sys.stdout, indent=1)
