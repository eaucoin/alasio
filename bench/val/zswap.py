#!/usr/bin/env python3
"""Spike, not for main: a frozen sandbox's memory reclaimed into zswap and faulted back (E8 =
X-ZSWAP), by compressor, on raw runsc as alasio runs it (systrap, dirty-tracking wp,
app-huge-pages false).

The runner gets a swapfile, which zswap needs to exist, but never writes to: each sandbox's
scope has zswap writeback off (memory.zswap.writeback 0) and its swap unlimited, as the node
agent would write a T2 session's cgroup (M5 step 3). For each compressor, size and content:
the sandbox is started, filled, frozen (`runsc pause`), its scope's memory.reclaim written with
its size, timed; then resumed (`runsc resume`, timed) and every page read (faulted back), timed,
and read again (resident). Each repetition is one JSON line of --out.

    zswap.py --runsc R --out F [--compressors lzo lz4 zstd] [--sizes 256 1024 3072] [--reps 5]
"""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

from common import MIB, WORK, Sandbox, emit, host, log, sh

HERE = Path(__file__).resolve().parent
ZSWAP = Path("/sys/module/zswap/parameters")


def setup(swap_gib: int) -> dict:
    swapfile = WORK / "swapfile"
    if not swapfile.exists():
        sh("fallocate", "-l", f"{swap_gib}G", str(swapfile))
        swapfile.chmod(0o600)
        sh("mkswap", str(swapfile))
    sh("swapon", str(swapfile), check=False)
    (ZSWAP / "enabled").write_text("Y")
    (ZSWAP / "max_pool_percent").write_text("60")
    said = {name: (ZSWAP / name).read_text().strip() for name in ("enabled", "compressor", "zpool", "max_pool_percent", "accept_threshold_percent") if (ZSWAP / name).exists()}
    said["swaps"] = Path("/proc/swaps").read_text()
    return said


def rootfs(corpus_mib: int) -> Path:
    root = WORK / "rootfs-z"
    if not (root / "zsw").exists():
        (root / "tmp").mkdir(parents=True, exist_ok=True)
        sh("gcc", "-O2", "-static", "-o", str(root / "zsw"), str(HERE / "zsw.c"))
        # Real bytes of moderate compressibility: the runner's shared libraries and Python's sources, as a heap holds code-like data and text.
        sh("sh", "-c", f"(cat /usr/lib/x86_64-linux-gnu/*.so* 2>/dev/null; find /usr/lib/python3* -name '*.py' -exec cat {{}} + 2>/dev/null) | head -c {corpus_mib}M > {root}/corpus")
    return root


def stat(cg: Path) -> dict:
    out = {"current": int((cg / "memory.current").read_text())}
    for line in (cg / "memory.stat").read_text().splitlines():
        key, value = line.split()
        if key in ("anon", "file", "shmem", "zswap", "zswapped", "swapcached", "pgmajfault", "zswpin", "zswpout", "zswpwb"):
            out[key] = int(value)
    for name in ("memory.swap.current", "memory.zswap.current"):
        if (cg / name).exists():
            out[name.split(".", 1)[1]] = int((cg / name).read_text())
    return out


def ratio(path: Path, tool: list[str]) -> float | None:
    """How much `tool` compresses the first 64 MiB of `path`, for reference beside zswap's."""
    try:
        packed = subprocess.run(["sh", "-c", f"head -c 64M {path} | {' '.join(tool)} | wc -c"], capture_output=True, text=True, check=True).stdout
        return 64 * MIB / int(packed)
    except (subprocess.CalledProcessError, ValueError):
        return None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runsc", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--compressors", nargs="+", default=["lzo", "lz4", "zstd"])
    p.add_argument("--sizes", type=int, nargs="+", default=[256, 1024, 3072])
    p.add_argument("--contents", nargs="+", default=["corpus"])
    p.add_argument("--reps", type=int, default=5)
    a = p.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    if not ZSWAP.exists():
        emit(a.out, {"error": "this kernel has no zswap", "host": host()})
        return
    said = setup(swap_gib=12)
    root = rootfs(corpus_mib=512)
    corpus = {"lz4": ratio(root / "corpus", ["lz4", "-1", "-c"]), "zstd-1": ratio(root / "corpus", ["zstd", "-1", "-c"])}
    log(f"zswap {said}; corpus ratios {corpus}")
    for compressor in a.compressors:
        sh("modprobe", compressor, check=False)
        try:
            (ZSWAP / "compressor").write_text(compressor)
        except OSError as error:
            emit(a.out, {"compressor": compressor, "error": f"not taken: {error}", "host": host()})
            continue
        taken = (ZSWAP / "compressor").read_text().strip()
        for content in a.contents:
            for mib in a.sizes:
                for rep in range(1, a.reps + 1):
                    sb = Sandbox(a.runsc, root, ["/zsw", str(mib), "/corpus" if content == "corpus" else "random"],
                                 ["--platform=systrap", "--dirty-tracking=wp", "--app-huge-pages=false"],
                                 props=[f"MemoryMax={mib + 2048}M", "MemorySwapMax=infinity"])
                    try:
                        sb.start(timeout=600)
                        cg = sb.cgroup()
                        # As the node agent would write a T2 session's: zswap without writing back, swap unlimited.
                        (cg / "memory.zswap.writeback").write_text("0")
                        (cg / "memory.zswap.max").write_text("max")
                        writeback = (cg / "memory.zswap.writeback").read_text().strip() if (cg / "memory.zswap.writeback").exists() else None
                        before = stat(cg)
                        t0 = time.monotonic()
                        sb.run("pause", sb.cid)
                        pause_s = time.monotonic() - t0
                        t0 = time.monotonic()
                        reclaim = subprocess.run(["sh", "-c", f"echo {mib}M > {cg}/memory.reclaim"], capture_output=True, text=True)
                        reclaim_s = time.monotonic() - t0
                        reclaimed = stat(cg)
                        t0 = time.monotonic()
                        sb.run("resume", sb.cid)
                        resume_s = time.monotonic() - t0
                        faulted_s, faulted = sb.ask("touch", "TOUCHED")
                        after = stat(cg)
                        resident_s, resident = sb.ask("touch", "TOUCHED")
                        moved = before["current"] - reclaimed["current"]
                        fault_ms, resident_ms = float(faulted.split()[1]), float(resident.split()[1])
                        record = {
                            "compressor": taken, "content": content, "mib": mib, "rep": rep, "writeback": writeback,
                            "pause_s": pause_s, "reclaim_s": reclaim_s, "reclaim_said": reclaim.stderr.strip()[-200:],
                            "reclaimed_bytes": moved, "reclaim_Bps": moved / reclaim_s if reclaim_s > 0 else None,
                            "zswap_bytes": reclaimed.get("zswap"), "zswapped_bytes": reclaimed.get("zswapped"),
                            "ratio": reclaimed["zswapped"] / reclaimed["zswap"] if reclaimed.get("zswap") else None,
                            "resume_s": resume_s, "fault_back_ms": fault_ms, "resident_touch_ms": resident_ms,
                            "beta_zswap_Bps": reclaimed.get("zswapped", 0) / ((fault_ms - resident_ms) / 1000) if fault_ms > resident_ms else None,
                            "before": before, "reclaimed": reclaimed, "after": after, "corpus_ratios": corpus, "host": host(),
                        }
                        emit(a.out, record)
                        log(f"{taken} {content} {mib} MiB rep {rep}: reclaimed {moved / MIB:.0f} MiB in {reclaim_s:.2f} s "
                            f"(zswap {reclaimed.get('zswap', 0) / MIB:.0f} MiB for {reclaimed.get('zswapped', 0) / MIB:.0f}), "
                            f"faulted back in {fault_ms:.0f} ms (resident {resident_ms:.0f} ms), resume {resume_s * 1000:.0f} ms")
                    finally:
                        sb.destroy()
                        sb.remove()


if __name__ == "__main__":
    main()
