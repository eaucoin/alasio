#!/usr/bin/env python3
"""Spike, not for main: what clock pre-calibration at restore changes for the first exec (G-18),
upstream gVisor master (runsc/boot/restore.go, the clocks' first sample taken before the kernel
loads) against the same build without that sample (as the fork and release-20261026.0 restore),
alternating, on systrap.

Each repetition: the workload (clk.c) checkpointed (stopping the sandbox), restored, and at once
asked to read the time for --probe ms, in 10 ms windows; then asked to answer a ping. Per window,
the nanoseconds a read cost: the VDSO serves it once the clocks are calibrated, a system call
before. Each repetition is one JSON line of --out.

    g18.py --with R1 --without R2 --out F [--reps 20] [--probe 1500]
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

from common import WORK, Sandbox, emit, host, log, sh

HERE = Path(__file__).resolve().parent


def rootfs() -> Path:
    root = WORK / "rootfs-clk"
    if not (root / "clk").exists():
        (root / "tmp").mkdir(parents=True, exist_ok=True)
        sh("gcc", "-O2", "-static", "-o", str(root / "clk"), str(HERE / "clk.c"))
    return root


def rep(runsc: str, build: str, index: int, probe_ms: int, root: Path) -> dict:
    sb = Sandbox(runsc, root, ["/clk"], ["--platform=systrap"])
    image = WORK / "images" / sb.cid
    try:
        sb.start()
        _, before = sb.ask(f"probe {probe_ms}", "PROBE")
        image.mkdir(parents=True)
        sb.checkpoint(image, ["--compression=none"])
        sb.destroy()
        restore_s = sb.restore(image, [])
        t0 = time.monotonic()
        sb.send(f"probe {probe_ms}")
        t, after = sb.tail.wait("PROBE", 60, 0)
        answered_s = t - t0
        ping_s, _ = sb.ask("ping 1", "PONG 1")
        windows = [float(v) for v in after.split()[1:]]
        steady = sorted(float(v) for v in before.split()[1:])[len(before.split()[1:]) // 2]
        fast = next((i for i, ns in enumerate(windows) if ns < 3 * steady), None)
        return {"build": build, "rep": index, "restore_s": restore_s, "probe_answered_s": answered_s, "ping_s": ping_s,
                "steady_ns_per_read": steady, "first_window_ns_per_read": windows[0] if windows else None,
                # When reads became as cheap as before the checkpoint (within 3x), in milliseconds after the probe began.
                "vdso_after_ms": fast * 10 if fast is not None else None,
                "windows_ns": windows, "host": host()}
    finally:
        sb.destroy()
        sb.remove()
        shutil.rmtree(image, ignore_errors=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--with", dest="with_", required=True, help="runsc of upstream master as it is")
    p.add_argument("--without", required=True, help="the same runsc without the restore's first clock sample")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--reps", type=int, default=20)
    p.add_argument("--probe", type=int, default=1500)
    a = p.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    root = rootfs()
    for index in range(1, a.reps + 1):
        for build, runsc in (("pre-calibrated", a.with_), ("not", a.without)):
            record = rep(runsc, build, index, a.probe, root)
            emit(a.out, record)
            log(f"{build} rep {index}: first window {record['first_window_ns_per_read']} ns/read, VDSO after {record['vdso_after_ms']} ms, steady {record['steady_ns_per_read']}")


if __name__ == "__main__":
    main()
