#!/usr/bin/env python3
"""Spike, not for main: does an OOM of the parent of two sandboxes, as of kubepods, kill the
second while the first's memory is torn down (X-OOM)? On raw runsc as alasio runs it (systrap,
dirty-tracking wp, app-huge-pages false), each sandbox in a scope of its own under one slice
whose memory.max the two together outgrow.

Each trial: B holds --hold MiB and charges pages beside (allocating, touching and freeing 4 MiB
at --churn MiB/s), A grows at --grow MiB/s until the slice is out of memory. Then: which scopes'
memory.events counted an OOM kill, whether each sandbox still runs, and how A's scope's memory
fell after its kill, sampled every 2 ms. With memory.oom.group 0 and 1 on the scopes (kubelet
sets it on containers' cgroups). Each trial is one JSON line of --out.

    oom.py --runsc R --out F [--trials 20] [--limit 4096] [--hold 1536] [--groups 0 1]
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

from common import MIB, WORK, Sandbox, emit, host, log, sh

HERE = Path(__file__).resolve().parent
SLICE = "valoom.slice"
PARENT = Path("/sys/fs/cgroup") / SLICE


def rootfs() -> Path:
    root = WORK / "rootfs-oom"
    if not (root / "om").exists():
        (root / "tmp").mkdir(parents=True, exist_ok=True)
        om = HERE / "om"
        if om.exists():
            shutil.copy(om, root / "om")
        else:
            sh("gcc", "-O2", "-static", "-o", str(root / "om"), str(HERE / "om.c"))
    return root


def events(cg: Path) -> dict:
    try:
        return {k: int(v) for k, v in (line.split() for line in (cg / "memory.events").read_text().splitlines())}
    except OSError:
        return {}


def alive(sb: Sandbox) -> bool:
    said = sb.run("state", sb.cid, check=False)
    return said.returncode == 0 and '"running"' in said.stdout


def trial(a: argparse.Namespace, group: int, index: int, root: Path) -> dict:
    flags = ["--platform=systrap", "--dirty-tracking=wp", "--app-huge-pages=false"]
    b = Sandbox(a.runsc, root, ["/om", "churn", str(a.hold), str(a.churn)], flags, slice_=SLICE)
    grower = Sandbox(a.runsc, root, ["/om", "grow", str(a.grow)], flags, slice_=SLICE)
    try:
        b.start()
        (PARENT / "memory.max").write_text(str(a.limit * MIB))
        (PARENT / "memory.swap.max").write_text("0")
        (b.cgroup() / "memory.oom.group").write_text(str(group))
        parent_before = events(PARENT)
        grower.start()
        a_cg = grower.cgroup()
        (a_cg / "memory.oom.group").write_text(str(group))
        b_cg = b.cgroup()
        # Until the slice's first OOM kill, then A's memory sampled every 2 ms until it is freed, B's events beside.
        deadline = time.monotonic() + 120
        while events(PARENT).get("oom_kill", 0) == parent_before.get("oom_kill", 0) and time.monotonic() < deadline:
            time.sleep(0.002)
        killed_at = time.monotonic()
        peak = int((a_cg / "memory.peak").read_text()) if (a_cg / "memory.peak").exists() else None
        falling: list[tuple[float, int]] = []
        while time.monotonic() - killed_at < a.watch:
            try:
                current = int((a_cg / "memory.current").read_text())
            except OSError:
                falling.append((time.monotonic() - killed_at, 0))
                break
            falling.append((time.monotonic() - killed_at, current))
            if current < 16 * MIB:
                break
            time.sleep(0.002)
        time.sleep(1)
        b_alive, a_alive = alive(b), alive(grower)
        record = {
            "oom_group": group, "trial": index, "limit_mib": a.limit, "hold_mib": a.hold, "churn_mib_s": a.churn, "grow_mib_s": a.grow,
            "parent_events": {k: v - parent_before.get(k, 0) for k, v in events(PARENT).items()},
            "a_events": events(a_cg), "b_events": events(b_cg),
            "a_alive": a_alive, "b_alive": b_alive, "b_killed": not b_alive or events(b_cg).get("oom_kill", 0) > 0,
            "a_peak_bytes": peak,
            # How long A's charge took to fall below half of its peak and to under 16 MiB, after the first kill was counted.
            "a_half_freed_s": next((t for t, c in falling if peak and c < peak / 2), None),
            "a_freed_s": next((t for t, c in falling if c < 16 * MIB), None),
            "a_samples": falling[:: max(1, len(falling) // 50)],
            "host": host(),
        }
        return record
    finally:
        for sb in (grower, b):
            sb.destroy()
            sb.remove()
        if PARENT.exists():
            (PARENT / "memory.max").write_text("max")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runsc", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--trials", type=int, default=20)
    p.add_argument("--limit", type=int, default=4096)
    p.add_argument("--hold", type=int, default=1536)
    p.add_argument("--churn", type=float, default=200)
    p.add_argument("--grow", type=float, default=800)
    p.add_argument("--watch", type=float, default=10)
    p.add_argument("--groups", type=int, nargs="+", default=[0, 1])
    a = p.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    root = rootfs()
    for group in a.groups:
        for index in range(1, a.trials + 1):
            record = trial(a, group, index, root)
            emit(a.out, record)
            log(f"oom.group {group} trial {index}: B killed {record['b_killed']}, A freed in {record['a_freed_s']} s, parent {record['parent_events']}")


if __name__ == "__main__":
    main()
