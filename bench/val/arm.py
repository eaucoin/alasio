#!/usr/bin/env python3
"""Spike, not for main: arming and first writes under the fork's `wp` dirty tracking (X-ARM:
G-19, G-20 as built, G-22, G-24), on raw runsc, with the flags alasio gives its sandboxes
(cli/src/cluster/k3s.ts: dirty-tracking wp, app-huge-pages false) and its live saves
(src/node-agent/runsc.ts: --leave-running --precopy=auto --precopy-throttle=on --direct
--compression=none).

    arm.py carm    --runsc R --platform P --out F   c_arm: the in-pause arm after 0/25/100 % of
                                                      the memory was first written, by unit and size
    arm.py prearm  --runsc R --platform P --out F   g_arm: live saves with pre-copy under a writer
                                                      at the poller's and the saturating fixture's rates
    arm.py rates   --runsc R --platform P --out F   rho_w: the Bun fixtures' bytes first written in
                                                      a 1 s epoch, two saves 1 s apart

Each measurement is one JSON line of --out.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from pathlib import Path

from common import MIB, WORK, Sandbox, emit, host, log, rootfs_of_image, save_lines, sh

HERE = Path(__file__).resolve().parent
LIVE = ["--leave-running", "--precopy=auto", "--precopy-throttle=on", "--direct", "--compression=none"]
PLAIN = ["--leave-running", "--compression=none"]
BUN_IMAGE = "oven/bun:1.4.2"


def flags(a: argparse.Namespace, unit: int) -> list[str]:
    return [f"--platform={a.platform}", "--dirty-tracking=wp", f"--dirty-tracking-unit={unit}", "--app-huge-pages=false"]


def c_rootfs() -> Path:
    root = WORK / "rootfs-c"
    if not (root / "wpw").exists():
        (root / "tmp").mkdir(parents=True, exist_ok=True)
        sh("gcc", "-O2", "-static", "-pthread", "-o", str(root / "wpw"), str(HERE / "wpw.c"))
    return root


def image_dir(name: str) -> Path:
    d = WORK / "images" / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    return d


def carm(a: argparse.Namespace) -> None:
    rootfs = c_rootfs()
    for unit in a.units:
        for mib in a.sizes:
            sb = Sandbox(a.runsc, rootfs, ["/wpw", str(mib)], flags(a, unit), props=[f"MemoryMax={2 * mib + 2048}M", "MemorySwapMax=0"])
            try:
                ready = sb.start()
                # The first save starts the tracking, every unit armed.
                first = save_lines(sb.checkpoint(image_dir("carm"), PLAIN)["lines"])
                log(f"carm {a.platform} unit {unit} {mib} MiB: ready {ready:.1f} s, first arm {first['arms']}")
                for rep in range(1, a.reps + 1):
                    for fill in a.fills:
                        took, said = sb.ask(f"write {fill}", "WROTE")
                        pages, ms = int(said.split()[1]), float(said.split()[2])
                        ck = sb.checkpoint(image_dir("carm"), PLAIN)
                        s = save_lines(ck["lines"])
                        in_pause = [arm for arm in s["arms"] if s.get("paused_at") is not None and arm["at"] is not None and arm["at"] >= s["paused_at"]]
                        record = {"part": "carm", "platform": a.platform, "unit": unit, "mib": mib, "fill_pct": fill, "rep": rep,
                                  "written_bytes": pages * 4096, "write_ms": ms, "write_us_per_page": ms * 1000 / pages if pages else None,
                                  "arm_s": sum(arm["s"] for arm in in_pause) if in_pause else None, "arms": s["arms"],
                                  "pause_logged_s": s.get("pause_logged_s"), "checkpoint_s": ck["s"], "kernel_save": s.get("kernel_save"),
                                  "host": host()}
                        emit(a.out, record)
                        log(f"  rep {rep} fill {fill} %: wrote {pages} pages in {ms:.1f} ms; arm {record['arm_s']}; pause {record['pause_logged_s']}")
            except RuntimeError as error:
                emit(a.out, {"part": "carm", "platform": a.platform, "unit": unit, "mib": mib, "error": str(error)[-6000:], "host": host()})
                log(f"carm {a.platform} unit {unit} {mib} MiB failed: {str(error)[-3000:]}")
            finally:
                sb.destroy()
                sb.remove()


def prearm(a: argparse.Namespace) -> None:
    rootfs = c_rootfs()
    for mib in a.sizes:
        for rate in a.rates:
            sb = Sandbox(a.runsc, rootfs, ["/wpw", str(mib), str(rate)], flags(a, 65536), props=[f"MemoryMax={2 * mib + 2048}M", "MemorySwapMax=0"])
            try:
                sb.start()
                save_lines(sb.checkpoint(image_dir("prearm"), PLAIN)["lines"])
                for rep in range(1, a.reps + 1):
                    time.sleep(a.interval)
                    ck = sb.checkpoint(image_dir("prearm"), LIVE)
                    s = save_lines(ck["lines"])
                    paused = s.get("paused_at")
                    before = [arm for arm in s["arms"] if paused is not None and arm["at"] is not None and arm["at"] < paused]
                    in_pause = [arm for arm in s["arms"] if paused is not None and arm["at"] is not None and arm["at"] >= paused]
                    record = {"part": "prearm", "platform": a.platform, "unit": 65536, "mib": mib, "rate_mib_s": rate, "rep": rep,
                              # From the end of the last arm while tasks ran to the pause: what the in-pause arm re-arms was first written in it.
                              "gap_s": (paused - before[-1]["at"]) if before else None,
                              "running_arms_s": [arm["s"] for arm in before], "in_pause_arm_s": sum(arm["s"] for arm in in_pause) if in_pause else None,
                              "pause_logged_s": s.get("pause_logged_s"), "precopy": s.get("precopy"), "rounds": s["precopy_rounds"],
                              "checkpoint_s": ck["s"], "kernel_save": s.get("kernel_save"), "host": host()}
                    emit(a.out, record)
                    log(f"prearm {mib} MiB at {rate} MiB/s rep {rep}: gap {record['gap_s']}, in-pause arm {record['in_pause_arm_s']}, pause {record['pause_logged_s']}")
            except RuntimeError as error:
                emit(a.out, {"part": "prearm", "platform": a.platform, "mib": mib, "rate_mib_s": rate, "error": str(error)[-6000:], "host": host()})
                log(f"prearm {a.platform} {mib} MiB at {rate} MiB/s failed: {str(error)[-3000:]}")
            finally:
                sb.destroy()
                sb.remove()


def copy_mib_s() -> float:
    """The runner's copy bandwidth, as bench/common/host.sh measures it: 1 GiB written O_DIRECT."""
    probe = WORK / ".copy-bandwidth"
    t0 = time.monotonic()
    sh("dd", "if=/dev/zero", f"of={probe}", "bs=16M", "count=64", "oflag=direct", "conv=fsync")
    took = time.monotonic() - t0
    probe.unlink()
    return 1024 / took


def rates(a: argparse.Namespace) -> None:
    rootfs = rootfs_of_image(BUN_IMAGE, WORK / "rootfs-bun")
    (rootfs / "workspace").mkdir(exist_ok=True)
    rate = copy_mib_s()
    fixture = (HERE / "bench.bun.js").read_text()
    # The fixture as the driver installs it, PARAMS before it; then held open, as bayma's session holds it.
    for case in a.cases:
        name, size = case.split("/")
        params = {"fixture": name, "sizeMib": int(size), "runId": f"val-{case}", "rateMibS": rate, "workspace": "/workspace"}
        (rootfs / "val-fixture.mjs").write_text(f"const PARAMS = {json.dumps(params)};\n{fixture}\nsetInterval(() => {{}}, 1 << 30);\n")
        sb = Sandbox(a.runsc, rootfs, ["/usr/local/bin/bun", "/val-fixture.mjs"], flags(a, 65536), props=[f"MemoryMax={int(size) * 2 + 2048}M", "MemorySwapMax=0"])
        try:
            sb.start(ready="BENCH:", timeout=600)
            time.sleep(3)
            for rep in range(1, a.reps + 1):
                parent = image_dir("rates-parent")
                first = sb.checkpoint(parent, PLAIN)
                f = save_lines(first["lines"])
                time.sleep(a.epoch)
                second = sb.checkpoint(image_dir("rates-delta"), PLAIN + [f"--parent-image-path={parent}"])
                s = save_lines(second["lines"])
                epoch = (s["paused_at"] - f["resumed_at"]) % 86400 if s.get("paused_at") is not None and f.get("resumed_at") is not None else None
                in_pause = [arm for arm in s["arms"] if s.get("paused_at") is not None and arm["at"] is not None and arm["at"] >= s["paused_at"]]
                record = {"part": "rates", "platform": a.platform, "case": case, "rate_mib_s": rate if name == "saturating" else None, "rep": rep,
                          "epoch_s": epoch, "delta_written_bytes": s.get("written_bytes"),
                          "first_write_Bps": s["written_bytes"] / epoch if epoch and s.get("written_bytes") is not None else None,
                          "arm_s": sum(arm["s"] for arm in in_pause) if in_pause else None, "pause_logged_s": s.get("pause_logged_s"),
                          "host": host()}
                emit(a.out, record)
                log(f"rates {case} rep {rep}: {record['delta_written_bytes']} B in {epoch} s, arm {record['arm_s']}")
        except (RuntimeError, TimeoutError) as error:
            emit(a.out, {"part": "rates", "platform": a.platform, "case": case, "error": str(error)[-6000:], "host": host()})
            log(f"rates {case} failed: {str(error)[-3000:]}")
        finally:
            sb.destroy()
            sb.remove()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("part", choices=["carm", "prearm", "rates"])
    p.add_argument("--runsc", required=True)
    p.add_argument("--platform", default="systrap")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--reps", type=int, default=5)
    p.add_argument("--units", type=int, nargs="+", default=[4096, 65536])
    p.add_argument("--sizes", type=int, nargs="+", default=[256, 1024, 3072])
    p.add_argument("--fills", type=float, nargs="+", default=[0, 25, 100])
    p.add_argument("--rates", type=float, nargs="+", default=[20, 440])
    p.add_argument("--interval", type=float, default=5)
    p.add_argument("--epoch", type=float, default=1)
    p.add_argument("--cases", nargs="+", default=["idle/16", "poller/1024", "saturating/256", "saturating/1024", "saturating/3072"])
    a = p.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    {"carm": carm, "prearm": prearm, "rates": rates}[a.part](a)


if __name__ == "__main__":
    main()
