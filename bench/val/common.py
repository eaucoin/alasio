"""Spike, not for main: what the validation experiments on raw runsc share (bench/val/*.py).

A sandbox is one container under a runsc binary, its root a directory holding the workload,
run in a transient systemd scope under a slice of the experiment's (so that its memory is
measured and limited, as tools/fork/bench/bench.py of the gVisor fork runs its own); its
stdin a FIFO held open, its stdout a log followed line by line; runsc's logs (the Sentry's
among them) in a directory of its own. Run as root, with python3's standard library only.
"""

from __future__ import annotations

import json
import os
import platform as host_platform
import re
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

MIB = 2**20
WORK = Path(os.environ.get("VAL_WORK", "/mnt/val"))
STATE = WORK / "state"


def log(*parts: object) -> None:
    print(*parts, file=sys.stderr, flush=True)


def sh(*cmd: str, check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True, **kw)


def host() -> dict:
    """The runner, as each result records it."""
    model = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "")
    mem = next(int(l.split()[1]) * 1024 for l in open("/proc/meminfo") if l.startswith("MemTotal"))
    return {"cpu_model": model, "kernel": host_platform.release(), "cores": os.cpu_count(), "memory_bytes": mem,
            "runner_image": f"{os.environ.get('ImageOS', 'unknown')}/{os.environ.get('ImageVersion', 'unknown')}"}


def stamp(line: str) -> float | None:
    """The time of day, in seconds, at which runsc logged `line`."""
    m = re.match(r"[IWDEF]\d{4} (\d\d):(\d\d):(\d\d\.\d+)", line)
    return int(m[1]) * 3600 + int(m[2]) * 60 + float(m[3]) if m else None


def go_seconds(text: str) -> float:
    """Seconds in a Go duration's string, as 1m2.5s, 812ms or 40µs."""
    units = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}
    return sum(float(v) * units[u] for v, u in re.findall(r"([\d.]+)(h|ms|m|s|us|µs|ns)", text))


class Tail:
    """Follows a workload's stdout log, timestamping each line on arrival."""

    def __init__(self, path: Path):
        self.path, self.pos, self.partial, self.lines = path, 0, b"", []

    def poll(self) -> None:
        with open(self.path, "rb") as f:
            f.seek(self.pos)
            data = f.read()
        self.pos += len(data)
        now = time.monotonic()
        *done, self.partial = (self.partial + data).split(b"\n")
        self.lines += [(now, line.decode(errors="replace")) for line in done]

    def wait(self, prefix: str, timeout: float, start: int = 0) -> tuple[float, str]:
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            for t, line in self.lines[start:]:
                if line.startswith(prefix):
                    return t, line
            if time.monotonic() > deadline:
                raise TimeoutError(f"no {prefix!r} in {self.path} within {timeout} s")
            time.sleep(0.001)


class Sandbox:
    """One container (and sandbox) under `runsc`, in a scope of `slice_`."""

    def __init__(self, runsc: str, rootfs: Path, argv: list[str], flags: list[str], slice_: str = "val.slice",
                 props: list[str] | None = None, env: list[str] | None = None):
        self.runsc_path, self.rootfs, self.argv, self.flags = runsc, rootfs, argv, flags
        self.slice, self.props, self.env = slice_, props or [], env or []
        self.cid = "val-" + uuid.uuid4().hex[:10]
        self.dir = WORK / "runs" / self.cid
        self.logs = self.dir / "logs"
        self.logs.mkdir(parents=True)
        self.bundle = self.dir / "bundle"
        self.unit = f"{self.cid}-0"
        self.n = 0
        self.fifo_fd: int | None = None
        self.tail: Tail | None = None

    def runsc(self, *args: str) -> list[str]:
        return [self.runsc_path, f"--root={STATE}", "--ignore-cgroups", "--network=none", f"--debug-log={self.logs}/",
                *self.flags, *args]

    def make_bundle(self) -> None:
        self.bundle.mkdir(parents=True)
        sh(self.runsc_path, "spec", f"--bundle={self.bundle}", "--", *self.argv)
        spec = json.loads((self.bundle / "config.json").read_text())
        spec["root"] = {"path": str(self.rootfs), "readonly": False}
        spec["process"]["terminal"] = False
        spec["process"]["env"] = spec["process"].get("env", []) + self.env
        spec["process"]["cwd"] = "/"
        (self.bundle / "config.json").write_text(json.dumps(spec, indent=1))

    def _start(self, verb: list[str]) -> subprocess.Popen:
        if self.fifo_fd is not None:
            os.close(self.fifo_fd)
        fifo = self.dir / f"in-{self.n}.fifo"
        os.mkfifo(fifo)
        self.fifo_fd = os.open(fifo, os.O_RDWR)
        out = self.dir / f"out-{self.n}.log"
        self.unit = f"{self.cid}-{self.n}"
        cmd = ["systemd-run", "--quiet", "--scope", f"--unit={self.unit}", f"--slice={self.slice}",
               *[f"-p{p}" for p in self.props], "--", *self.runsc(*verb)]
        with open(out, "ab") as o, open(self.dir / f"runsc-{self.n}.err", "ab") as e:
            proc = subprocess.Popen(cmd, stdin=self.fifo_fd, stdout=o, stderr=e)
        self.tail = Tail(out)
        return proc

    def start(self, ready: str = "READY", timeout: float = 300) -> float:
        """Runs the workload until it prints `ready`: how long that took, in seconds."""
        self.make_bundle()
        t0 = time.monotonic()
        proc = self._start(["run", "--detach", f"--bundle={self.bundle}", self.cid])
        if proc.wait() != 0:
            raise RuntimeError(f"runsc run failed: {(self.dir / 'runsc-0.err').read_text()[-2000:]}")
        t, _ = self.tail.wait(ready, timeout)
        return t - t0

    def restore(self, image: Path, flags: list[str]) -> float:
        """Restores the sandbox from `image` with `flags`, its stdin and stdout new: how long the command took, in seconds."""
        self.n += 1
        t0 = time.monotonic()
        proc = self._start(["restore", "--detach", f"--bundle={self.bundle}", f"--image-path={image}", *flags, self.cid])
        if proc.wait() != 0:
            raise RuntimeError(f"runsc restore failed: {(self.dir / f'runsc-{self.n}.err').read_text()[-2000:]}")
        return time.monotonic() - t0

    def send(self, line: str) -> None:
        os.write(self.fifo_fd, (line + "\n").encode())

    def ask(self, command: str, prefix: str, timeout: float = 600) -> tuple[float, str]:
        """Sends `command` and waits for its answer: (seconds it took, the answer)."""
        self.tail.poll()
        n = len(self.tail.lines)
        t0 = time.monotonic()
        self.send(command)
        t, line = self.tail.wait(prefix, timeout, n)
        return t - t0, line

    def boot_log(self) -> list[str]:
        """The lines the sandbox's Sentry logged so far, those of earlier incarnations first."""
        lines: list[str] = []
        for f in sorted(self.logs.glob("*boot*")):
            lines += f.read_text(errors="replace").splitlines()
        return lines

    def cgroup(self) -> Path:
        cg = sh("systemctl", "show", "-P", "ControlGroup", f"{self.unit}.scope").stdout.strip()
        return Path(f"/sys/fs/cgroup{cg}")

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return sh(*self.runsc(*args), check=check)

    def checkpoint(self, image: Path, flags: list[str]) -> dict:
        """Checkpoints the sandbox to `image` with `flags`: how long the command took, and the Sentry's lines of it."""
        mark = len(self.boot_log())
        t0 = time.monotonic()
        ck = subprocess.run(self.runsc("checkpoint", f"--image-path={image}", *flags, self.cid), capture_output=True, text=True)
        took = time.monotonic() - t0
        if ck.returncode != 0:
            raise RuntimeError(f"checkpoint {' '.join(flags)} failed: {ck.stderr[-2000:]}")
        # The Sentry's lines may land a moment after the command returns.
        time.sleep(0.2)
        return {"s": took, "lines": self.boot_log()[mark:]}

    def destroy(self) -> None:
        subprocess.run(self.runsc("kill", self.cid, "KILL"), capture_output=True)
        subprocess.run(self.runsc("delete", "--force", self.cid), capture_output=True)
        for _ in range(500):
            if subprocess.run(["systemctl", "is-active", "-q", f"{self.unit}.scope"]).returncode != 0:
                break
            time.sleep(0.01)
        subprocess.run(["systemctl", "stop", f"{self.unit}.scope"], capture_output=True)
        if self.fifo_fd is not None:
            os.close(self.fifo_fd)
            self.fifo_fd = None

    def remove(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)


def save_lines(lines: list[str]) -> dict:
    """What the Sentry logged of a checkpoint: each arm (its time of day, MemoryManagers, duration, unit),
    the pause (pausing to resumed, as times of day), the pre-copy rounds, and what it wrote."""
    r: dict = {"arms": [], "precopy_rounds": []}
    for line in lines:
        at = stamp(line)
        if m := re.search(r"Dirty tracking: armed (\d+) MemoryManagers in (\S+) \(unit (\d+) bytes\)", line):
            r["arms"].append({"at": at, "mms": int(m[1]), "s": go_seconds(m[2]), "unit": int(m[3])})
        elif "pausing all tasks" in line or "Pausing all tasks" in line:
            r.setdefault("paused_at", at)
        elif m := re.search(r"Tasks resumed after save, paused for (\S+?)\.?$", line):
            r["resumed_at"] = at
            r["pause_logged_s"] = go_seconds(m[1])
        elif m := re.search(r"Pre-copy round (\d+): wrote (\d+) bytes in (\S+); (\d+) bytes dirty since", line):
            r["precopy_rounds"].append({"at": at, "round": int(m[1]), "bytes": int(m[2]), "s": go_seconds(m[3]), "pending": int(m[4])})
        elif m := re.search(r"Pre-copy done after (\d+) rounds \((.*)\): (\d+) bytes in (\S+)", line):
            r["precopy"] = {"at": at, "rounds": int(m[1]), "stop": m[2], "bytes": int(m[3]), "s": go_seconds(m[4])}
        elif m := re.search(r"(\d+) bytes written to the pages file", line):
            r["written_bytes"] = r.get("written_bytes", 0) + int(m[1])
        elif "Kernel save took" in line or "kernel save took" in line.lower():
            r.setdefault("kernel_save", line[-120:])
    return r


def rootfs_of_image(image: str, into: Path) -> Path:
    """Exports a container image's filesystem into `into` (once), for runsc to run from."""
    if (into / ".exported").exists():
        return into
    sh("docker", "pull", "-q", image)
    cid = sh("docker", "create", image).stdout.strip()
    into.mkdir(parents=True, exist_ok=True)
    try:
        export = subprocess.Popen(["docker", "export", cid], stdout=subprocess.PIPE)
        subprocess.run(["tar", "-x", "-C", str(into)], stdin=export.stdout, check=True)
        export.wait()
    finally:
        sh("docker", "rm", cid)
    (into / ".exported").write_text(image)
    return into


def pinned(cpu: str, kernel: str) -> None:
    """Fails at once, with status 3, on a runner of another CPU model or kernel than pinned."""
    h = host()
    if cpu and h["cpu_model"] != cpu:
        print(f"::error::this runner's CPU is {h['cpu_model']}, not {cpu}: re-run this job", flush=True)
        sys.exit(3)
    if kernel and not h["kernel"].startswith(kernel):
        print(f"::error::this runner's kernel is {h['kernel']}, not {kernel}*: re-run this job", flush=True)
        sys.exit(3)


def emit(out: Path, record: dict) -> None:
    with open(out, "a") as f:
        f.write(json.dumps(record) + "\n")
