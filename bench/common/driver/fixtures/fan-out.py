# The fan-out fixture, installed into a bayma Python session by the driver's setup exec, which
# prepends `PARAMS = {...}`: eleven worker processes, each holding 10 MiB of random bytes and
# answering `ping` and `verify` on its stdin; and the ticker, a thread of this process that wakes
# every 1 ms while a window is marked and keeps the longest gap between two wakes.
import hashlib
import json
import os
import subprocess
import sys
import threading
import time

if "bench" in globals():
    raise RuntimeError("this session holds a fixture already")

WORKER = r"""
import hashlib, os, sys
held = bytearray(os.urandom(10 << 20))
print(hashlib.sha256(held).hexdigest(), flush=True)
for line in sys.stdin:
    asked = line.strip()
    if asked == "ping":
        print("pong", flush=True)
    elif asked == "verify":
        print(hashlib.sha256(held).hexdigest(), flush=True)
"""


class Bench:
    def __init__(self, params):
        self.marker = hashlib.sha256(f"{params['runId']}/fan-out".encode()).hexdigest()
        self.workers = [
            subprocess.Popen([sys.executable, "-c", WORKER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
            for _ in range(params.get("workers", 11))
        ]
        self.digests = [worker.stdout.readline().strip() for worker in self.workers]
        self.measuring = threading.Event()
        self.max_gap_ms = 0.0
        self.ticks = 0
        threading.Thread(target=self._tick, daemon=True).start()

    def _tick(self):
        while True:
            self.measuring.wait()
            last = time.monotonic()
            while self.measuring.is_set():
                time.sleep(0.001)
                now = time.monotonic()
                self.max_gap_ms = max(self.max_gap_ms, (now - last) * 1000)
                self.ticks += 1
                last = now

    def _ask(self, worker, line):
        worker.stdin.write(line + "\n")
        worker.stdin.flush()
        return worker.stdout.readline().strip()

    def rss_bytes(self):
        def of(pid):
            try:
                with open(f"/proc/{pid}/status") as status:
                    line = next(each for each in status if each.startswith("VmRSS:"))
                return int(line.split()[1]) * 1024
            except (OSError, StopIteration):
                return 0

        return sum(of(pid) for pid in [os.getpid(), *(worker.pid for worker in self.workers)])

    def baseline(self):
        return {"baseline_mib_s": 0, "rss_bytes": self.rss_bytes()}

    def mark(self):
        self.max_gap_ms = 0.0
        self.ticks = 0
        self.marked_at = time.monotonic()
        self.measuring.set()
        return {"marked": True}

    def gaps(self):
        self.measuring.clear()
        return {"max_gap_ms": self.max_gap_ms, "ticks": self.ticks, "window_ms": (time.monotonic() - self.marked_at) * 1000, "rate_mib_s": 0, "slowdown": None}

    def first(self):
        return {"marker": self.marker, "pongs": sum(self._ask(worker, "ping") == "pong" for worker in self.workers)}

    def verify(self):
        answered = [self._ask(worker, "verify") for worker in self.workers]
        return {
            "marker": self.marker,
            "memory": answered == self.digests,
            "children": all(worker.poll() is None for worker in self.workers) and all(self._ask(worker, "ping") == "pong" for worker in self.workers),
            "file": None,
            "socket": None,
            "running": None,
            "rss_bytes": self.rss_bytes(),
        }


bench = Bench(PARAMS)
print("BENCH:" + json.dumps({"marker": bench.marker, "workers": len(bench.workers), "pids": [os.getpid(), *(worker.pid for worker in bench.workers)], "rss_bytes": bench.rss_bytes()}))
