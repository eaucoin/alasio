# The realistic Python fixtures (repl-python, repl-python-checkpointed), installed into a bayma
# Python session by the driver's setup exec, which prepends `PARAMS = {...}`, then worked by a few
# execs (`work`), then left idle, as an agent's session is. The session imports common modules and
# holds about `sizeMib` of what such a session builds, as Python objects: records (dicts) of text,
# identifiers, dates and numbers, indexed in a dict, a fifth of them kept as JSON text and a tenth
# in an in-memory SQLite table, beside a numeric series. repl-python-checkpointed also holds 20 MiB
# of other records in its checkpoint (bayma_write_checkpoint), which bayma pickles at the end of
# every exec and loads again at the start of the next. The content is pseudo-random from a seed of
# the run's id: words of a vocabulary made of syllables, drawn with a skew as words in text are, so
# it compresses as text does. Only the ticker runs in the background, a thread blocked until a
# window is measured.
import array
import collections
import datetime
import gzip
import hashlib
import json
import os
import pickle
import random
import sqlite3
import statistics
import threading
import time

if "bench" in globals():
    raise RuntimeError("this session holds a fixture already")

MIB = 1 << 20
# The bytes the checkpoint holds, pickled, in repl-python-checkpointed.
CHECKPOINT_BYTES = 20 * MIB
# Records built between two looks at the process's memory.
BATCH = 2000
SYLLABLES = "ka lo mi ne ru sa ti vo ze ba de fi go hu ja ke li mo nu pa qui re so tu va we xi yo zu an el in on un ar er ir or ur al il ol ul as es is os us at et it ot ut".split()
EPOCH = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


def rss_bytes():
    with open("/proc/self/status") as status:
        line = next(each for each in status if each.startswith("VmRSS:"))
    return int(line.split()[1]) * 1024


class Bench:
    def __init__(self, params):
        self.fixture = params["fixture"]
        self.checkpointed = self.fixture == "repl-python-checkpointed"
        self.marker = hashlib.sha256(f"{params['runId']}/{self.fixture}".encode()).hexdigest()
        # The same content for the same run.
        self.random = random.Random(int(self.marker[:8], 16))
        rng = self.random
        self.vocabulary = ["".join(rng.choice(SYLLABLES) for _ in range(1 + rng.randrange(4))) for _ in range(4000)]
        self.records = []
        self.by_id = {}
        self.docs = []
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, at TEXT, payload TEXT)")
        size = params["sizeMib"] * MIB
        start = rss_bytes()
        # A tenth of the size in a numeric series, the rest in records, until the process has grown by the size.
        self.series = array.array("d", [0.0]) * max(1, size // 10 // 8)
        for i in range(1, len(self.series)):
            self.series[i] = self.series[i - 1] + rng.random() - 0.5
        while rss_bytes() - start < size:
            for _ in range(BATCH):
                self.add()
        self.db.commit()
        if self.checkpointed:
            # Records of its own, beside the size, as many as pickle to CHECKPOINT_BYTES.
            kept = [self.record(-1 - i) for i in range(1000)]
            each = len(pickle.dumps(kept, protocol=5)) / len(kept)
            while len(kept) < CHECKPOINT_BYTES / each:
                kept.append(self.record(-1 - len(kept)))
            bayma_write_checkpoint({"records": kept})
        self.expected = self.digest()
        self.measuring = threading.Event()
        self.max_gap_ms = 0.0
        self.ticks = 0
        threading.Thread(target=self._tick, daemon=True).start()

    def word(self):
        # The frequent words far more often, as in text.
        return self.vocabulary[int(len(self.vocabulary) * self.random.random() ** 3)]

    def sentence(self, words):
        return " ".join(self.word() for _ in range(words)).capitalize() + "."

    def record(self, id):
        rng = self.random
        first, last = self.word(), self.word()
        return {
            "id": id,
            "uuid": rng.randbytes(16).hex(),
            "name": f"{first.capitalize()} {last.capitalize()}",
            "email": f"{first}.{last}@{self.word()}.com",
            "url": f"https://{self.word()}.example/{self.word()}/{id}",
            "created_at": (EPOCH + datetime.timedelta(seconds=rng.randrange(30_000_000))).isoformat(),
            "score": round(rng.random() * 100, 2),
            "count": rng.randrange(1000),
            "tags": [self.word() for _ in range(2 + rng.randrange(5))],
            "body": " ".join(self.sentence(8 + rng.randrange(24)) for _ in range(1 + rng.randrange(4))),
            "meta": {"views": rng.randrange(1_000_000), "ratio": rng.random(), "active": rng.random() < 0.7, "history": [rng.randrange(500) for _ in range(5 + rng.randrange(16))]},
        }

    def add(self):
        made = self.record(len(self.records))
        self.records.append(made)
        self.by_id[made["id"]] = made
        if made["id"] % 5 == 0:
            self.docs.append(json.dumps(made))
        if made["id"] % 10 == 0:
            self.db.execute("INSERT INTO events (id, kind, at, payload) VALUES (?, ?, ?, ?)", (made["id"], made["tags"][0], made["created_at"], made["body"]))
        return made

    def digest(self):
        # Everything the session holds, in order: every record, the JSON, the table, the series, the checkpoint.
        digest = hashlib.sha256()
        for each in self.records:
            digest.update(json.dumps(each).encode())
        for doc in self.docs:
            digest.update(doc.encode())
        for row in self.db.execute("SELECT id, kind, at, payload FROM events ORDER BY id"):
            digest.update(json.dumps(row).encode())
        digest.update(self.series.tobytes())
        if self.checkpointed:
            digest.update(json.dumps(bayma_read_checkpoint()).encode())
        return digest.hexdigest()

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

    def baseline(self):
        return {"baseline_mib_s": 0, "rss_bytes": rss_bytes()}

    def work(self):
        # One exec of an agent's work: a search, a top list, counts per tag, statistics, a report
        # compressed; some records updated and some added; the table queried. What it builds
        # besides is garbage.
        rng = self.random
        tag = self.records[rng.randrange(len(self.records))]["tags"][0]
        found = [each for each in self.records if tag in each["tags"]]
        top = sorted(found, key=lambda each: each["score"], reverse=True)[:100]
        per_tag = collections.Counter(t for each in self.records for t in each["tags"])
        spread = statistics.pstdev(each["score"] for each in found) if len(found) > 1 else 0.0
        report = gzip.compress(json.dumps({"tag": tag, "top": top, "per_tag": per_tag.most_common(1000)}).encode())
        for _ in range(len(self.records) // 100):
            self.by_id[rng.randrange(len(self.records))]["meta"]["views"] += 1
        for _ in range(len(self.records) // 200):
            self.add()
        self.db.commit()
        kinds = self.db.execute("SELECT kind, count(*) AS n FROM events GROUP BY kind ORDER BY n DESC LIMIT 5").fetchall()
        self.expected = self.digest()
        return {"tag": tag, "found": len(found), "spread": spread, "report_bytes": len(report), "kinds": len(kinds), "records": len(self.records), "rss_bytes": rss_bytes()}

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
        # The first exec after a restore, as an agent's often is: a look at what the session holds, not a walk of it.
        return {"marker": self.marker, "records": len(self.records), "last": self.records[-1]["id"]}

    def verify(self):
        # Everything the session holds, read and compared with what the last exec left.
        return {"marker": self.marker, "memory": self.digest() == self.expected, "children": None, "file": None, "socket": None, "running": None, "records": len(self.records), "rss_bytes": rss_bytes()}


bench = Bench(PARAMS)
print("BENCH:" + json.dumps({"marker": bench.marker, "records": len(bench.records), "docs": len(bench.docs), "checkpointed": bench.checkpointed, "pids": [os.getpid()], "rss_bytes": rss_bytes()}))
