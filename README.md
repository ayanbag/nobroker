<p align="center">
  <img src="https://raw.githubusercontent.com/ayanbag/nobroker/master/assets/banner-git.png" alt="nobroker — the job queue with nobody in the middle" width="100%">
</p>

<p align="center">
  <a href="https://github.com/ayanbag/nobroker/actions/workflows/ci.yml"><img alt="tests" src="https://img.shields.io/github/actions/workflow/status/ayanbag/nobroker/ci.yml?branch=main&label=tests&style=flat-square&labelColor=12160F&color=2C6A4E"></a>
  <a href="https://pypi.org/project/nobroker/"><img alt="pypi" src="https://img.shields.io/pypi/v/nobroker?style=flat-square&labelColor=12160F&color=9E3B18"></a>
  <a href="https://pypi.org/project/nobroker/"><img alt="python" src="https://img.shields.io/pypi/pyversions/nobroker?style=flat-square&labelColor=12160F&color=8A6A1F"></a>
  <img alt="dependencies: 0" src="https://img.shields.io/badge/dependencies-0-2C6A4E?style=flat-square&labelColor=12160F">
  <img alt="tests: 124" src="https://img.shields.io/badge/tests-124%20passing-2C6A4E?style=flat-square&labelColor=12160F">
  <img alt="delivery: at-least-once" src="https://img.shields.io/badge/delivery-at--least--once-8A6A1F?style=flat-square&labelColor=12160F">
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-MIT-535E55?style=flat-square&labelColor=12160F"></a>
</p>

<p align="center">
  <b>The job queue with nobody in the middle.</b><br>
  <a href="https://ayanbag.github.io/nobroker/">Docs</a> ·
  <a href="https://ayanbag.github.io/nobroker/playground.html">Interactive playground</a> ·
  <a href="STDLIB.md">STDLIB.md</a> ·
  <a href="deps-proof.txt">Dependency proof</a> ·
  <a href="#limits">Limits</a>
</p>

---

A durable, crash-safe job queue for Python. Enqueue work, lease it, ack it. It
survives `kill -9`, it is safe across processes, and it needs no Redis, no
RabbitMQ, no server, and **no dependencies at all** — just the standard library
and a file.

The name is the thesis. Every other job queue makes you run a broker. This one
doesn't.

> Built for the [Zero Dependency Hackathon](https://zerodepshack.com) — **Track D, Data & Storage**.

---

## Why this exists

You want a background job queue. Your options today:

| | What you install | What you operate |
|---|---|---|
| Celery | `celery`, `kombu`, `billiard`, `vine`, `amqp`, … | a Redis or RabbitMQ server |
| RQ | `rq`, `redis` | a Redis server |
| Dramatiq | `dramatiq`, `pika` or `redis` | a broker |
| **nobroker** | **nothing** | **nothing** |

For a single machine — a cron box, a CLI tool, a desktop app, a small service, a
CI runner, a Raspberry Pi — the broker is pure operational overhead. The durable
storage and the mutual exclusion you actually need are both already in your
kernel. nobroker is what is left when you use them directly.

**It is not a Celery replacement for a fleet.** It does not distribute work
across machines and never will. See [Limits](#limits).

---

## Install

```bash
pip install nobroker
```

That installs one package and nothing else — `dependencies = []` is not a
rounding error, it is the product.

Or skip `pip` entirely — from a fresh clone, with nothing installed:

```bash
git clone https://github.com/ayanbag/nobroker && cd nobroker

python make.py demo      # end-to-end tour: enqueue, retry, crash, recover, compact
python make.py build     # -> dist/nobroker.pyz, the whole tool as one 40 KB file
python dist/nobroker.pyz --help
```

Requires Python 3.11 or newer. That is the entire dependency list. You can also
just copy `src/nobroker/` into your project — it is pure standard library, so it
will run wherever your code does.

### The build is one step, and needs nothing either

`python make.py build` produces `dist/nobroker.pyz`: a single 40 KB file that
runs on any Python 3.11+, with no install and nothing unpacked to disk.

```bash
PYZ=$PWD/dist/nobroker.pyz
cd /tmp                        # anywhere at all: no install, no PYTHONPATH
python "$PYZ" --dir ./q enqueue '{"resize":"photo.jpg"}'
python "$PYZ" --dir ./q stats
```

No compiler, no bundler, no `pip install` — that is `zipapp` from the standard
library. `shiv` and `pex` exist to solve the *hard* part of single-file packaging,
which is vendoring third-party dependencies and their compiled extensions. There
are none here, so the hard part is absent.

**`make` also works** (`make build`, `make test`, …) and is a one-line-per-target
delegation to `make.py`, so the two cannot drift. `make.py` is the canonical one
on purpose: `make` is a **separately installed program**, so a build that requires
it has a dependency that appears in no manifest — and it is absent by default on
Windows, where `make build` fails before it can print anything. The claim is that
you need Python and nothing else, and that should be true of the build too.

---

## Verify the zero-dependency claim in one command

```bash
python make.py check-deps
```

```
python:   3.11.9 (win32)
scanned:  src/**/*.py, tests/**/*.py, tools/**/*.py, examples/**/*.py
method:   ast.parse + sys.stdlib_module_names (no imports executed)

standard library modules imported (31):
  argparse             examples/video_demo.py, src/nobroker/cli.py, tools/check_deps.py
  ast                  tools/check_deps.py
  …
first-party modules (1):
  nobroker

third-party dependencies: 0

VERDICT: nobroker runs on the Python standard library alone.
```

It parses every file in `src/`, `tests/`, `tools/` and `examples/` with `ast` —
reading the source **without importing it**, since importing a module to inspect
it runs its top-level code — and checks each imported top-level module against
`sys.stdlib_module_names`, the frozen set maintained by the people who decide what
the standard library is. The committed output is [deps-proof.txt](deps-proof.txt),
and CI fails the build if it ever changes.

Proving this with `deptry` would have been self-refuting, so it is 100 lines of
`ast` instead. The claim is checked at three levels: source imports, the manifest
(`dependencies = []`, empty [requirements.txt](requirements.txt)), and
`Requires-Dist` inside the built wheel.

---

## Thirty seconds

```python
from nobroker import Queue, Worker

q = Queue("./jobs")
q.enqueue({"send_email_to": "ada@example.com"})

def handle(job):
    send_email(**job.payload)      # raising = retry with backoff

Worker(q, handle).run()            # Ctrl-C finishes in-flight jobs, then exits
```

Or without a worker, if you want the loop yourself:

```python
job = q.lease_one()                # invisible to everyone else for 30s
if job:
    try:
        do_the_work(job.payload)
        q.ack(job)                 # done
    except Exception as exc:
        q.nack(job, error=str(exc))  # retry later, or DLQ after max_attempts
```

Or from a shell:

```bash
nobroker enqueue '{"resize": "photo.jpg"}' --priority 5
nobroker stats
nobroker work myapp.tasks:handle --concurrency 4   # see examples/handlers.py
nobroker dlq --requeue              # after you fix the bug
nobroker inspect                    # read the raw log, record by record
```

`examples/handlers.py` has runnable handlers you can point that at from a clone,
including a slow one built for killing mid-job:

```bash
python make.py build
python dist/nobroker.pyz --dir /tmp/q enqueue '{"report":"q3"}'
python dist/nobroker.pyz --dir /tmp/q work examples.handlers:slow
```

---

## Delivery semantics: **at-least-once**

Read this section before you use nobroker for anything.

**A job can be delivered more than once. Your handlers must be idempotent.**

This is not a limitation nobroker will fix in a later version — it is the
strongest honest guarantee any queue of this shape can offer. Here is the exact
window:

1. A worker leases a job and starts running the handler.
2. The handler completes its side effect (the email is sent, the row is written).
3. The worker is killed before it can call `ack()`.
4. The lease expires, the job becomes visible again, another worker runs it.

The email is sent twice. No amount of engineering closes that window, because
step 2 and step 3 are in different systems. Exactly-once would require the
handler's side effect and the queue's acknowledgement to commit in a *single*
transaction — which means the queue must live inside your database, and then it
is not a general-purpose queue any more.

**What nobroker does instead:**

- **Fencing tokens.** Every lease carries a token. If your lease expires and the
  job is redelivered elsewhere, your `ack()` is *rejected* with `NotLeasedError`
  rather than silently completing someone else's delivery. You find out.
- **Lease heartbeats.** `Worker` extends the lease of a running handler, so a
  slow-but-healthy worker is not a duplicate-work source. Only real failures
  cause redelivery.
- **Idempotent enqueue.** `q.enqueue(payload, job_id="order-42")` de-duplicates
  on the key. This is the one place exactly-once *is* honestly available, because
  de-duplication on a key is something a log can actually do. A retried HTTP
  request that enqueues twice schedules one job.

---

## What it guarantees

| Property | How |
|---|---|
| If `enqueue()` returned, the job is on disk | `fsync` before return, always (unless you pass `fsync=False`) |
| A crash at any point leaves every unacked job recoverable | Append-only log; the interrupted write is the only thing lost, and its caller never got a return value |
| A torn write is detected, never silently applied | CRC32 on every record; recovery truncates back to the last clean one |
| Log damage is reported, not hidden | `queue.recovery.repaired`, and `nobroker recover` |
| Two processes never lease the same job | Kernel file lock held across the whole read-modify-append |
| A dead worker's jobs come back | Visibility timeout; the kernel releases the lock on death |
| Replaying a log twice gives identical state | No handler reads the clock or calls `random` — see below |
| Compaction is all-or-nothing | New generation written and fsynced, then one atomic pointer flip |

### The property that makes the rest testable

**Replay is a pure function of the log.** Nothing in the replay path reads the
clock, generates a UUID, or samples jitter. Every non-deterministic value — the
lease deadline, the jittered retry time, the job id — is decided *once* by the
writer and recorded as an absolute value.

That is why the test suite can take a real log, truncate it at **every single
byte offset**, and assert that recovery lands somewhere consistent at each one.
A crash can only interrupt a write at a byte boundary, so correctness at all
~1,300 boundaries of a real log is correctness for any crash that log could have
suffered.

---

## How it works

```
jobs/
  emails.000001.log     the write-ahead log — the only durable state
  emails.current        one line: which generation is authoritative
  emails.lock           the cross-process lock
```

A new queue has only two of those: `emails.000000.log` and a zero-byte lock file
whose only job is to exist, so the kernel has something to arbitrate on. The
`.current` pointer appears at the first compaction, when there is finally more
than one generation to choose between.

Everything in memory — the priority heap, the lease table, the DLQ — is a *cache*
of the log. There is no second source of truth to keep consistent with it.

**Record framing.** Records are appended, never modified:

```
file header:  <8s magic><H version><I generation><H reserved>    16 bytes
record:       <I length><B type><I crc32><json payload>          9 + n bytes
```

The length prefix catches an incomplete record. The CRC catches a full-length
record with a hole in the middle — the failure that would otherwise be applied
silently. The payload is JSON on purpose: a durability format you cannot read
with your eyes is one you cannot debug. `nobroker inspect` prints it.

**Every operation is four beats:**

1. Take the file lock.
2. Read forward from our last offset — applying whatever peers appended.
3. Reclaim leases whose visibility timeout expired.
4. Append the new records, `fsync`, then apply them in memory.

Step 2 is what makes multi-process work with no coordinator: a process that has
been idle for an hour simply reads forward and finds out what happened. Step 4's
ordering is not negotiable — the record reaches disk *before* memory changes, so
a crash between the two replays to the same state.

**Compaction** writes a new numbered generation containing one record per live
job, then flips `emails.current`. That flip is the commit point and it is a
single atomic `os.replace` of a file nobody holds open. Before it, the old
generation is authoritative and the new file is ignorable garbage; after it, the
reverse. Peers notice by comparing an integer.

---

## Benchmarks

Measured on the development machine (Windows 11, NVMe SSD, Python 3.11.9) with
`python make.py bench`. **Run it yourself — that is the only number that matters
for your hardware,** and NVMe versus spinning rust changes the fsync rows by
orders of magnitude.

| Operation | ops/sec | µs/op | Notes |
|---|---:|---:|---|
| enqueue (fsync per job) | 1,348 | 741.7 | the durability guarantee, paid one job at a time |
| enqueue_many (one fsync) | 57,676 | 17.3 | batching amortises the fsync; same durability at the batch boundary |
| enqueue (fsync=False) | 6,014 | 166.3 | **not durable** — a machine crash loses recent jobs. Shown for contrast |
| lease+ack round trip | 1,433 | 697.9 | leases in batches of 100, acks individually |
| cold-start replay | 47,732 | 21.0 | full replay on open, including CRC of every record |
| compact | 537,184 | 1.9 | 2.6 MB → 16 bytes |
| nack + reschedule | 6,712 | 149.0 | computes backoff, rewrites availability, re-heaps |

### Is it faster than Redis?

**No.** Redis will do 50,000–100,000 ops/sec on a loopback connection. nobroker
does ~1,300 durable enqueues/sec, because it calls `fsync` and Redis (by
default) does not. That is not a fair fight in either direction: nobroker is
paying for a guarantee Redis is not making.

The honest comparisons:

- **Against Redis with `appendfsync always`** — the configuration that makes the
  same promise — you are in the same order of magnitude, and nobroker saves you a
  network hop and a server.
- **Against Redis default (`appendfsync everysec`)** — Redis wins on throughput
  and can lose up to a second of acknowledged writes. `Queue(fsync=False)` is the
  comparable nobroker setting, and it is labelled "not durable" everywhere it
  appears.
- **On batches**, `enqueue_many` does 57k/sec durably, because fsync costs the
  same for one record as for a thousand.

If you need 100k jobs/sec, run a broker. If you need 1,000 jobs/sec that are
still there after the power cut, this is simpler and there is nothing to operate.

---

## Limits

Stated plainly, because a naive implementation that is honest about its corners
beats a fast one that hides them.

- **Single machine only.** The lock is a kernel file lock; it does not work over
  NFS or SMB and it will not coordinate two hosts. There is no distributed mode
  and there will not be one.
- **At-least-once, never exactly-once.** See above. Handlers must be idempotent.
- **Polling, not push.** Workers poll (default every 100 ms). A broker-less queue
  has nobody to send a notification. An idle worker costs one `stat` and one short
  read per poll; it is cheap, but it is not free and it is not zero-latency.
- **The whole index lives in memory.** One `Job` per job, all of them resident.
  Roughly 500 bytes each, so a million jobs is ~500 MB. Fine for the workloads
  this targets; not a database.
- **`stats()` is O(n).** It counts by iterating every job. Honest and simple; if
  you have a million jobs and poll stats in a tight loop, you will notice.
- **Recovery reads the entire log at startup.** ~48k records/sec, so a 1M-record
  log takes ~20 seconds to open. Compact regularly.
- **No result backend, no chaining, no workflows, no cron DSL, no async
  handlers.** All deliberately out of scope.
- **Payloads must be JSON-serialisable.** No bytes, no arbitrary objects. That is
  the price of a log you can read.
- **Windows caveats.** Directory `fsync` does not exist there, so the durability
  of a rename is weaker than on POSIX (the file contents are still fsynced). The
  crash tests use `multiprocessing` with `spawn` instead of `os.fork`, which does
  not exist on Windows.
- **Clock-dependent.** Visibility timeouts and delays use the wall clock. A large
  backwards clock jump (NTP step, not slew) can delay reclaims by that amount.

---

## Questions a reviewer will ask

### "Why not just use `sqlite3`? It's in the standard library too."

SQLite would give me storage and transactions. It would not give me any of the
things this project actually is: lease semantics, visibility timeouts, backoff
with jitter, fencing tokens, a dead-letter queue, priority ordering with delayed
jobs, or crash-recovery semantics I can reason about.

A job queue on SQLite is *all of this same code*, written on top of a query layer
I do not need, plus a schema, plus the `BEGIN IMMEDIATE`/`busy_timeout` dance to
make polling workers not livelock. It would be more code, not less, and the
durability story would be "SQLite handles it" — which is true, and also means I
would have learned nothing about the thing this hackathon is about.

The honest counterpoint: SQLite's storage engine is vastly better tested than
mine. If you need a queue you would bet a company on today, that is a real
argument for it. If you want to see what the primitives underneath actually cost,
this is the more interesting build.

### "Why is `enqueue` only ~1,300/sec?"

Because it calls `fsync` and waits. That is the product. `enqueue_many` gets 57k
because one fsync covers the batch. `fsync=False` gets 6k and is not durable.

### "Isn't a file lock per operation slow?"

It was — it was 44% of a non-durable enqueue, spent opening and closing the lock
file. The descriptor is now held for the queue's lifetime and only the lock is
taken and released, which was a 2.4× improvement with no semantic change. Both
`flock` and `msvcrt.locking` associate the lock with the open file description,
so this is exactly as exclusive as reopening each time.

### "What happens if two processes compact at once?"

They cannot. Compaction runs inside the same lock as everything else.

---

## API

```python
Queue(path, name="default", *, fsync=True, visibility_timeout=30.0,
      max_attempts=5, backoff=BackoffPolicy(), lock_timeout=10.0)

  .enqueue(payload, *, priority=0, delay=0.0, max_attempts=None, job_id=None) -> Job
  .enqueue_many(payloads, *, priority=0, delay=0.0, ...) -> list[Job]
  .lease(count=1, *, visibility_timeout=None) -> list[Job]
  .lease_one(*, visibility_timeout=None) -> Job | None
  .ack(job)                                    # raises NotLeasedError if stale
  .nack(job, *, error=None, delay=None) -> Job # retry, or DLQ if exhausted
  .extend(job, seconds) -> Job                 # buy a slow handler more time
  .get(job_id) -> Job
  .stats() -> QueueStats
  .list_jobs(state=None, *, limit=None) -> list[Job]
  .dlq(*, limit=None) -> list[Job]
  .requeue_dead(job_id=None) -> int            # None revives the whole DLQ
  .purge() -> int
  .compact() -> CompactionResult
  .close()

Worker(queue, handler, *, concurrency=1, poll_interval=0.1,
       max_jobs=None, idle_timeout=None, handle_signals=True).run() -> WorkerStats

BackoffPolicy(base=1.0, factor=2.0, max_delay=300.0, jitter=0.5)
```

Errors all derive from `NobrokerError`: `NotLeasedError`, `JobNotFoundError`,
`LockTimeoutError`, `CorruptLogError`, `CompactionError`, `QueueClosedError`,
`SerializationError`.

---

## Development

`python make.py` on its own lists every task. Each has a `make` alias.

```bash
python make.py build         # dist/nobroker.pyz — one runnable file, stdlib zipapp
python make.py test          # 124 tests, ~42s (the truncation sweep dominates)
python make.py test-quick    # ~8s, skips that sweep
python make.py bench         # throughput on your machine
python make.py bench-md      # the same, as a Markdown table for this README
python make.py check-deps    # fails if any non-stdlib import exists anywhere
python make.py deps-proof    # regenerate deps-proof.txt on this machine
python make.py demo          # the tour
python make.py video-demo    # the same material, paced for screen recording
python make.py clean         # caches, scratch queues, build output
```

Arguments pass through: `python make.py bench --jobs 100`, or
`python make.py video-demo --scenes 6` to run one scene of the demo alone.

```
src/nobroker/     11 modules, in strict dependency order (errors -> … -> queue)
src/__main__.py   entry point for the zipapp bundle
tests/            8 files, 124 tests, unittest only
tools/            check_deps.py (the dependency proof), build_pyz.py (the bundle)
examples/         demo.py (the tour), video_demo.py (paced), handlers.py (for `work`)
web/              docs site and playground — two static files, no build step
make.py           every task; the Makefile delegates here
```

**~3,000 lines of library, ~1,700 lines of tests, 0 dependencies.**

The test suite deliberately over-invests in one thing:

- `test_crash_recovery.py` — hard kills via `os._exit(9)`, and the truncation
  sweep over every byte offset of a real log
- `test_concurrency.py` — four separate OS processes racing on one queue, each
  recording which job ids it received; the parent asserts the sets are disjoint
  and complete

Both of those caught real bugs during development. The multi-process test found
a stale cached file offset that made peers overwrite each other's records; the
compaction test found `os.open` defaulting to text mode on Windows and corrupting
the log. Neither would have shown up in a single-process happy-path test.

See **[STDLIB.md](STDLIB.md)** for all 22 "I would normally have installed X"
decisions, including the five places the standard library was genuinely the worse
option.

---

## License

MIT.
