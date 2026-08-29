# STDLIB.md

Every place I would normally have typed `pip install`, and what the standard
library gave me instead. Twenty-two entries.

The rule for this list: each entry names a **real package I have used before**,
the **actual stdlib module** that replaced it, and **why the replacement was
adequate** — or where it was not. Entries where the stdlib version is genuinely
worse are marked, because pretending otherwise would be the kind of hidden corner
this hackathon exists to expose.

---

## The big one

### 1. Redis / RabbitMQ / a broker process → `os` + `fcntl` + an append-only file

**Would have installed:** `redis` (plus a Redis server), or `pika` (plus
RabbitMQ).

**Used instead:** an append-only file, `os.fsync` for durability, and
`fcntl.flock` for mutual exclusion between processes.

A broker is three things: durable storage, a serialisation point, and a network
endpoint. On a single machine you do not need the third, and the operating system
already provides the first two. `fsync` is the durability primitive. `flock` is
the serialisation point — the kernel arbitrates, it is fair, and it is released
automatically when a process dies, which is more than most brokers manage.

What is genuinely lost: cross-machine work distribution. nobroker is
single-machine by design and says so in the README rather than pretending the gap
is not there.

### 2. `celery` / `rq` / `dramatiq` / `huey` → this repository

**Would have installed:** `celery` (which pulls `kombu`, `billiard`, `vine`,
`amqp`, `click`, `tzdata`… ~40 MB with a broker), or `rq` (which needs `redis`).

**Used instead:** ~1,400 lines of Python across nine modules.

The leases, the visibility timeout, the exponential backoff, the DLQ, the
priority ordering — none of that came from the broker in a Celery deployment
either. It is application logic that happens to be shipped inside a package. Once
you have the log and the lock, writing it directly is the smaller of the two
jobs.

---

## Storage and durability

### 3. `msgpack` / `protobuf` / `pickle` → `struct` + `json`

**Would have installed:** `msgpack` for a compact binary record format.

**Used instead:** `struct.Struct("<IBI")` for the fixed 9-byte record frame, and
`json` for the variable payload.

`struct` handles exactly the part that must be byte-exact: a little-endian length,
a type byte, a checksum. `json` handles the part that must be flexible. The
combination is slower and larger than msgpack — measurably, JSON encoding is ~7%
of a non-fsyncing enqueue — and I took that deliberately, because a durability
format you cannot read with your eyes is a durability format you cannot debug.
`nobroker inspect` exists because of this choice.

`pickle` was never an option: it executes arbitrary code on load, which is an
absurd property for a file whose entire job is to be read back after a crash.

### 4. `crc32c` / `xxhash` → `zlib.crc32`

**Would have installed:** `crc32c` for a hardware-accelerated checksum.

**Used instead:** `zlib.crc32`, which ships with Python and runs at GB/s.

Checksums are how recovery distinguishes a complete record from a half-written
one. The requirement is "detects a torn write and a flipped bit", not
"cryptographic". CRC32 does that, and the test suite flips **every single bit of a
record** and asserts each one is caught.

### 5. `atomicwrites` / `python-atomicwrites` → `os.replace` + `os.fsync`

**Would have installed:** `atomicwrites` for a safe file-swap helper.

**Used instead:** `os.replace`, which is documented as atomic on both POSIX and
Windows, plus an `fsync` of the containing directory so the rename itself is
durable.

The package is a thin wrapper over exactly these two calls. Writing them out
directly made it obvious that the directory fsync was needed — a subtlety the
wrapper would have hidden, and one that matters: without it a well-timed power
cut can leave a compacted log that exists but that no directory entry points at.

### 6. `filelock` / `portalocker` / `fasteners` → `fcntl.flock` and `msvcrt.locking`

**Would have installed:** `filelock`, the standard answer for cross-process
locking in Python.

**Used instead:** `fcntl.flock` on POSIX, `msvcrt.locking` on Windows, behind a
25-line shim in `lock.py`.

The reason `filelock` exists is that there is no single portable stdlib call —
not that the stdlib lacks the capability. Both platform primitives are
kernel-backed, which is the property that actually matters: the lock is released
when the file descriptor closes, *including when the process is killed with
SIGKILL*. Note what the shim does **not** contain — no pid file, no stale-lock
timeout, no "is that process still alive" heuristic. Those are what you write
when your lock is not kernel-backed, and they are all subtly wrong.

### 7. `pywin32` → `ctypes` + `msvcrt.open_osfhandle`

**Would have installed:** `pywin32` to call `CreateFileW` with
`FILE_SHARE_DELETE`.

**Used instead:** `ctypes.windll.kernel32.CreateFileW`, handed to the C runtime
with `msvcrt.open_osfhandle`.

Windows will not let you delete or rename a file that anyone has open, unless
every holder opened it with `FILE_SHARE_DELETE` — which `os.open` does not set.
That meant a peer process merely *having the queue open* blocked compaction from
retiring the old log. `ctypes` reaches the same Win32 call `pywin32` would,
without the 10 MB and the build step, and the resulting descriptor is an ordinary
fd that `os.read`/`os.write`/`os.fsync` accept unchanged.

---

## Data structures and scheduling

### 8. `sortedcontainers` / `heapdict` → `heapq`

**Would have installed:** `sortedcontainers` for a sorted set supporting removal.

**Used instead:** two `heapq` heaps plus the lazy-deletion pattern **documented
in the `heapq` module docs themselves**.

`heapq` has no "remove this element", which is the usual reason people reach for
`sortedcontainers`. The documented workaround — leave stale entries in place,
tag each with a version, skip them when they surface — is about eight lines and
avoids the dependency entirely. Reading the `heapq` docs to the end was worth
more here than any package.

The two-heap split (eligible jobs sorted by priority, pending jobs sorted by
availability time) is not a stdlib substitution but it is the design the stdlib
pushed me toward, and it prevents the classic bug where an urgent job scheduled
for tomorrow sits at the head of the queue and starves everything behind it.

### 9. `apscheduler` → `heapq` + wall-clock timestamps

**Would have installed:** `apscheduler` for delayed job execution.

**Used instead:** an `available_at` float on each job and a heap keyed by it.

Delayed delivery is "do not hand this out before time T". That is a comparison,
not a scheduler. No background thread, no timer, no cron parser — the check
happens on the lease path, which is the only moment the answer can matter.

### 10. `tenacity` / `backoff` → `random` + three lines of arithmetic

**Would have installed:** `tenacity` for retry policy with jitter.

**Used instead:** `min(base * factor ** (n - 1), max_delay)` with an
`random.uniform` jitter band, in a frozen dataclass.

Exponential backoff is one expression. The part worth thinking about was not the
formula but *when the dice are rolled*: nobroker samples the jitter at nack time
and records the resulting absolute timestamp in the log, so replaying a log never
re-rolls it. That property — replay is a pure function of the bytes on disk — is
what makes crash recovery testable, and no retry library would have given it to
me.

### 11. `pydantic` / `attrs` / `marshmallow` → `dataclasses` + type hints

**Would have installed:** `pydantic` for the `Job` model and validation.

**Used instead:** `@dataclass(slots=True)` with explicit `to_dict`/`from_dict`.

`slots=True` gives the memory win that matters when a million jobs are resident.
Validation is genuinely thinner than pydantic's — the trade is real. But the
serialisation boundary here is one class with eleven fields, and hand-writing it
bought something back: `from_dict` keeps unknown keys in `_extra` rather than
dropping them, so a log written by a newer nobroker survives a round trip through
an older one.

### 12. `shortuuid` / `nanoid` → `uuid.uuid4().hex`

**Would have installed:** `nanoid` for compact ids.

**Used instead:** `uuid.uuid4().hex`, and `[:16]` of one for lease tokens.

122 bits of randomness with no coordination between processes, which is exactly
the requirement for ids minted by four workers that cannot talk to each other.

---

## Interface

### 13. `click` / `typer` / `argh` → `argparse`

**Would have installed:** `click` for the CLI — subcommands, help, types.

**Used instead:** `argparse` with `add_subparsers` and `set_defaults(func=…)`.

The `set_defaults(func=...)` idiom gives the same "one function per subcommand"
structure as `click`'s decorators in about the same number of lines. What is
genuinely missing: shell completion and colour. Neither is worth a dependency for
an operator tool.

One thing worth stealing: `main()` returns an exit code instead of calling
`sys.exit`, which makes all 21 CLI tests run in-process. Testing the CLI without
spawning a single subprocess is faster and proves more.

### 14. `structlog` / `loguru` → `logging`

**Would have installed:** `loguru`, out of habit.

**Used instead:** `logging.getLogger("nobroker.worker")`.

A library should not configure logging; it should emit to a named logger and let
the application decide. `logging` does that correctly and `loguru`'s main selling
point — nicer defaults — is a thing a *library* should not be imposing anyway.

### 15. `rich` / `tabulate` → f-string field widths

**Would have installed:** `tabulate` for the benchmark and job-listing tables.

**Used instead:** `f"{name:<34}{state:<9}{priority:>4}"`.

Format-spec alignment is a language feature. For fixed-column output it is the
whole of what `tabulate` does.

---

## Testing and tooling

### 16. `pytest` → `unittest`

**Would have installed:** `pytest`, plus `pytest-cov`, plus probably
`pytest-timeout`.

**Used instead:** `unittest`, 124 tests, no plugins.

The hackathon allows dev-only dependencies. This project should not take that
exemption: its entire claim is that the standard library is enough, and Python
ships a test runner. Two `unittest` features earned their keep and I would not
have found them otherwise:

- **`subTest`** turns the exhaustive truncation sweep into ~1,300 independently
  reported cases inside one test method, with the failing byte offset named in
  the output.
- **`addCleanup`** is strictly better than `tearDown` for resources acquired
  partway through a test, because cleanup registers at the point of acquisition.

What is missing versus pytest: bare `assert` rewriting, fixtures, and `-k`
expression negation (`-k 'not slow'` is pytest syntax — `unittest`'s `-k` has no
negation, so the slow sweep is gated behind an environment variable instead).

### 17. `pytest-xdist` / `psutil` / a `kill` subprocess → `os.fork` and `multiprocessing`

**Would have installed:** nothing, but the obvious lazy path is
`subprocess.run(["kill", "-9", str(pid)])`.

**Used instead:** `os.fork()` in the child followed by `os._exit(9)`, with
`multiprocessing` (spawn) as the equivalent on Windows, which has no `fork`.

Shelling out to `kill` would be a hidden dependency on a tool outside Python, and
it does not exist on Windows. `os._exit(9)` is the honest in-process equivalent:
it terminates immediately, skipping `atexit` handlers, `finally` blocks, buffer
flushes and destructors. Whatever is on disk at that instant is all recovery gets
— which is precisely the condition being tested.

### 18. `freezegun` / `time-machine` → parameterised policy objects

**Would have installed:** `freezegun` to control time in the retry tests.

**Used instead:** `BackoffPolicy(base=0.0, jitter=0.0)` and short visibility
timeouts.

Patching the clock is a workaround for a design where the clock is implicit.
Making the delay policy an injected object removed the need: tests that care
about retry *state* set the delay to zero and assert on state, and only the two
tests that genuinely test *timing* sleep at all (for 150 ms).

### 19. `pytest-benchmark` → `time.perf_counter`

**Would have installed:** `pytest-benchmark` for the throughput table.

**Used instead:** `time.perf_counter()` around a loop, in `bench.py`.

`perf_counter` is the monotonic high-resolution clock; the statistical machinery
`pytest-benchmark` adds on top is more rigour than a "how many jobs per second"
table needs, and the results are published with their caveats rather than dressed
up.

### 20. `deptry` / `pipdeptree` / `pip-audit` → `ast` + `sys.stdlib_module_names`

**Would have installed:** `deptry` to verify the zero-dependency claim.

**Used instead:** `tools/check_deps.py` — 100 lines that parse every source file
with `ast` and check each imported top-level module against
`sys.stdlib_module_names`.

Using a third-party tool to prove a zero-dependency claim would be
self-refuting. `ast.parse` reads the source *without importing it*, which matters:
importing a module to inspect it runs its top-level code.
`sys.stdlib_module_names` (Python 3.10+) is the authoritative frozen set,
maintained by the people who decide what is in the standard library. The output
is committed as `deps-proof.txt` and `make check-deps` fails the build if
anything third-party appears.

This one was a genuine surprise. I did not know `sys.stdlib_module_names`
existed, and it turns the "are we actually zero-dependency?" question from a
promise into an assertion.

### 21. `shiv` / `pex` / `PyInstaller` → `zipapp`

**Would have installed:** `shiv` or `pex` to ship the tool as one runnable file.

**Used instead:** `zipapp.create_archive` (stdlib, PEP 441) in
`tools/build_pyz.py`. `make build` produces a 40 KB `dist/nobroker.pyz` that runs
on any Python 3.11+ with no install step.

All three of those bundlers exist to solve the *hard* part of single-file
packaging: vendoring third-party dependencies and their compiled extensions into
one archive and making the import machinery find them at runtime. This project
has no dependencies and no extensions, so the hard part is simply absent and
`zipapp` is the entire job. Installing a bundler here would mean installing a
package whose purpose is to package the packages we do not have — which is close
to a proof that the dependency is unnecessary.

Two details the wrapper script adds over `python -m zipapp` on its own, both
because the CLI form cannot do them: a `filter` that excludes `__pycache__` (a
bytecode cache from whichever interpreter last ran the tests is dead weight in an
archive, since it cannot be used by a different version), and a verification step
that actually executes the built artifact with `--version` before declaring
success.

The archive's `__main__.py` is also a real file rather than the stub `zipapp -m`
generates, because the generated stub calls `main()` and **discards the return
value** — which would make every CLI failure exit 0. `sys.exit(main())` keeps the
exit codes the 21 CLI tests assert on.

### 22. `make` / `invoke` / `nox` / `just` → `argparse` + `subprocess` in `make.py`

**Would have installed:** `invoke` or `nox` as a task runner — or, more subtly,
**relied on `make` being present**, which is the version of this dependency that
hides best.

**Used instead:** `make.py`, a ~150-line task table where each entry is an argv
list run against `sys.executable`.

This one is easy to miss, and it is the most interesting entry in the list for
that reason. A `Makefile` looks free — it isn't a `pip install`, so it doesn't
show up in any manifest. But `make` is a **separately installed program**, and
`CLAUDE.md` §1 is explicit that shelling out to one is "a hidden dependency."
It is genuinely absent on a stock Windows box: `make build` there fails before it
can print anything, which means the project's own headline command didn't work on
the machine it was written on.

So the tasks live in Python and the `Makefile` delegates to them, one line per
target. `make test` still works for the reviewer whose fingers type it; `python
make.py test` works everywhere, including where `make` does not exist. Neither can
drift from the other, because there is only one definition.

`subprocess.run([sys.executable, ...])` is not a shell-out to a third-party tool
— it is the interpreter already running the file. No `shell=True`, no quoting
rules, no per-platform argv differences.

---

## Things the standard library did *not* solve well

Listed because a log of only wins is not a log.

- **No portable file lock.** `fcntl` and `msvcrt` are both fine; the absence of
  one call that works on both is a real gap, and `lock.py` exists only to paper
  over it.
- **`os.open` defaults to text mode on Windows.** A descriptor without
  `O_BINARY` silently rewrites every `0x0A` as `0x0D 0x0A`. This cost me a real
  debugging session: compaction wrote a corrupt log, but only when a record's
  length or checksum field happened to contain a newline byte, so it presented as
  an *intermittent* CRC failure at reopen, far from the actual bug. There is a
  regression test for it now.
- **`os.replace` cannot overwrite an open file on Windows,** and unlike the
  deletion case, `FILE_SHARE_DELETE` does not help — `MoveFileEx` refuses
  regardless. This forced a real design change: instead of overwriting the log in
  place, compaction writes a numbered generation and atomically flips a tiny
  pointer file that nobody holds open. The design is better for it (peers detect
  compaction by comparing an integer instead of stat'ing inodes), but the stdlib
  did not hand it to me.
- **`fsync` on a directory is not available on Windows,** so the rename-durability
  guarantee is weaker there. Documented in the README rather than papered over.
- **`json` is slow** relative to a binary encoder, and it cannot represent bytes.
  Both accepted knowingly in exchange for a readable log.
