# AdaptiveFetch — Technical Architecture

## Overview

AdaptiveFetch is a batch HTTP fetcher that samples its own resource usage before every batch and re-tunes itself in response. Memory pressure resizes the batch, latency switches payload size and concurrency, process RSS triggers HTTP client recycling, and low disk halts the run cleanly with resumable state. Progress checkpoints incrementally; results go to local files (JSON/CSV/Excel) or S3.

**Design principle:** The batch size is an output, not an input. A constant chosen before the run cannot track memory, response size or link quality — all of which move during the run. So the tool measures itself and decides, rather than following a plan made in advance.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLI (adaptive-fetch)                               │
│                                                                         │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────────────┐  │
│  │   run        │   │   probe          │   │   plan                 │  │
│  │ - fetch list │   │ - current        │   │ - starting batch size  │  │
│  │ - sink/format│   │   snapshot       │   │ - recycle threshold    │  │
│  │ - resume     │   │ - decision it    │   │   for this workload    │  │
│  │ - batch size │   │   would produce  │   │ - no fetching          │  │
│  └──────┬───────┘   └──────────────────┘   └────────────────────────┘  │
└─────────┼───────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   RESOURCE SAMPLING (core/resources.py)                   │
│                                                                         │
│  read_memory()      -> psutil.virtual_memory()  percent + available MB   │
│  read_process_rss() -> psutil.Process().rss     this process footprint   │
│  read_disk_free()   -> psutil.disk_usage()      walks up to real parent  │
│  probe_latency()    -> socket.create_connection TCP connect time, or None│
│                                                                         │
│  snapshot() -> ResourceSnapshot   (frozen dataclass, 5 fields)           │
└─────────┬───────────────────────────────────────────────────────────────┘
          │  one snapshot per batch
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     GOVERNOR (core/governor.py)                           │
│                     ResourceGovernor.evaluate(snap)                       │
│                                                                         │
│  1. DISK HALT      free < disk_halt_mb  -> return halt_reason, stop      │
│                    (checked FIRST, before any resizing)                  │
│                                                                         │
│  2. CLASSIFY       memory >= 90  -> CRITICAL                             │
│                    memory >= 80  -> HIGH                                 │
│                    memory <= 60  -> LOW                                  │
│                    else          -> NORMAL                               │
│                                                                         │
│  3. RESIZE         CRITICAL -> floor, and lower _ceiling to prev//2      │
│                    HIGH     -> prev // 2                                 │
│                    NORMAL   -> hold, reset _healthy_streak               │
│                    LOW      -> +25%, only after growth_patience checks   │
│                                                                         │
│  4. RECYCLE        rss >= recycle_at_mb -> recycle_client = True         │
│                    threshold scales with total_items (2000MB..800MB)     │
│                                                                         │
│  5. NETWORK        latency > slow -> MINIMAL payload, concurrency // 2   │
│                    latency < fast -> FULL payload, full concurrency      │
│                    latency None   -> leave mode unchanged                │
│                                                                         │
│  GovernorDecision(batch_size, concurrency, payload_mode,                 │
│                   recycle_client, halt_reason, pressure, notes)          │
└─────────┬───────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      FETCH LOOP (core/fetcher.py)                         │
│                                                                         │
│  while cursor < len(pending):                                            │
│      decision = governor.evaluate(sampler())                             │
│      record decision in report.decisions                                 │
│                                                                         │
│      if decision.should_halt:    checkpoint.halt(reason); break          │
│      if decision.recycle_client: await client.aclose(); client = None    │
│                                                                         │
│      chunk = pending[cursor : cursor + decision.batch_size]              │
│      semaphore = Semaphore(decision.concurrency)                         │
│      results = await gather(*(bounded(i) for i in chunk))                │
│                                                                         │
│      for record in results: checkpoint.mark(record["id"], record)        │
│                                                                         │
│  Per-record: 1 + retries attempts, backoff 2**(n-1),                     │
│              final failure -> {"status": "error", "error": "..."}        │
│              never raises into gather                                    │
└─────────┬───────────────────────────────────────────────────────────────┘
          │
   ┌──────┴────────────────────────┐
   ▼                               ▼
┌──────────────────────────┐  ┌────────────────────────────────────────┐
│  CHECKPOINT              │  │  SINKS (sinks/)                         │
│  core/checkpoint.py      │  │                                         │
│                          │  │  Sink (ABC): write(), is_available()    │
│  progress.json           │  │                                         │
│   - completed: set[str]  │  │  ┌────────────────┐ ┌────────────────┐ │
│   - records: list[dict]  │  │  │ LocalSink      │ │ S3Sink         │ │
│   - halted_reason        │  │  │ results.json   │ │ put_object     │ │
│                          │  │  │ results.csv    │ │ prefix/UTC-ts/ │ │
│  save every N via        │  │  │ results.xlsx   │ │ results.json   │ │
│  tmp file + os.replace   │  │  │ summary.json   │ │ summary.json   │ │
│  (atomic)                │  │  └────────────────┘ └────────────────┘ │
└──────────────────────────┘  └────────────────────────────────────────┘
```

---

## Components

### 1. CLI (`src/adaptive_fetch/cli.py`)

Three Click commands. Entry point `adaptive-fetch`.

| Command | Purpose |
|:--|:--|
| `run INPUT_PATH` | Fetch every ID. Options: `--output`, `--base-url`, `--sink`, `--format`, `--batch-size`, `--concurrent`, `--s3-bucket`, `--s3-prefix`, `--resume/--no-resume`, `--log-level` |
| `probe` | Print the current snapshot and the decision it produces. The debugging command — answers "why is this run behaving like this" |
| `plan INPUT_PATH` | Print starting batch size and recycle threshold for a workload without fetching |

CLI flags are collected into a dict, `None` values dropped, and the remainder passed to `Settings(**overrides)` so environment variables stay in effect for anything not passed explicitly.

`_configure_logging()` sets the root level and then forces `httpx`, `httpcore`, `botocore`, `boto3` and `urllib3` down to WARNING. Without this, httpx logs one INFO line per request and the governor's decisions — the actually interesting output — are buried.

### 2. Resource Sampling (`src/adaptive_fetch/core/resources.py`)

Pure reader functions, no state:

- **`read_memory()`** — returns `(percent, available_mb)` from `psutil.virtual_memory()`.
- **`read_process_rss()`** — `psutil.Process().memory_info().rss` in MB. Deliberately separate from system memory: a modest process on a busy machine and a bloated process on an idle one need different responses.
- **`read_disk_free()`** — walks up parent directories until it finds one that exists before calling `psutil.disk_usage()`, so it works before the output directory has been created.
- **`probe_latency(url)`** — times a `socket.create_connection()`. Returns `None` for an empty URL, an unresolvable host, a malformed URL, or any `OSError`. `None` means "no reading", which the governor treats as "leave payload mode alone" rather than "assume fast".
- **`snapshot(disk_path, probe_url)`** — assembles one `ResourceSnapshot`.

Two sizing helpers:

- **`recycle_threshold_mb(total_items)`** — 2000MB at ≤10 items, 1600 at ≤50, 1200 at ≤150, 1000 at ≤300, 800 above. Small jobs can afford a fat process; large jobs need frequent recycling.
- **`initial_batch_size(available_mb, min, max)`** — 512 above 16GB, 256 above 8GB, 128 above 4GB, 64 above 2GB, else 32, clamped to bounds.

`ResourceSnapshot` is `@dataclass(frozen=True)` with a `latency_known` property. Frozen because the governor must not be able to mutate its own input.

### 3. Governor (`src/adaptive_fetch/core/governor.py`)

The decision layer. `evaluate(snap) -> GovernorDecision`.

Two enums (`PayloadMode`, `Pressure`), one frozen decision dataclass, one mutable `GovernorLimits` config dataclass, and the `ResourceGovernor` class.

Governor state across calls:

| Field | Role |
|:--|:--|
| `_batch_size` | Current size, carried between batches |
| `_ceiling` | Set on CRITICAL to `prev // 2`. Only ever lowered. Caps `_clamp()` |
| `_healthy_streak` | Consecutive LOW readings. Reset by NORMAL, HIGH and CRITICAL |
| `_payload_mode` | Sticky between calls — a mid-range latency leaves it alone |
| `_concurrency` | Halved on slow latency, restored on fast |
| `recycle_at_mb` | Fixed at construction from `total_items` |

`_clamp()` enforces `min_batch_size <= value <= min(max_batch_size, _ceiling)`. It is called in `__init__` too, which is why `_ceiling` must be assigned before `_batch_size`.

Ordering inside `evaluate()` is load-bearing: the disk halt returns before any resizing happens, so a run that is about to lose the ability to write its checkpoint does not first mutate its own state.

### 4. Fetch Loop (`src/adaptive_fetch/core/fetcher.py`)

**`load_ids(path)`** — CSV (finds `id`, `record_id` or `sku`, else first column) or one ID per line for anything else.

**`Fetcher`** takes `(cfg, fetch_one=None, sampler=None)`. Both are injectable, which is what makes the loop testable: tests pass a fake fetcher and a scripted snapshot sequence. Defaults are the real httpx call and the real psutil sampler.

- `_get_client()` lazily builds `httpx.AsyncClient`. Import is deferred so `probe` and `plan` do not pay for it.
- `_recycle()` closes the client and nulls it; the next fetch rebuilds.
- `_http_fetch()` formats `record_path`, adds `?fields=id,status` in MINIMAL mode, raises for status, returns body plus byte count.
- `_fetch_with_retry()` attempts `1 + retries` times with `2 ** (attempt - 1)` backoff. Returns an `error` record rather than raising.
- `run()` orchestrates: load, checkpoint filter, initial sizing, then the governor loop.

`RunReport` carries totals plus `batch_sizes` and a per-batch `decisions` list, which is what makes an adaptive run auditable after the fact.

### 5. Checkpoint (`src/adaptive_fetch/core/checkpoint.py`)

Holds `completed: set[str]`, `records: list[dict]`, `halted_reason: str | None`.

`mark()` increments a counter and calls `save()` on reaching `save_every`. `save()` writes to `progress.json.tmp` then `os.replace()` — atomic, so an interrupted write cannot leave a half-file that breaks the next resume. A `JSONDecodeError` on load is swallowed and treated as no prior state.

`remaining(ids)` filters by list comprehension, so input order is preserved and IDs added between runs are picked up.

### 6. Sinks (`src/adaptive_fetch/sinks/`)

`Sink` ABC with `write(records, summary) -> list[str]` and `is_available() -> bool`.

| Sink | Output | Availability check |
|:--|:--|:--|
| `LocalSink` | `results.{json,csv,xlsx}` plus `summary.json` | Always true |
| `S3Sink` | `{prefix}/{UTC timestamp}/results.json` and `summary.json` | `head_bucket()` — false on empty bucket or any exception |

`LocalSink` orders CSV/Excel columns by a known `FIELD_ORDER` then appends any extra keys alphabetically, so output is stable across runs. Excel colours the status column and imports `openpyxl` lazily with an actionable error naming the extra to install. `boto3` is likewise imported only when the S3 client is first built.

---

## Data Flow

```
1.  adaptive-fetch run data/records.csv --base-url https://api.example.com --format csv
2.  CLI drops None overrides, builds Settings, quiets httpx logging
3.  get_sink("local", cfg) -> LocalSink; is_available() -> True
4.  Fetcher.run() starts. load_ids() reads 60 IDs from the id column
5.  Checkpoint loads .checkpoints/progress.json -> 20 already completed
6.  pending = remaining(all_ids) -> 40; report.skipped = 20
7.  read_memory() -> 2420MB available -> initial_batch_size() -> 64
8.  ResourceGovernor(limits, 64, total_items=60); recycle_at_mb = 1200
9.  LOOP 1: snapshot -> memory 84.9%, disk 38026MB, latency 2ms
10.           -> HIGH pressure, batch 64 -> 32, note "memory 85% high, halved batch"
11.           -> latency 2ms < 200ms fast -> FULL payload, concurrency 8
12.           chunk = pending[0:32], Semaphore(8), gather 32 requests
13.           each: GET /records/{id}, 200 OK, mark() in checkpoint
14. LOOP 2: snapshot -> memory still 85% -> HIGH -> batch 32 -> 16
15.           chunk = pending[32:40] -> only 8 remain, fetch 8
16. cursor == len(pending), loop exits
17. checkpoint.save() flushes the partial batch; client closed
18. LocalSink.write(records, report.as_summary()) -> results.csv + summary.json
19. CLI prints totals, batch size history [32, 8], and written paths
```

---

## Resource Control Design

The whole project turns on one idea: **the cost of overshooting and the cost of undershooting are not comparable, so the response to them should not be symmetric.**

### Shrink fast, grow slow

Overshoot means the OOM killer takes the process and everything since the last checkpoint is gone. Undershoot means the job runs slower. Treating those as equal-and-opposite errors — halve on pressure, double when healthy — is the obvious implementation and it oscillates.

The oscillation has a specific cause worth naming: after the batch halves, the next reading looks healthy *partly because the batch just halved*. The controller is reading its own last action and interpreting it as evidence about the environment. Doubling on that reading walks straight back into the wall.

Three mechanisms damp it:

1. **`growth_patience`** — three consecutive LOW readings before growing at all. A single post-shrink reading cannot trigger growth.
2. **25% growth steps** rather than doubling. A wrong growth decision costs a quarter step.
3. **`_ceiling`** — after CRITICAL at size 128, the ceiling drops to 64 and never rises. 128 is known-unsafe for this machine and this workload; there is no reason to retest it.

`_healthy_streak` resets on NORMAL as well as on pressure. Only sustained LOW counts, so drifting around the 60–80% band never accumulates toward growth.

### Why process RSS is separate from system memory

They answer different questions. System memory percent says whether the machine is under pressure, which is about how much work to attempt at once. Process RSS says whether *this process* has accumulated state it should shed, which is about connection pools and buffers rather than batch size.

The responses differ accordingly: system pressure resizes the batch, process growth recycles the client. A tidy process on a machine crowded by something else should shrink its batch but has nothing to gain from recycling. A bloated process on an idle machine should recycle and can keep its batch size.

The recycle threshold scaling by workload size is inherited from the internal tool this is modelled on, which tracked browser memory. The reasoning transfers: recycling costs a connection re-establishment, and on a 10-item job you will never recycle often enough for accumulated state to matter, so tolerate 2000MB. On a 5,000-item job you want it early and often, so 800MB.

### Disk is a stop, not a degradation

Memory and network pressure both have a "do less" response. Disk exhaustion has none — you cannot write a smaller checkpoint into no space.

Worse, it is self-destroying. The write most likely to fail on a full disk is the checkpoint, which is the one artefact that would have made the run resumable. So the disk check is first in `evaluate()`, returns before any state mutation, and uses a threshold well above zero (default 100MB) so there is room to write the checkpoint on the way out.

The resulting behaviour — stop deliberately, save state, report why — is strictly better than crashing on an `OSError` with an unknown amount of work lost.

### Latency drives payload, not batch size

A slow link is not a memory problem, and shrinking the batch would not help: the same total bytes still have to cross it. What helps is asking for fewer bytes. So slow latency switches to `PayloadMode.MINIMAL` (a reduced field set) and halves concurrency to stop piling requests onto a struggling service.

Verified against a local server: forcing the slow branch moved responses from **628 bytes to 38 bytes**, a 94% reduction, with all records still fetched.

Payload mode is sticky. Only a reading below `latency_fast_ms` restores FULL, so a mid-range latency does not flap the mode. A `None` reading leaves it alone rather than optimistically assuming a fast link.

### What this design does not handle

**Latency is TCP connect time, which is a proxy.** It measures reachability and round-trip, not throughput or server load. A fast connect to an overloaded server reads as healthy. The honest version would feed observed request durations from the loop back into the governor. The socket probe is kept because it produces a reading *before the first request exists* — the governor needs a value for batch one. Anything better requires a warmup period.

**Decisions are per batch, not per request.** A batch that starts healthy and degrades halfway through is not re-evaluated until it completes. Finer granularity would mean sampling inside the concurrency pool, which is in tension with batching as a unit of work.

**Records accumulate in memory until the end.** The checkpoint holds every result and the sink writes once. For a tool whose premise is memory awareness this is an uncomfortable irony, and past a few hundred thousand records it becomes the binding constraint. Streaming to disk on completion would fix it, at the cost of the single well-formed output document the sink interface currently produces.

**All exceptions are retried equally.** A 404 gets the same three attempts and backoff as a timeout. Distinguishing terminal from retryable status codes would be a real improvement.

**A misconfiguration looks exactly like bad data.** Because record-level failures are absorbed, a wrong `--base-url` yields N error rows indistinguishable from N genuinely missing records. This is the direct price of never letting one record kill a batch, and it is why `probe` and `plan` exist as pre-flight checks.

---

## Configuration

All settings are environment variables with the `AF_` prefix, or a `.env` file. No list-typed fields.

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AF_",
        env_file=".env",
        extra="ignore",
    )

    input_path: Path = Path("input.csv")
    output_path: Path = Path("output")
    base_url: str = "http://localhost:8080"
    record_path: str = "/records/{id}"

    batch_size: int = 0
    min_batch_size: int = 8
    max_batch_size: int = 512
    max_concurrent: int = 8

    memory_low_pct: float = 60.0
    memory_high_pct: float = 80.0
    memory_critical_pct: float = 90.0
    growth_patience: int = 3

    disk_warn_mb: float = 500.0
    disk_halt_mb: float = 100.0

    latency_fast_ms: float = 200.0
    latency_slow_ms: float = 800.0
    probe_url: str = ""

    timeout_seconds: float = 30.0
    retries: int = 2

    checkpoint_dir: Path = Path(".checkpoints")
    checkpoint_every: int = 25
    resume: bool = True

    sink: str = "local"
    output_format: str = "json"
    s3_bucket: str = ""
    s3_prefix: str = "adaptive-fetch"
    aws_region: str = "us-east-1"

    log_level: str = "INFO"
```

`batch_size: int = 0` means auto-size from available memory. `probe_url: str = ""` means skip the latency probe entirely, which is the default because the target host is not always the right thing to time.

---

## Running

```bash
bash setup.sh                  # verifies psutil sampling and runs an end-to-end check
bash setup.sh --s3             # + boto3, and checks AWS credentials resolve
bash setup.sh --excel          # + openpyxl
bash setup.sh --all
source .venv/bin/activate

# What does this machine look like, and what would the governor do about it
adaptive-fetch probe
adaptive-fetch probe --probe-url https://api.example.com

# Sizing for a specific workload, without fetching
adaptive-fetch plan data/sample_input.csv

# Fetch
adaptive-fetch run data/sample_input.csv --base-url https://api.example.com

# Pin the batch size, override concurrency
adaptive-fetch run data/sample_input.csv --batch-size 64 --concurrent 4

# Excel out, or straight to S3
adaptive-fetch run data/sample_input.csv --format excel
adaptive-fetch run data/sample_input.csv --sink s3 --s3-bucket my-results-bucket

# Resume is the default. Re-run the same command after an interruption.
adaptive-fetch run data/sample_input.csv --no-resume    # start over instead
```

---

## Testing

```bash
pytest                          # 96 tests
pytest tests/unit -v            # governor, resources, checkpoint, sinks
pytest tests/integration -v     # full loop under injected resource pressure
pytest --cov=adaptive_fetch --cov-report=term-missing
```

Both injection points exist for testing. `Fetcher(cfg, fetch_one, sampler)` takes a fake fetcher and a scripted snapshot sequence, so "batches shrink when memory spikes at batch two" and "the run halts when disk drops" are ordinary fast tests — no memory exhaustion, no real disk, no network.

The governor being a pure function of a snapshot is what makes the pressure ladder testable at all: every band, boundary and clamp is a constructed `ResourceSnapshot` and an assertion.

Coverage by file:

- `test_governor.py` — pressure bands including exact boundaries (60/80/90), shrink-fast-grow-slow, patience and streak reset, ceiling after critical, min/max clamps, disk halt priority over memory, network degradation and recovery, workload-scaled recycling
- `test_resources.py` — recycle threshold bands and monotonicity, initial batch ladder, real psutil reads, latency probe returning `None` on empty/unresolvable/malformed input
- `test_checkpoint.py` — atomic temp-file write, corrupt-file tolerance, save batching, resume ordering, new IDs between runs, halt persistence
- `test_sinks.py` — factory dispatch, all three local formats, stable column ordering, empty records, S3 guards without credentials
- `integration/test_fetch_loop.py` — input parsing, happy path, failure isolation, governor driving the loop mid-run, resume across runs

Not covered: the real `_http_fetch` against a live service, and `S3Sink.write` against real S3. Both were verified manually — the fetch path against a local HTTP server (60/60 records, batch sizes stepping 32→16→8→4 under genuine 84% machine memory pressure), the S3 path only as far as its guard behaviour.
