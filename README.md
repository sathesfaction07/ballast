# Ballast

A batch fetcher that watches its own resource usage and re-tunes itself while it runs. It reads a list of record IDs, pulls each one over HTTP, and adjusts batch size, concurrency and payload size in response to memory pressure, disk space and network latency. Progress is checkpointed, so an interrupted run resumes where it stopped. Results go to local files or to S3.

---

## The Problem

You have 50,000 record IDs and a service to pull them from. The obvious script is a loop with a fixed batch size.

Pick that number too high and the process gets killed by the OOM reaper somewhere around record 30,000 — and if you were writing results at the end, you lose all of it. Pick it too low and a job that could take twenty minutes takes three hours.

The real problem is that there is no correct number. The right batch size depends on how much memory the machine has free *right now*, how large the responses happen to be, what else is running, and how fast the network is at this moment. All of those change during the run. A constant chosen before the run started cannot track any of them.

Worse, the failure modes are silent and asymmetric:

- **Memory** creeps up as response bodies accumulate. Nothing warns you. The process just dies.
- **Disk** fills up as checkpoints and results are written. The run crashes on a write, usually while trying to save the very progress that would have let you resume.
- **Network** degrades and every request starts timing out. Retries pile on top of a service that is already struggling.

So the tool has to measure itself and react, not follow a plan made in advance.

---

## How It Works

Before every batch, the tool takes a resource snapshot and hands it to a governor. The governor returns a decision: how many records to fetch next, how many at once, whether to request full or trimmed payloads, whether to recycle the HTTP client, or whether to stop entirely.

The core rule is **shrink fast, grow slow**:

| Memory | Reaction |
|:--|:--|
| 90%+ (critical) | Drop straight to the floor batch size, and lower the ceiling so it cannot climb back to where it broke |
| 80–90% (high) | Halve the batch immediately |
| 60–80% (normal) | Hold |
| Under 60% (low) | Grow by 25% — but only after 3 consecutive healthy checks |

The asymmetry is deliberate. Being too large costs you the whole process; being too small costs you some time. Those are not comparable, so the responses are not symmetric either.

Disk and network are handled as degradation rather than failure. Low disk warns, then halts cleanly with the checkpoint saved before results become unwritable. High latency switches to a trimmed payload and halves concurrency instead of letting requests time out.

Process memory is watched separately from system memory. When RSS crosses a threshold, the HTTP client is closed and rebuilt to drop accumulated connection state. That threshold scales with workload size — a 5-record job can afford a fat process, a 50,000-record job cannot.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     CLI (ballast)                          │
│                                                                  │
│    run              probe                 plan                   │
│    fetch a list     show current          show sizing for        │
│                     snapshot + the        this workload          │
│                     decision it implies                          │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                  RESOURCE SAMPLING (resources.py)                 │
│                                                                  │
│   psutil.virtual_memory()  ->  system memory percent + available │
│   psutil.Process().rss     ->  this process's footprint          │
│   psutil.disk_usage()      ->  free space on the output volume   │
│   socket.create_connection ->  TCP connect time as latency       │
│                                                                  │
│   Output: ResourceSnapshot (frozen dataclass)                    │
└───────────────────────────────┬──────────────────────────────────┘
                                │  one snapshot per batch
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                     GOVERNOR (governor.py)                        │
│                                                                  │
│   1. Disk halt check       -> stop before results are unwritable │
│   2. Classify pressure     -> critical / high / normal / low     │
│   3. Resize batch          -> shrink now, grow after patience    │
│   4. Check process RSS     -> recycle client if over threshold   │
│   5. Read latency          -> full or minimal payload            │
│                                                                  │
│   Output: GovernorDecision (batch_size, concurrency,             │
│           payload_mode, recycle_client, halt_reason, notes)      │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                       FETCH LOOP (fetcher.py)                     │
│                                                                  │
│   while records remain:                                          │
│       decision = governor.evaluate(sample())                     │
│       if decision.should_halt:      save checkpoint and stop     │
│       if decision.recycle_client:   close and rebuild client     │
│       take decision.batch_size records                           │
│       fetch them under a semaphore of decision.concurrency       │
│       retry each failure with exponential backoff                │
│       mark every outcome in the checkpoint                       │
│                                                                  │
│   One record failing is recorded as an error, never raised       │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                ┌───────────────┴────────────────┐
                ▼                                ▼
┌───────────────────────────┐    ┌──────────────────────────────┐
│  CHECKPOINT               │    │  SINKS                        │
│  progress.json            │    │                               │
│  - completed ID set       │    │  local -> JSON / CSV / Excel  │
│  - result records         │    │  s3    -> timestamped prefix  │
│  - halt reason            │    │                               │
│  written every N records  │    │  Selected by one setting      │
│  via temp file + replace  │    │                               │
└───────────────────────────┘    └──────────────────────────────┘
```

---

## Project Structure

```
ballast/
├── src/adaptive_fetch/
│   ├── cli.py                 run / probe / plan
│   ├── config.py              Pydantic Settings, AF_ prefix
│   ├── core/
│   │   ├── resources.py       psutil + socket sampling
│   │   ├── governor.py        the control loop
│   │   ├── fetcher.py         async batch execution
│   │   └── checkpoint.py      atomic resume state
│   └── sinks/
│       ├── base.py            Sink ABC
│       ├── local.py           JSON / CSV / Excel
│       └── s3.py              boto3 upload
├── tests/
│   ├── conftest.py            snapshot factory + config fixtures
│   ├── unit/                  governor, resources, checkpoint, sinks
│   └── integration/           full loop with injected pressure
├── docs/adr/
├── data/sample_input.csv
└── setup.sh
```

---

## Running

```bash
bash setup.sh
source .venv/bin/activate

# See what the machine looks like right now and what the governor would do
ballast probe

# See the sizing chosen for a specific workload
ballast plan data/sample_input.csv

# Fetch
ballast run data/sample_input.csv --base-url https://api.example.com

# Pin the batch size instead of auto-sizing
ballast run data/sample_input.csv --batch-size 64

# Excel output
ballast run data/sample_input.csv --format excel

# Upload results to S3 instead of writing locally
ballast run data/sample_input.csv --sink s3 --s3-bucket my-results-bucket

# Resume is on by default. Re-run the same command after an interruption.
# To reprocess everything from scratch:
ballast run data/sample_input.csv --no-resume
```

---

## Usage Reference

### `run INPUT_PATH`

Fetches every ID in the input file. Accepts CSV (looks for an `id`, `record_id` or `sku` column, else the first column) or a plain text file with one ID per line.

| Option | Default | Purpose |
|:--|:--|:--|
| `--output` | `output` | Directory for local results |
| `--base-url` | `http://localhost:8080` | Service base URL |
| `--sink` | `local` | `local` or `s3` |
| `--format` | `json` | `json`, `csv` or `excel` |
| `--batch-size` | auto | Omit to size from available memory |
| `--concurrent` | 8 | Max simultaneous requests |
| `--s3-bucket` | — | Required when `--sink s3` |
| `--s3-prefix` | `ballast` | Key prefix; a UTC timestamp is appended |
| `--resume/--no-resume` | resume | Skip IDs already in the checkpoint |
| `--log-level` | `INFO` | Governor decisions log at WARNING |

### `probe`

Prints the current snapshot and the decision it produces. Use it to understand why a run is behaving the way it is.

```
memory used       47.2%
memory available  8431MB
process rss       38MB
disk free         104839MB
latency           not probed

pressure          low
batch size        256
concurrency       8
payload mode      full
recycle client    False
```

### `plan INPUT_PATH`

Shows the starting batch size and recycle threshold for a given workload without fetching anything.

---

## Testing

```bash
pytest                          # everything
pytest tests/unit -v            # governor logic, no I/O
pytest tests/integration -v     # full loop with injected resource pressure
pytest --cov=adaptive_fetch --cov-report=term-missing
```

The governor is a pure function of a snapshot, so its behaviour is tested directly by constructing snapshots — no mocking of psutil and no need to actually exhaust memory. Integration tests inject a scripted sequence of snapshots into the fetch loop, which is how "batches shrink when memory spikes mid-run" and "the run halts cleanly when disk runs out" are verified without touching a real disk or a real service.

---

## Configuration

Every setting is an environment variable with the `AF_` prefix, or a `.env` file.

```bash
AF_BASE_URL=https://api.example.com
AF_RECORD_PATH=/records/{id}

AF_MIN_BATCH_SIZE=8
AF_MAX_BATCH_SIZE=512
AF_MAX_CONCURRENT=8

AF_MEMORY_LOW_PCT=60
AF_MEMORY_HIGH_PCT=80
AF_MEMORY_CRITICAL_PCT=90
AF_GROWTH_PATIENCE=3

AF_DISK_WARN_MB=500
AF_DISK_HALT_MB=100

AF_LATENCY_FAST_MS=200
AF_LATENCY_SLOW_MS=800

AF_CHECKPOINT_EVERY=25
AF_SINK=local
AF_OUTPUT_FORMAT=json
```

CLI flags override environment variables for the options that expose both.

---

## Tradeoffs and Decisions I Made Along the Way

**The governor is a pure function, and that was the best decision here.** `evaluate(snapshot) -> decision` takes a frozen dataclass and returns a frozen dataclass. It touches no psutil, no clock, no filesystem. That meant I could write a test for "memory goes critical at 92%" by constructing a snapshot, instead of trying to actually fill memory on a CI runner. The sampling functions are separately testable, and the loop just wires the two together. Almost every hard-to-test problem in this kind of tool comes from mixing measurement with decision-making, and keeping them apart cost nothing.

**Shrink fast, grow slow — and I got this wrong the first time.** My first version resized symmetrically: halve on pressure, double when healthy. It oscillated badly. Memory would spike, the batch would halve, the next check looked fine because the batch had just shrunk, so it doubled straight back and spiked again. Classic control loop with no damping. Adding `growth_patience` (three consecutive healthy checks before growing at all) plus growing by 25% rather than 100% stopped it dead. The `_ceiling` that gets lowered after a critical event is the same idea taken further: if we hit 90% at batch size 128, never try 128 again this run.

**Disk gets a hard halt, memory does not.** These felt like they should be symmetric and they are not. Memory pressure is recoverable — shrink the batch and keep going. Disk exhaustion is not, and worse, it breaks the thing that would have saved you: if the disk fills while writing a checkpoint, you lose the record of what you had already done. So the disk check runs first, before any resizing, and it halts while there is still room to write the checkpoint. Halting deliberately with resumable state is a much better outcome than crashing on a write.

**Latency via TCP connect time is crude and I kept it anyway.** Timing a socket connection tells you about reachability and round-trip, not throughput or server load. A fast connect to an overloaded server looks healthy. The honest version would measure observed request durations from the fetch loop itself and feed those back. I stopped at the socket probe because it works before the first request has been made — the governor needs a reading for batch one, and there is no request history yet. Bootstrapping is the actual justification; anything richer would need a warmup period.

**Recycling thresholds scale with workload, which came from the original tool.** The internal version this is based on tracked browser memory and restarted the browser on a sliding scale — roughly 2000MB for tiny jobs, down to 800MB for jobs over 300 items. I kept that shape for the HTTP client. It looks arbitrary and it half is, but the reasoning holds: recycling costs a connection re-establishment, and on a 5-item job you will never recycle enough times for accumulated state to matter, while on a 5,000-item job you want it to happen often and early.

**I built this over HTTP rather than a browser, which is a deliberate departure.** The tool this is modelled on drove a browser, and browser memory growth is the textbook motivation for recycling. But Twinlane in this portfolio already covers Playwright automation, and repeating it would teach nothing new. Over HTTP the governor is the whole point, it is far easier to test, and the pattern transfers to any batch workload. The cost is that memory growth is less dramatic than it would be with a browser, so the adaptive behaviour is less visceral in a demo — which is exactly why `probe` exists.

**Records are held in memory until the end.** The checkpoint accumulates every result and the sink writes once. For a tool whose entire premise is memory awareness, that is an uncomfortable irony, and at a few hundred thousand records it becomes the binding constraint. Streaming results to disk as they complete would fix it. I did not do it because the current shape lets a sink write one well-formed document with a summary block, and streaming would mean either an append-only format or a rewrite of the sink interface. Worth doing before this ever runs at that scale.

**One bad record never stops the batch, and the cost is real.** Every fetch is wrapped, retried, and on final failure recorded as an ERROR row. A batch of 50,000 will not die on record 37. The price is that a misconfiguration is indistinguishable from bad data: point `--base-url` at the wrong host and you get 50,000 ERROR rows that look exactly like 50,000 missing records. `probe` and `plan` exist partly to catch that before you spend the run.

---

## Stack

| Concern | Choice |
|:--|:--|
| Language | Python 3.11+ |
| HTTP | httpx (async) |
| Resource sampling | psutil, stdlib socket |
| CLI | Click |
| Config | Pydantic Settings |
| Output | stdlib json/csv, openpyxl, boto3 |
| Tests | pytest, pytest-asyncio |
| Lint | ruff |
