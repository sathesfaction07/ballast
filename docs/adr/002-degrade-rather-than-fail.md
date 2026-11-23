# ADR 002: Degrade rather than fail, and halt rather than crash

## Status

Accepted

## Context

Three resources can run out during a long batch run, and each has a natural crash mode:

- **Memory** — the OOM killer takes the process. No traceback, no chance to save state.
- **Disk** — a write raises `OSError`. Usually the write that fails is the checkpoint, which is the one thing that would have made the run resumable.
- **Network** — requests start timing out. Retries stack on top of a service that is already struggling, which makes things worse rather than better.

The instinct is to treat all three the same way: catch the error, log it, exit non-zero. That is uniform and it is wrong, because the three situations are not equivalent. Memory pressure is recoverable by doing less work at a time. Network degradation is recoverable by asking for less data. Disk exhaustion is not recoverable at all, and it actively destroys the recovery mechanism.

There is also a question of what "failure" should mean for a single record. A batch of 50,000 will contain records that are missing upstream, records that time out, and records that return malformed JSON. If any one of those aborts the run, the tool is unusable at scale.

## Decision

Give every resource limit a graceful path, and choose the path to match what is actually recoverable.

**Memory — degrade the work.** Shrink the batch (halve on high, floor on critical) and keep going. The run gets slower and does not stop. Recovery is possible because the pressure is a function of how much we attempt at once.

**Network — degrade the request.** When latency crosses the slow threshold, switch to `PayloadMode.MINIMAL`, which asks the service for a reduced field set, and halve concurrency. Less data over a worse link, rather than full payloads that time out. When latency recovers below the fast threshold, switch back.

**Disk — halt deliberately.** The disk check runs first in `evaluate()`, before any resizing, and returns a `halt_reason`. The loop stops, writes the checkpoint, and reports why. The threshold is deliberately well above zero (default 100MB) so there is room to write that checkpoint. This is a clean stop with resumable state, not a crash.

**Individual records — absorb.** Each fetch is wrapped with retries and exponential backoff. On final failure the record becomes a row with `status: "error"` and the exception text, and the batch continues. `asyncio.gather` over the batch never sees an exception propagate.

The distinction the design turns on: **a resource that can be traded against throughput gets degraded; a resource that cannot gets a controlled stop.**

## Verified behaviour

Both degradation paths were exercised against a local HTTP server:

- Forcing the slow-latency branch dropped the response from 628 bytes to 38 bytes per record, a 94% reduction, with all records still fetched successfully.
- Setting the disk halt threshold above actual free space stopped the run before the first batch, wrote `halted_reason` into the checkpoint, and still produced output files.
- Pointing the tool at a dead server produced 60 error rows with `ConnectError`, three attempts each, no crash, and results written.

## Consequences

**Good**

- A run that hits a wall produces resumable state and an explanation instead of a stack trace
- Slow links get slower but still complete, rather than failing on timeout
- One bad record cannot cost the batch. At 50,000 records this is the difference between a usable tool and an unusable one
- The halt reason is persisted in the checkpoint, so the next operator can see why the previous run stopped

**Bad**

- **A misconfiguration is indistinguishable from bad data.** Point `--base-url` at the wrong host and you get N error rows that look exactly like N missing records. This is the direct cost of absorbing record-level failures, and it is why `probe` and `plan` exist as pre-flight commands
- Degradation is silent unless someone reads the logs or the summary. A run that quietly spent an hour in minimal-payload mode at batch size 8 looks like a successful run in the exit code
- The minimal payload mode assumes the service honours a `fields` parameter. Against a service that ignores it, the degradation achieves nothing while still halving concurrency, so the run is slower for no benefit

**Neutral**

- Retry backoff is `2 ** (attempt - 1)` seconds and the retry count is a setting, but retries are attempted for every exception type. A 404 is retried exactly as hard as a timeout, which is wasteful. Distinguishing retryable from terminal status codes would be a real improvement and is not implemented
- The disk threshold protects the checkpoint but not the final output write. A run that completes and then cannot write a large result file will still fail at the end, with the checkpoint intact
