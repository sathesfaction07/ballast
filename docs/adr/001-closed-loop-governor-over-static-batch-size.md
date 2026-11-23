# ADR 001: Closed-loop governor instead of a static batch size

## Status

Accepted

## Context

The job is to pull tens of thousands of records over HTTP. The conventional shape is a loop with a batch size chosen up front, either hardcoded or passed as a flag.

That number cannot be chosen correctly, because the thing it needs to match keeps moving. The right batch size depends on free memory at this instant, the size of the responses actually coming back, whatever else is running on the box, and current network conditions. All of those change during a run that lasts an hour.

The consequences of guessing are not symmetric:

- **Too large** — the process is OOM-killed. Everything not yet checkpointed is gone, and the run has to be restarted.
- **Too small** — the job takes longer than it needed to.

One of those is a lost afternoon and the other is a slow afternoon.

`ImageCluster` in this portfolio already does the simple version: read available memory once at startup, pick a batch size from a ladder, and additionally halve it if usage later crosses 85%. That is genuinely useful and it covers the common case. What it does not do is recover — the batch size only ever goes down, so a transient spike permanently penalises the rest of the run. It also has no notion of disk or network, and no separation between "the machine is under pressure" and "this process has grown too fat".

## Decision

Sample resources before every batch and pass the reading to a governor that returns a decision. The governor is a pure function of a snapshot:

```
evaluate(ResourceSnapshot) -> GovernorDecision
```

`ResourceSnapshot` is a frozen dataclass holding system memory percent, available MB, this process's RSS, free disk on the output volume, and an optional latency reading. `GovernorDecision` is a frozen dataclass holding batch size, concurrency, payload mode, a recycle flag, an optional halt reason, and human-readable notes.

The resizing rule is deliberately asymmetric — **shrink fast, grow slow**:

| Pressure | Trigger | Response |
|:--|:--|:--|
| Critical | memory >= 90% | Drop to the floor batch size, and set a ceiling at half the size that caused it |
| High | memory >= 80% | Halve the batch immediately |
| Normal | 60% < memory < 80% | Hold, and reset the healthy streak |
| Low | memory <= 60% | Grow by 25%, but only after `growth_patience` consecutive low readings |

Three other concerns are folded into the same call:

- **Disk** is checked first and can halt the run outright, before any resizing happens.
- **Process RSS** is tracked separately from system memory and triggers an HTTP client recycle. The threshold scales with workload size, from 2000MB for jobs of 10 items or fewer down to 800MB above 300 items.
- **Latency**, when a reading is available, switches payload mode between full and minimal and halves concurrency when slow.

The fetch loop does no reasoning of its own. It samples, asks, and obeys.

## Why the asymmetry matters

The first implementation resized symmetrically: halve under pressure, double when healthy. It oscillated. Memory would spike, the batch would halve, the next reading looked fine precisely because the batch had just shrunk, so it doubled straight back into the same wall. A control loop with no damping.

Two changes fixed it. `growth_patience` requires three consecutive healthy readings before growing at all, which stops the loop reacting to its own last move. Growing by 25% rather than 100% means an incorrect growth decision costs a quarter of a step instead of doubling into a cliff.

The ceiling is the same idea taken one step further. If 90% memory was reached at batch size 128, then 128 is known to be unsafe for this machine under this workload, and there is no reason to try it again in this run. The ceiling is only lowered, never raised.

## Consequences

**Good**

- The governor is a pure function, so its behaviour is unit-testable by constructing snapshots. "What happens at 92% memory" is a test that runs in microseconds and never has to actually exhaust memory
- Sampling and deciding are separate, so each can be tested and changed independently
- Every decision carries `notes`, which is what makes a run explainable after the fact. The `--log-level` output and the `probe` command both come free from this
- Batch size recovers after transient pressure instead of ratcheting permanently downward
- Adding a new signal means adding a field to the snapshot and a rule to `evaluate`, with no change to the loop

**Bad**

- More moving parts than a constant. There are eleven tunable limits in `GovernorLimits`, and someone has to understand them to tune them
- The governor is stateful across calls (`_healthy_streak`, `_ceiling`, `_payload_mode`), so it is a pure function of snapshot *plus its own history*, not of snapshot alone. That is what makes hysteresis possible, and it does mean call order matters in tests
- Sampling costs a `psutil` call and possibly a TCP connect per batch. Negligible next to a batch of HTTP requests, but not free, and it would matter if batches were tiny

**Neutral**

- The thresholds (60/80/90, patience of 3, growth of 25%) are judgement calls, not values fitted to measurements. They are all settings, and the defaults are a starting point rather than a tuned optimum
- The governor decides per batch, not per request. A batch that starts fine and degrades halfway through will not be re-evaluated until it completes. Finer granularity would mean sampling inside the concurrency pool, which conflicts with batching as a concept
