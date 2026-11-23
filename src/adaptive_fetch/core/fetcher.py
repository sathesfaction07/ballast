import asyncio
import csv
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..config import Settings
from .checkpoint import Checkpoint
from .governor import GovernorDecision, GovernorLimits, PayloadMode, ResourceGovernor
from .resources import ResourceSnapshot, initial_batch_size, read_memory, snapshot

logger = logging.getLogger("adaptive_fetch")

FetchOne = Callable[[str, PayloadMode], Awaitable[dict[str, Any]]]


@dataclass
class RunReport:
    total: int = 0
    fetched: int = 0
    errors: int = 0
    skipped: int = 0
    batches: int = 0
    recycles: int = 0
    halted_reason: str | None = None
    duration_seconds: float = 0.0
    batch_sizes: list[int] = field(default_factory=list)
    decisions: list[dict[str, Any]] = field(default_factory=list)

    def as_summary(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "fetched": self.fetched,
            "errors": self.errors,
            "skipped": self.skipped,
            "batches": self.batches,
            "recycles": self.recycles,
            "halted_reason": self.halted_reason,
            "duration_seconds": round(self.duration_seconds, 2),
            "batch_sizes": self.batch_sizes,
            "decisions": self.decisions,
        }


def load_ids(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"input not found: {path}")

    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                return []
            key = next(
                (f for f in reader.fieldnames if f.strip().lower() in {"id", "record_id", "sku"}),
                reader.fieldnames[0],
            )
            return [row[key].strip() for row in reader if row.get(key, "").strip()]

    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def limits_from_settings(cfg: Settings) -> GovernorLimits:
    return GovernorLimits(
        min_batch_size=cfg.min_batch_size,
        max_batch_size=cfg.max_batch_size,
        max_concurrent=cfg.max_concurrent,
        memory_low_pct=cfg.memory_low_pct,
        memory_high_pct=cfg.memory_high_pct,
        memory_critical_pct=cfg.memory_critical_pct,
        growth_patience=cfg.growth_patience,
        disk_warn_mb=cfg.disk_warn_mb,
        disk_halt_mb=cfg.disk_halt_mb,
        latency_fast_ms=cfg.latency_fast_ms,
        latency_slow_ms=cfg.latency_slow_ms,
    )


class Fetcher:
    def __init__(
        self,
        cfg: Settings,
        fetch_one: FetchOne | None = None,
        sampler: Callable[[], ResourceSnapshot] | None = None,
    ) -> None:
        self.cfg = cfg
        self._fetch_one = fetch_one or self._http_fetch
        self._sampler = sampler or (
            lambda: snapshot(cfg.checkpoint_dir, cfg.probe_url or cfg.base_url)
        )
        self._client: Any = None

    async def _get_client(self) -> Any:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                base_url=self.cfg.base_url,
                timeout=self.cfg.timeout_seconds,
            )
        return self._client

    async def _recycle(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _http_fetch(self, item_id: str, mode: PayloadMode) -> dict[str, Any]:
        client = await self._get_client()
        path = self.cfg.record_path.format(id=item_id)
        params = {"fields": "id,status"} if mode is PayloadMode.MINIMAL else None
        response = await client.get(path, params=params)
        response.raise_for_status()
        return {"body": response.json(), "bytes": len(response.content)}

    async def _fetch_with_retry(self, item_id: str, mode: PayloadMode) -> dict[str, Any]:
        attempts = 0
        last_error = ""
        while attempts <= self.cfg.retries:
            attempts += 1
            try:
                payload = await self._fetch_one(item_id, mode)
                return {
                    "id": item_id,
                    "status": "ok",
                    "attempts": attempts,
                    "payload_mode": mode.value,
                    "bytes": payload.get("bytes", 0),
                    "body": payload.get("body"),
                }
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempts <= self.cfg.retries:
                    await asyncio.sleep(2 ** (attempts - 1))

        return {
            "id": item_id,
            "status": "error",
            "attempts": attempts,
            "payload_mode": mode.value,
            "bytes": 0,
            "error": last_error,
        }

    async def run(self) -> RunReport:
        started = time.perf_counter()
        report = RunReport()

        all_ids = load_ids(self.cfg.input_path)
        report.total = len(all_ids)

        checkpoint = Checkpoint(self.cfg.checkpoint_dir, self.cfg.checkpoint_every)
        pending = checkpoint.remaining(all_ids) if self.cfg.resume else all_ids
        report.skipped = report.total - len(pending)

        _, available_mb = read_memory()
        start_batch = self.cfg.batch_size or initial_batch_size(
            available_mb, self.cfg.min_batch_size, self.cfg.max_batch_size
        )
        governor = ResourceGovernor(
            limits=limits_from_settings(self.cfg),
            starting_batch_size=start_batch,
            total_items=report.total,
        )

        logger.info(
            "starting: %d pending of %d, batch=%d, recycle_at=%.0fMB",
            len(pending),
            report.total,
            governor.batch_size,
            governor.recycle_at_mb,
        )

        cursor = 0
        while cursor < len(pending):
            decision = governor.evaluate(self._sampler())
            self._log_decision(decision)
            report.decisions.append(
                {
                    "batch": report.batches + 1,
                    "pressure": decision.pressure.value,
                    "batch_size": decision.batch_size,
                    "payload_mode": decision.payload_mode.value,
                    "notes": list(decision.notes),
                }
            )

            if decision.should_halt:
                report.halted_reason = decision.halt_reason
                checkpoint.halt(decision.halt_reason or "unknown")
                logger.error("halting: %s", decision.halt_reason)
                break

            if decision.recycle_client:
                await self._recycle()
                report.recycles += 1

            chunk = pending[cursor : cursor + decision.batch_size]
            cursor += len(chunk)
            report.batches += 1
            report.batch_sizes.append(len(chunk))

            semaphore = asyncio.Semaphore(decision.concurrency)

            async def bounded(item_id: str, mode: PayloadMode = decision.payload_mode):
                async with semaphore:
                    return await self._fetch_with_retry(item_id, mode)

            results = await asyncio.gather(*(bounded(i) for i in chunk))

            for record in results:
                if record["status"] == "ok":
                    report.fetched += 1
                else:
                    report.errors += 1
                checkpoint.mark(record["id"], record)

        checkpoint.save()
        await self._recycle()
        report.duration_seconds = time.perf_counter() - started
        return report

    @staticmethod
    def _log_decision(decision: GovernorDecision) -> None:
        for note in decision.notes:
            logger.warning("governor: %s", note)
