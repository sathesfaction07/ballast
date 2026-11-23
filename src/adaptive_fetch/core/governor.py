from dataclasses import dataclass
from enum import Enum

from .resources import ResourceSnapshot, recycle_threshold_mb


class PayloadMode(str, Enum):
    FULL = "full"
    MINIMAL = "minimal"


class Pressure(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


@dataclass(frozen=True)
class GovernorDecision:
    batch_size: int
    concurrency: int
    payload_mode: PayloadMode
    recycle_client: bool
    halt_reason: str | None
    pressure: Pressure
    notes: tuple[str, ...] = ()

    @property
    def should_halt(self) -> bool:
        return self.halt_reason is not None


@dataclass
class GovernorLimits:
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


class ResourceGovernor:
    def __init__(
        self,
        limits: GovernorLimits,
        starting_batch_size: int,
        total_items: int,
    ) -> None:
        self.limits = limits
        self.total_items = total_items
        self.recycle_at_mb = recycle_threshold_mb(total_items)
        self._ceiling: int | None = None
        self._batch_size = self._clamp(starting_batch_size)
        self._concurrency = limits.max_concurrent
        self._payload_mode = PayloadMode.FULL
        self._healthy_streak = 0

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def payload_mode(self) -> PayloadMode:
        return self._payload_mode

    def _clamp(self, value: int) -> int:
        upper = self.limits.max_batch_size
        if self._ceiling is not None:
            upper = min(upper, self._ceiling)
        return max(self.limits.min_batch_size, min(value, upper))

    def _classify(self, snap: ResourceSnapshot) -> Pressure:
        if snap.memory_percent >= self.limits.memory_critical_pct:
            return Pressure.CRITICAL
        if snap.memory_percent >= self.limits.memory_high_pct:
            return Pressure.HIGH
        if snap.memory_percent <= self.limits.memory_low_pct:
            return Pressure.LOW
        return Pressure.NORMAL

    def evaluate(self, snap: ResourceSnapshot) -> GovernorDecision:
        notes: list[str] = []

        if snap.disk_free_mb < self.limits.disk_halt_mb:
            return GovernorDecision(
                batch_size=self._batch_size,
                concurrency=self._concurrency,
                payload_mode=self._payload_mode,
                recycle_client=False,
                halt_reason=(
                    f"disk free {snap.disk_free_mb:.0f}MB below halt threshold "
                    f"{self.limits.disk_halt_mb:.0f}MB"
                ),
                pressure=self._classify(snap),
                notes=("halting before results become unwritable",),
            )

        if snap.disk_free_mb < self.limits.disk_warn_mb:
            notes.append(f"disk free {snap.disk_free_mb:.0f}MB is low")

        pressure = self._classify(snap)
        previous = self._batch_size

        if pressure is Pressure.CRITICAL:
            self._batch_size = self.limits.min_batch_size
            self._ceiling = max(self.limits.min_batch_size, previous // 2)
            self._healthy_streak = 0
            notes.append(f"memory {snap.memory_percent:.0f}% critical, dropped to floor")
        elif pressure is Pressure.HIGH:
            self._batch_size = self._clamp(previous // 2)
            self._healthy_streak = 0
            notes.append(f"memory {snap.memory_percent:.0f}% high, halved batch")
        elif pressure is Pressure.LOW:
            self._healthy_streak += 1
            if self._healthy_streak >= self.limits.growth_patience:
                self._batch_size = self._clamp(previous + max(1, previous // 4))
                self._healthy_streak = 0
                if self._batch_size > previous:
                    notes.append(f"memory stable, grew batch to {self._batch_size}")
        else:
            self._healthy_streak = 0

        recycle = snap.process_rss_mb >= self.recycle_at_mb
        if recycle:
            notes.append(
                f"process rss {snap.process_rss_mb:.0f}MB reached recycle threshold "
                f"{self.recycle_at_mb:.0f}MB"
            )

        if snap.latency_known:
            if snap.latency_ms > self.limits.latency_slow_ms:
                self._payload_mode = PayloadMode.MINIMAL
                self._concurrency = max(1, self.limits.max_concurrent // 2)
                notes.append(f"latency {snap.latency_ms:.0f}ms slow, minimal payload")
            elif snap.latency_ms < self.limits.latency_fast_ms:
                if self._payload_mode is not PayloadMode.FULL:
                    notes.append(f"latency {snap.latency_ms:.0f}ms recovered, full payload")
                self._payload_mode = PayloadMode.FULL
                self._concurrency = self.limits.max_concurrent

        return GovernorDecision(
            batch_size=self._batch_size,
            concurrency=self._concurrency,
            payload_mode=self._payload_mode,
            recycle_client=recycle,
            halt_reason=None,
            pressure=pressure,
            notes=tuple(notes),
        )
