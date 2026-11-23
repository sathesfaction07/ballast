from pathlib import Path

import pytest

from adaptive_fetch.config import Settings
from adaptive_fetch.core.governor import GovernorLimits, PayloadMode
from adaptive_fetch.core.resources import ResourceSnapshot


@pytest.fixture
def limits() -> GovernorLimits:
    return GovernorLimits(
        min_batch_size=8,
        max_batch_size=256,
        max_concurrent=8,
        memory_low_pct=60.0,
        memory_high_pct=80.0,
        memory_critical_pct=90.0,
        growth_patience=3,
        disk_warn_mb=500.0,
        disk_halt_mb=100.0,
        latency_fast_ms=200.0,
        latency_slow_ms=800.0,
    )


@pytest.fixture
def healthy() -> ResourceSnapshot:
    return ResourceSnapshot(
        memory_percent=50.0,
        memory_available_mb=8192.0,
        process_rss_mb=120.0,
        disk_free_mb=20000.0,
        latency_ms=80.0,
    )


def make_snapshot(**overrides) -> ResourceSnapshot:
    base = {
        "memory_percent": 50.0,
        "memory_available_mb": 8192.0,
        "process_rss_mb": 120.0,
        "disk_free_mb": 20000.0,
        "latency_ms": 80.0,
    }
    base.update(overrides)
    return ResourceSnapshot(**base)


@pytest.fixture
def snapshot_factory():
    return make_snapshot


@pytest.fixture
def ids_file(tmp_path: Path) -> Path:
    target = tmp_path / "input.csv"
    target.write_text("id\n" + "\n".join(f"REC-{i:04d}" for i in range(1, 41)) + "\n")
    return target


@pytest.fixture
def cfg(tmp_path: Path, ids_file: Path) -> Settings:
    return Settings(
        input_path=ids_file,
        output_path=tmp_path / "out",
        checkpoint_dir=tmp_path / ".checkpoints",
        checkpoint_every=5,
        batch_size=10,
        max_concurrent=4,
        retries=0,
        resume=True,
        sink="local",
        output_format="json",
    )


@pytest.fixture
def always_ok():
    async def _fetch(item_id: str, mode: PayloadMode) -> dict:
        return {"body": {"id": item_id, "mode": mode.value}, "bytes": 128}

    return _fetch
