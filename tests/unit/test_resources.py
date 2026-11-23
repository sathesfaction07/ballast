from pathlib import Path

import pytest

from adaptive_fetch.core.resources import (
    initial_batch_size,
    probe_latency,
    read_disk_free,
    read_memory,
    read_process_rss,
    recycle_threshold_mb,
    snapshot,
)


class TestRecycleThreshold:
    @pytest.mark.parametrize(
        "total_items,expected",
        [
            (1, 2000.0),
            (10, 2000.0),
            (11, 1600.0),
            (50, 1600.0),
            (51, 1200.0),
            (150, 1200.0),
            (151, 1000.0),
            (300, 1000.0),
            (301, 800.0),
            (10000, 800.0),
        ],
    )
    def test_scales_down_as_workload_grows(self, total_items, expected):
        assert recycle_threshold_mb(total_items) == expected

    def test_monotonically_non_increasing(self):
        sizes = [1, 10, 11, 50, 51, 150, 151, 300, 301, 5000]
        thresholds = [recycle_threshold_mb(s) for s in sizes]
        assert thresholds == sorted(thresholds, reverse=True)


class TestInitialBatchSize:
    @pytest.mark.parametrize(
        "available_mb,expected",
        [
            (32 * 1024, 512),
            (12 * 1024, 256),
            (6 * 1024, 128),
            (3 * 1024, 64),
            (1024, 32),
        ],
    )
    def test_scales_with_available_memory(self, available_mb, expected):
        assert initial_batch_size(available_mb, minimum=8, maximum=512) == expected

    def test_respects_maximum(self):
        assert initial_batch_size(64 * 1024, minimum=8, maximum=64) == 64

    def test_respects_minimum(self):
        assert initial_batch_size(128, minimum=100, maximum=512) == 100


class TestReaders:
    def test_memory_returns_percent_and_available(self):
        percent, available_mb = read_memory()
        assert 0.0 <= percent <= 100.0
        assert available_mb > 0

    def test_process_rss_is_positive(self):
        assert read_process_rss() > 0

    def test_disk_free_walks_up_to_existing_parent(self, tmp_path: Path):
        missing = tmp_path / "does" / "not" / "exist"
        assert read_disk_free(missing) > 0


class TestLatencyProbe:
    def test_empty_url_returns_none(self):
        assert probe_latency("") is None

    def test_unresolvable_host_returns_none(self):
        assert probe_latency("http://host.invalid:9", timeout=0.5) is None

    def test_malformed_url_returns_none(self):
        assert probe_latency("::::", timeout=0.5) is None


class TestSnapshot:
    def test_populates_every_field(self, tmp_path: Path):
        snap = snapshot(tmp_path)
        assert snap.memory_percent >= 0
        assert snap.memory_available_mb > 0
        assert snap.process_rss_mb > 0
        assert snap.disk_free_mb > 0
        assert snap.latency_ms is None
        assert not snap.latency_known
