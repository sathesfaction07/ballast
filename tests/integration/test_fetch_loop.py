from pathlib import Path

import pytest

from adaptive_fetch.core.checkpoint import Checkpoint
from adaptive_fetch.core.fetcher import Fetcher, load_ids
from adaptive_fetch.core.governor import PayloadMode


def sampler_sequence(*snapshots):
    queue = list(snapshots)
    last = queue[-1]

    def _next():
        return queue.pop(0) if queue else last

    return _next


class TestInputLoading:
    def test_reads_csv_id_column(self, ids_file: Path):
        assert len(load_ids(ids_file)) == 40

    def test_reads_plain_text_lines(self, tmp_path: Path):
        target = tmp_path / "ids.txt"
        target.write_text("A\nB\n\nC\n")
        assert load_ids(target) == ["A", "B", "C"]

    def test_finds_alternate_id_column_name(self, tmp_path: Path):
        target = tmp_path / "in.csv"
        target.write_text("sku,extra\nS-1,x\nS-2,y\n")
        assert load_ids(target) == ["S-1", "S-2"]

    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_ids(tmp_path / "nope.csv")


class TestHappyPath:
    async def test_fetches_everything(self, cfg, always_ok, snapshot_factory):
        report = await Fetcher(cfg, always_ok, lambda: snapshot_factory()).run()
        assert report.total == 40
        assert report.fetched == 40
        assert report.errors == 0
        assert report.halted_reason is None

    async def test_batch_size_holds_under_normal_pressure(self, cfg, always_ok, snapshot_factory):
        report = await Fetcher(
            cfg, always_ok, lambda: snapshot_factory(memory_percent=70.0)
        ).run()
        assert report.batches == 4
        assert report.batch_sizes == [10, 10, 10, 10]

    async def test_grows_after_patience_when_memory_is_low(self, cfg, always_ok, snapshot_factory):
        report = await Fetcher(
            cfg, always_ok, lambda: snapshot_factory(memory_percent=40.0)
        ).run()
        assert sum(report.batch_sizes) == 40
        assert report.batch_sizes[0] == 10
        assert max(report.batch_sizes) > 10

    async def test_records_land_in_checkpoint(self, cfg, always_ok, snapshot_factory):
        await Fetcher(cfg, always_ok, lambda: snapshot_factory()).run()
        assert len(Checkpoint(cfg.checkpoint_dir).completed) == 40


class TestFailureIsolation:
    async def test_one_bad_record_does_not_stop_the_run(self, cfg, snapshot_factory):
        async def flaky(item_id: str, mode: PayloadMode) -> dict:
            if item_id == "REC-0007":
                raise RuntimeError("upstream exploded")
            return {"body": {"id": item_id}, "bytes": 64}

        report = await Fetcher(cfg, flaky, lambda: snapshot_factory()).run()
        assert report.fetched == 39
        assert report.errors == 1
        assert report.total == 40

    async def test_error_record_captures_exception_text(self, cfg, snapshot_factory):
        async def always_fails(item_id: str, mode: PayloadMode) -> dict:
            raise ValueError("nope")

        await Fetcher(cfg, always_fails, lambda: snapshot_factory()).run()
        records = Checkpoint(cfg.checkpoint_dir).records
        assert all(r["status"] == "error" for r in records)
        assert "ValueError: nope" in records[0]["error"]


class TestGovernorDrivesTheLoop:
    async def test_memory_pressure_shrinks_batches_mid_run(
        self, cfg, always_ok, snapshot_factory
    ):
        cfg.batch_size = 20
        sampler = sampler_sequence(
            snapshot_factory(memory_percent=50.0),
            snapshot_factory(memory_percent=85.0),
            snapshot_factory(memory_percent=95.0),
        )
        report = await Fetcher(cfg, always_ok, sampler).run()

        assert report.batch_sizes[0] == 20
        assert report.batch_sizes[1] < report.batch_sizes[0]
        assert report.fetched == 40

    async def test_slow_network_switches_payload_mode(self, cfg, snapshot_factory):
        seen: list[str] = []

        async def record_mode(item_id: str, mode: PayloadMode) -> dict:
            seen.append(mode.value)
            return {"body": {"id": item_id}, "bytes": 32}

        sampler = sampler_sequence(
            snapshot_factory(latency_ms=50.0),
            snapshot_factory(latency_ms=2000.0),
        )
        await Fetcher(cfg, record_mode, sampler).run()

        assert "full" in seen
        assert "minimal" in seen

    async def test_disk_exhaustion_halts_and_saves(self, cfg, always_ok, snapshot_factory):
        sampler = sampler_sequence(
            snapshot_factory(),
            snapshot_factory(disk_free_mb=10.0),
        )
        report = await Fetcher(cfg, always_ok, sampler).run()

        assert report.halted_reason is not None
        assert "disk free" in report.halted_reason
        assert report.fetched == 10
        assert Checkpoint(cfg.checkpoint_dir).halted_reason is not None

    async def test_recycle_is_counted(self, cfg, always_ok, snapshot_factory):
        report = await Fetcher(
            cfg, always_ok, lambda: snapshot_factory(process_rss_mb=5000.0)
        ).run()
        assert report.recycles == report.batches

    async def test_decisions_are_recorded_per_batch(self, cfg, always_ok, snapshot_factory):
        report = await Fetcher(cfg, always_ok, lambda: snapshot_factory()).run()
        assert len(report.decisions) == report.batches
        assert report.decisions[0]["pressure"] == "low"


class TestResumeAcrossRuns:
    async def test_second_run_skips_completed(self, cfg, always_ok, snapshot_factory):
        halting = sampler_sequence(
            snapshot_factory(),
            snapshot_factory(disk_free_mb=10.0),
        )
        first = await Fetcher(cfg, always_ok, halting).run()
        assert first.fetched == 10

        second = await Fetcher(cfg, always_ok, lambda: snapshot_factory()).run()
        assert second.skipped == 10
        assert second.fetched == 30
        assert len(Checkpoint(cfg.checkpoint_dir).completed) == 40

    async def test_no_resume_reprocesses_everything(self, cfg, always_ok, snapshot_factory):
        await Fetcher(cfg, always_ok, lambda: snapshot_factory()).run()
        cfg.resume = False
        report = await Fetcher(cfg, always_ok, lambda: snapshot_factory()).run()
        assert report.skipped == 0
        assert report.fetched == 40
