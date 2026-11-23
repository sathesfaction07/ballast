import json
from pathlib import Path

from adaptive_fetch.core.checkpoint import Checkpoint


class TestPersistence:
    def test_creates_directory(self, tmp_path: Path):
        target = tmp_path / "nested" / ".checkpoints"
        Checkpoint(target)
        assert target.exists()

    def test_empty_when_no_prior_state(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path)
        assert checkpoint.completed == set()
        assert checkpoint.records == []
        assert checkpoint.halted_reason is None

    def test_round_trips_state(self, tmp_path: Path):
        first = Checkpoint(tmp_path, save_every=1)
        first.mark("A", {"id": "A", "status": "ok"})
        first.mark("B", {"id": "B", "status": "error"})

        second = Checkpoint(tmp_path)
        assert second.completed == {"A", "B"}
        assert len(second.records) == 2

    def test_survives_corrupt_file(self, tmp_path: Path):
        (tmp_path / "progress.json").write_text("{not valid json")
        checkpoint = Checkpoint(tmp_path)
        assert checkpoint.completed == set()

    def test_writes_atomically_via_temp_file(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path, save_every=1)
        checkpoint.mark("A", {"id": "A"})
        assert checkpoint.path.exists()
        assert not checkpoint.path.with_suffix(".tmp").exists()


class TestBatchedSaves:
    def test_does_not_write_before_threshold(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path, save_every=5)
        for i in range(4):
            checkpoint.mark(f"ID-{i}", {"id": f"ID-{i}"})
        assert not checkpoint.path.exists()

    def test_writes_on_threshold(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path, save_every=5)
        for i in range(5):
            checkpoint.mark(f"ID-{i}", {"id": f"ID-{i}"})
        assert checkpoint.path.exists()
        assert len(json.loads(checkpoint.path.read_text())["completed"]) == 5

    def test_explicit_save_flushes_partial_batch(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path, save_every=100)
        checkpoint.mark("A", {"id": "A"})
        checkpoint.save()
        assert Checkpoint(tmp_path).completed == {"A"}


class TestResume:
    def test_remaining_excludes_completed(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path, save_every=1)
        checkpoint.mark("A", {"id": "A"})
        assert checkpoint.remaining(["A", "B", "C"]) == ["B", "C"]

    def test_remaining_preserves_input_order(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path, save_every=1)
        checkpoint.mark("B", {"id": "B"})
        assert checkpoint.remaining(["C", "A", "B", "D"]) == ["C", "A", "D"]

    def test_new_ids_added_between_runs_are_picked_up(self, tmp_path: Path):
        first = Checkpoint(tmp_path, save_every=1)
        first.mark("A", {"id": "A"})
        second = Checkpoint(tmp_path)
        assert second.remaining(["A", "B", "NEW"]) == ["B", "NEW"]


class TestHalt:
    def test_halt_records_reason_and_saves(self, tmp_path: Path):
        checkpoint = Checkpoint(tmp_path, save_every=100)
        checkpoint.mark("A", {"id": "A"})
        checkpoint.halt("disk free 40MB below halt threshold 100MB")

        reloaded = Checkpoint(tmp_path)
        assert reloaded.halted_reason is not None
        assert "disk free" in reloaded.halted_reason
        assert reloaded.completed == {"A"}
