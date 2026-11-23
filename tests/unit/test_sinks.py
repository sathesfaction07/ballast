import csv
import json
from pathlib import Path

import pytest

from adaptive_fetch.config import Settings
from adaptive_fetch.sinks import get_sink
from adaptive_fetch.sinks.local import LocalSink
from adaptive_fetch.sinks.s3 import S3Sink

RECORDS = [
    {"id": "A", "status": "ok", "attempts": 1, "payload_mode": "full", "bytes": 128},
    {"id": "B", "status": "error", "attempts": 3, "payload_mode": "minimal", "error": "Timeout"},
]
SUMMARY = {"total": 2, "fetched": 1, "errors": 1}


class TestFactory:
    def test_returns_local(self, tmp_path: Path):
        cfg = Settings(output_path=tmp_path, sink="local")
        assert isinstance(get_sink("local", cfg), LocalSink)

    def test_returns_s3(self):
        cfg = Settings(sink="s3", s3_bucket="example-bucket")
        assert isinstance(get_sink("s3", cfg), S3Sink)

    def test_rejects_unknown(self):
        with pytest.raises(ValueError, match="unknown sink"):
            get_sink("carrier-pigeon", Settings())


class TestLocalSink:
    def test_json_output(self, tmp_path: Path):
        written = LocalSink(tmp_path, "json").write(RECORDS, SUMMARY)
        payload = json.loads((tmp_path / "results.json").read_text())
        assert payload["summary"]["total"] == 2
        assert len(payload["records"]) == 2
        assert any("results.json" in p for p in written)

    def test_csv_output(self, tmp_path: Path):
        LocalSink(tmp_path, "csv").write(RECORDS, SUMMARY)
        with (tmp_path / "results.csv").open(encoding="utf-8-sig") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 2
        assert rows[0]["id"] == "A"

    def test_csv_header_starts_with_known_fields(self, tmp_path: Path):
        LocalSink(tmp_path, "csv").write(RECORDS, SUMMARY)
        with (tmp_path / "results.csv").open(encoding="utf-8-sig") as handle:
            header = next(csv.reader(handle))
        assert header[0] == "id"
        assert header[1] == "status"

    def test_excel_output(self, tmp_path: Path):
        LocalSink(tmp_path, "excel").write(RECORDS, SUMMARY)
        assert (tmp_path / "results.xlsx").exists()

    def test_summary_always_written(self, tmp_path: Path):
        LocalSink(tmp_path, "json").write(RECORDS, SUMMARY)
        assert json.loads((tmp_path / "summary.json").read_text())["errors"] == 1

    def test_creates_missing_output_directory(self, tmp_path: Path):
        target = tmp_path / "deep" / "nested"
        LocalSink(target, "json").write(RECORDS, SUMMARY)
        assert (target / "results.json").exists()

    def test_rejects_unknown_format(self, tmp_path: Path):
        with pytest.raises(ValueError, match="unknown output format"):
            LocalSink(tmp_path, "parquet").write(RECORDS, SUMMARY)

    def test_handles_empty_records(self, tmp_path: Path):
        LocalSink(tmp_path, "json").write([], {"total": 0})
        assert json.loads((tmp_path / "results.json").read_text())["records"] == []

    def test_is_always_available(self, tmp_path: Path):
        assert LocalSink(tmp_path, "json").is_available()


class TestS3Sink:
    def test_unavailable_without_bucket(self):
        assert not S3Sink("").is_available()

    def test_write_without_bucket_raises(self):
        with pytest.raises(ValueError, match="needs a bucket"):
            S3Sink("").write(RECORDS, SUMMARY)

    def test_strips_slashes_from_prefix(self):
        assert S3Sink("bucket", prefix="/nested/path/").prefix == "nested/path"
