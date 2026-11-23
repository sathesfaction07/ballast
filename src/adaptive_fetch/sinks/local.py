import csv
import json
from pathlib import Path
from typing import Any

from .base import Sink

FIELD_ORDER = ["id", "status", "attempts", "payload_mode", "bytes", "error"]


def _rows(records: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    extra = sorted({k for r in records for k in r} - set(FIELD_ORDER))
    header = [f for f in FIELD_ORDER if any(f in r for r in records)] + extra
    return header, records


class LocalSink(Sink):
    name = "local"

    def __init__(self, output_path: Path, output_format: str = "json") -> None:
        self.output_path = output_path
        self.output_format = output_format

    def is_available(self) -> bool:
        return True

    def write(self, records: list[dict[str, Any]], summary: dict[str, Any]) -> list[str]:
        self.output_path.mkdir(parents=True, exist_ok=True)
        written: list[str] = []

        if self.output_format == "json":
            target = self.output_path / "results.json"
            target.write_text(
                json.dumps({"summary": summary, "records": records}, indent=2, default=str)
            )
            written.append(str(target))
        elif self.output_format == "csv":
            target = self.output_path / "results.csv"
            header, rows = _rows(records)
            with target.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.DictWriter(handle, fieldnames=header, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            written.append(str(target))
        elif self.output_format == "excel":
            written.append(self._write_excel(records))
        else:
            raise ValueError(f"unknown output format: {self.output_format}")

        summary_path = self.output_path / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, default=str))
        written.append(str(summary_path))
        return written

    def _write_excel(self, records: list[dict[str, Any]]) -> str:
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font
        except ImportError as exc:
            raise RuntimeError(
                "excel output needs openpyxl. Install with: pip install 'ballast[excel]'"
            ) from exc

        header, rows = _rows(records)
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "results"
        sheet.append(header)
        for cell in sheet[1]:
            cell.font = Font(bold=True)

        colours = {"ok": "00A36C", "error": "C00000", "skipped": "808080"}
        for row in rows:
            sheet.append([row.get(key, "") for key in header])
            status = str(row.get("status", ""))
            if status in colours and "status" in header:
                sheet.cell(sheet.max_row, header.index("status") + 1).font = Font(
                    color=colours[status]
                )

        for index, key in enumerate(header, start=1):
            width = max(len(key), *(len(str(r.get(key, ""))) for r in rows)) if rows else len(key)
            sheet.column_dimensions[sheet.cell(1, index).column_letter].width = min(width + 2, 60)

        target = self.output_path / "results.xlsx"
        workbook.save(target)
        return str(target)
