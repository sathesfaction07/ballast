import json
from pathlib import Path
from typing import Any


class Checkpoint:
    def __init__(self, directory: Path, save_every: int = 25) -> None:
        self.path = directory / "progress.json"
        self.save_every = save_every
        self.completed: set[str] = set()
        self.records: list[dict[str, Any]] = []
        self.halted_reason: str | None = None
        self._since_save = 0
        directory.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            state = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return
        self.completed = set(state.get("completed", []))
        self.records = state.get("records", [])
        self.halted_reason = state.get("halted_reason")

    def remaining(self, item_ids: list[str]) -> list[str]:
        return [i for i in item_ids if i not in self.completed]

    def mark(self, item_id: str, record: dict[str, Any]) -> None:
        self.completed.add(item_id)
        self.records.append(record)
        self._since_save += 1
        if self._since_save >= self.save_every:
            self.save()

    def halt(self, reason: str) -> None:
        self.halted_reason = reason
        self.save()

    def save(self) -> None:
        payload = {
            "completed": sorted(self.completed),
            "records": self.records,
            "halted_reason": self.halted_reason,
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.path)
        self._since_save = 0
