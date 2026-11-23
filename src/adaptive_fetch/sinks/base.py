from abc import ABC, abstractmethod
from typing import Any


class Sink(ABC):
    name: str

    @abstractmethod
    def write(self, records: list[dict[str, Any]], summary: dict[str, Any]) -> list[str]: ...

    @abstractmethod
    def is_available(self) -> bool: ...
