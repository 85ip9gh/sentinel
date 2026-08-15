"""Readings held in memory until they are worth a file."""

from __future__ import annotations

import time
from typing import Callable


class Batch:
    """Groups pending readings by the partition they belong to.

    One batch can span several partitions at once, which happens routinely
    around midnight and whenever a spooled backlog replays.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._groups: dict[tuple[str, str], list[str]] = {}
        self.count = 0
        self.opened_at = clock()

    def add(self, kind: str, date: str, payload: str) -> None:
        self._groups.setdefault((kind, date), []).append(payload)
        self.count += 1

    @property
    def empty(self) -> bool:
        return self.count == 0

    def age(self) -> float:
        return self._clock() - self.opened_at

    def is_full(self, max_records: int, max_seconds: float) -> bool:
        if self.empty:
            return False
        return self.count >= max_records or self.age() >= max_seconds

    def drain(self) -> list[tuple[str, str, list[str]]]:
        """Hand back every group and reset. Order is stable for predictable writes."""
        groups = [(kind, date, payloads) for (kind, date), payloads in sorted(self._groups.items())]
        self._groups = {}
        self.count = 0
        self.opened_at = self._clock()
        return groups
