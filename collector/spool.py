"""Disk buffer for readings that could not be delivered to Kafka.

This is the piece that makes a collector survive the broker being unreachable.
The workstation running Kafka is not on all the time, so a collector on the
server would otherwise drop everything it observed during the gap. Readings are
appended as newline-delimited JSON, one file per topic, and replayed in order
once the broker answers again.

The spool is bounded. When a file passes its cap the oldest lines are dropped,
because recent telemetry is worth more than old telemetry and unbounded growth
on a 256 GB server disk is its own incident.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable

log = logging.getLogger(__name__)


class Spool:
    def __init__(self, directory: Path | str, max_bytes: int) -> None:
        self.directory = Path(directory)
        self.max_bytes = max_bytes
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, topic: str) -> Path:
        return self.directory / f"{topic}.ndjson"

    def append(self, topic: str, payload: str) -> None:
        path = self.path_for(topic)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(payload.rstrip("\n") + "\n")
        self._trim(path)

    def pending(self, topic: str) -> int:
        return len(self._read(self.path_for(topic)))

    def drain(self, topic: str, send: Callable[[str], bool]) -> int:
        """Replay spooled payloads oldest first.

        The file is claimed by rename before anything is sent, so a caller that
        re-spools a failed payload during the drain writes into a fresh file
        instead of one this method is about to delete. That claim is what makes
        the spool safe to use from an asynchronous producer, where "it failed"
        arrives after the drain has moved on.

        `send` returns True when the payload was handed off. The first refusal
        stops the drain, and everything from that payload onward goes back to
        the front of the queue.
        """
        path = self.path_for(topic)
        inflight = path.with_suffix(".inflight")

        # A drain that died mid-replay leaves a claim behind. Recover it first.
        if inflight.exists():
            self._push_front(path, self._read(inflight))
            inflight.unlink()

        if not path.exists():
            return 0
        os.replace(path, inflight)

        lines = self._read(inflight)
        sent = 0
        for index, payload in enumerate(lines):
            if not send(payload):
                self._push_front(path, lines[index:])
                inflight.unlink(missing_ok=True)
                return sent
            sent += 1

        inflight.unlink(missing_ok=True)
        return sent

    def _read(self, path: Path) -> list[str]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as handle:
            return [line.rstrip("\n") for line in handle if line.strip()]

    def _push_front(self, path: Path, head: list[str]) -> None:
        if not head:
            return
        self._rewrite(path, head + self._read(path))
        if path.exists():
            self._trim(path)

    def _rewrite(self, path: Path, lines: list[str]) -> None:
        if not lines:
            path.unlink(missing_ok=True)
            return
        temp = path.with_suffix(path.suffix + ".tmp")
        with temp.open("w", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
        os.replace(temp, path)

    def _trim(self, path: Path) -> None:
        if path.stat().st_size <= self.max_bytes:
            return

        with path.open("r", encoding="utf-8") as handle:
            lines = [line.rstrip("\n") for line in handle if line.strip()]

        kept: list[str] = []
        size = 0
        for payload in reversed(lines):
            size += len(payload.encode("utf-8")) + 1
            if size > self.max_bytes:
                break
            kept.append(payload)
        kept.reverse()

        dropped = len(lines) - len(kept)
        if dropped:
            log.warning("spool %s over cap, dropped %d oldest readings", path.name, dropped)
        self._rewrite(path, kept)
