"""Reading the archive back out.

The dashboard reads HDFS and nothing else. It could have kept a second Kafka
consumer for a fresher view, and deliberately does not: a page served from the
archive proves the archive is real, and a page served from the stream would look
identical whether or not anything had been stored.

The cost is honest and visible. Readings appear here one sink batch late, and
every card shows the age of the reading it is displaying.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sink.partition import partition_dir
from sink.writer import Writer

KINDS = ("system", "http", "container")


def _parse_ts(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class Archive:
    def __init__(self, writer: Writer, root: str) -> None:
        self._writer = writer
        self._root = root

    def dates(self, kind: str) -> list[str]:
        """Every partition present for a kind, newest first."""
        entries = self._writer.listing(f"{self._root.rstrip('/')}/{kind}")
        return sorted(
            (e.removeprefix("dt=") for e in entries if e.startswith("dt=")), reverse=True
        )

    def files(self, kind: str, date: str) -> list[str]:
        return self._writer.listing(partition_dir(self._root, kind, date))

    def records(self, kind: str, date: str, newest_files: int = 3) -> list[dict[str, Any]]:
        """Parse the most recent partition files for one day."""
        directory = partition_dir(self._root, kind, date)
        out: list[dict[str, Any]] = []
        for name in sorted(self.files(kind, date))[-newest_files:]:
            try:
                body = self._writer.read(f"{directory}/{name}")
            except Exception:  # noqa: BLE001 - a file being written is not an error
                continue
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return out

    def latest_by_host(self, kind: str, days_back: int = 2) -> dict[str, dict[str, Any]]:
        """The newest reading of a kind per host.

        Looks back more than one day on purpose. Just after midnight UTC the
        current partition can be empty while yesterday's holds everything.
        """
        today = datetime.now(timezone.utc).date()
        latest: dict[str, dict[str, Any]] = {}

        for offset in range(days_back):
            date = (today - timedelta(days=offset)).isoformat()
            for record in self.records(kind, date):
                host = record.get("host")
                ts = _parse_ts(record.get("ts", ""))
                if not host or ts is None:
                    continue
                held = latest.get(host)
                if held is None or ts > _parse_ts(held["ts"]):
                    latest[host] = record
            if latest:
                break
        return latest

    def summary(self) -> list[dict[str, Any]]:
        """Partition counts per kind, which is the archive proving it exists."""
        rows = []
        for kind in KINDS:
            dates = self.dates(kind)
            rows.append(
                {
                    "kind": kind,
                    "days": len(dates),
                    "newest": dates[0] if dates else None,
                    "oldest": dates[-1] if dates else None,
                    "files_newest_day": len(self.files(kind, dates[0])) if dates else 0,
                }
            )
        return rows


def age_seconds(record: dict[str, Any], now: datetime | None = None) -> float | None:
    ts = _parse_ts(record.get("ts", ""))
    if ts is None:
        return None
    return ((now or datetime.now(timezone.utc)) - ts).total_seconds()
