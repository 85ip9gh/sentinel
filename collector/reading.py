"""The single message type every collector source produces."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from .config import SCHEMA_VERSION


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Reading:
    """One observation from one host at one instant.

    The Kafka key is the host name, so every reading from a machine lands on the
    same partition and stays in order relative to its own history.
    """

    host: str
    role: str
    kind: str
    ts: str
    data: dict[str, Any]

    @classmethod
    def now(
        cls,
        host: str,
        role: str,
        kind: str,
        data: dict[str, Any],
        clock: Callable[[], datetime] = utc_now,
    ) -> "Reading":
        return cls(
            host=host,
            role=role,
            kind=kind,
            ts=clock().isoformat().replace("+00:00", "Z"),
            data=data,
        )

    def key(self) -> bytes:
        return self.host.encode("utf-8")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA_VERSION,
            "host": self.host,
            "role": self.role,
            "kind": self.kind,
            "ts": self.ts,
            "data": self.data,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)
