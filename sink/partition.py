"""Where a reading belongs in HDFS.

Partitioning is by the reading's own timestamp, not by arrival. A collector that
spent six hours spooled writes into the days it observed, so replaying a backlog
repairs history instead of stacking it all onto the day the broker came back.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

# Anything the sink cannot place in time goes here rather than being dropped or
# silently filed under today. It is a small pile that can be repaired later, and
# an empty one is evidence the timestamps are sound.
UNDATED = "unknown"


def kind_from_topic(topic: str) -> str:
    """`sentinel.system` becomes `system`."""
    return topic.split(".", 1)[1] if "." in topic else topic


def event_date(payload: str) -> str:
    """The UTC date of the reading's own timestamp."""
    try:
        record = json.loads(payload)
        ts = record["ts"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return UNDATED

    if not isinstance(ts, str):
        return UNDATED
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return UNDATED

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d")


def partition_dir(root: str, kind: str, date: str) -> str:
    return f"{root.rstrip('/')}/{kind}/dt={date}"


def file_name(clock: Any = None, token: str | None = None) -> str:
    """A unique, sortable name. Files are written once and never appended to.

    Appending over WebHDFS would mean a network round trip per reading and a
    partial file after any crash. One immutable file per batch costs a little
    more space and cannot be half written.
    """
    now = (clock or (lambda: datetime.now(timezone.utc)))()
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    suffix = token or uuid.uuid4().hex[:8]
    return f"part-{stamp}-{suffix}.ndjson"
