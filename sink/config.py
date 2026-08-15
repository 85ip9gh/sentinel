"""Settings for the HDFS sink."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_TOPICS = ("sentinel.system", "sentinel.http", "sentinel.container")


def _env_str(env: dict, name: str, default: str) -> str:
    return env.get(name, "").strip() or default


def _env_int(env: dict, name: str, default: int) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero, got {value}")
    return value


def _env_float(env: dict, name: str, default: float) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    value = float(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero, got {value}")
    return value


@dataclass(frozen=True)
class SinkConfig:
    bootstrap: str
    group_id: str
    topics: tuple[str, ...]
    hdfs_url: str
    hdfs_user: str
    root: str
    batch_max_records: int
    batch_max_seconds: float
    poll_timeout: float

    @classmethod
    def from_env(cls, env: dict | None = None) -> "SinkConfig":
        env = dict(os.environ if env is None else env)
        topics = _env_str(env, "SENTINEL_SINK_TOPICS", ",".join(DEFAULT_TOPICS))
        return cls(
            bootstrap=_env_str(env, "SENTINEL_BOOTSTRAP", "kafka:9095"),
            group_id=_env_str(env, "SENTINEL_SINK_GROUP", "sentinel-hdfs-sink"),
            topics=tuple(t.strip() for t in topics.split(",") if t.strip()),
            hdfs_url=_env_str(env, "SENTINEL_HDFS_URL", "http://namenode:9870"),
            hdfs_user=_env_str(env, "SENTINEL_HDFS_USER", "hadoop"),
            root=_env_str(env, "SENTINEL_HDFS_ROOT", "/sentinel/raw"),
            # A batch is closed by whichever limit is reached first. The record
            # count keeps files from growing unbounded on a busy topic, and the
            # time limit keeps a quiet topic from sitting unwritten.
            batch_max_records=_env_int(env, "SENTINEL_SINK_BATCH_RECORDS", 500),
            batch_max_seconds=_env_float(env, "SENTINEL_SINK_BATCH_SECONDS", 60.0),
            poll_timeout=_env_float(env, "SENTINEL_SINK_POLL_TIMEOUT", 1.0),
        )
