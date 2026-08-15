"""Environment-driven settings for the collector.

Every value has a default that works on a developer machine, so the collector
runs with no configuration at all and is tuned per host through the environment.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path

SCHEMA_VERSION = 1

TOPICS = {
    "system": "sentinel.system",
    "http": "sentinel.http",
    "container": "sentinel.container",
}

_DEFAULT_SPOOL_MAX_BYTES = 64 * 1024 * 1024


def _env_str(env: dict, name: str, default: str) -> str:
    value = env.get(name, "").strip()
    return value or default


def _env_float(env: dict, name: str, default: float) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero, got {value}")
    return value


def _env_int(env: dict, name: str, default: int) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero, got {value}")
    return value


def _env_list(env: dict, name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(frozen=True)
class Config:
    """Resolved collector settings for one host."""

    bootstrap: str
    host: str
    role: str
    system_interval: float
    http_interval: float
    container_interval: float
    http_targets: tuple[str, ...]
    http_timeout: float
    spool_dir: Path
    spool_max_bytes: int
    flush_timeout: float

    @classmethod
    def from_env(cls, env: dict | None = None) -> "Config":
        env = dict(os.environ if env is None else env)
        return cls(
            bootstrap=_env_str(env, "SENTINEL_BOOTSTRAP", "localhost:9092"),
            host=_env_str(env, "SENTINEL_HOST", socket.gethostname().lower()),
            role=_env_str(env, "SENTINEL_ROLE", "unknown"),
            system_interval=_env_float(env, "SENTINEL_SYSTEM_INTERVAL", 10.0),
            http_interval=_env_float(env, "SENTINEL_HTTP_INTERVAL", 60.0),
            container_interval=_env_float(env, "SENTINEL_CONTAINER_INTERVAL", 30.0),
            http_targets=_env_list(env, "SENTINEL_HTTP_TARGETS", ()),
            http_timeout=_env_float(env, "SENTINEL_HTTP_TIMEOUT", 10.0),
            spool_dir=Path(_env_str(env, "SENTINEL_SPOOL_DIR", "var/spool")),
            spool_max_bytes=_env_int(
                env, "SENTINEL_SPOOL_MAX_BYTES", _DEFAULT_SPOOL_MAX_BYTES
            ),
            flush_timeout=_env_float(env, "SENTINEL_FLUSH_TIMEOUT", 5.0),
        )
