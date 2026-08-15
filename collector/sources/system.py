"""Host metrics from psutil.

Everything here is cheap enough to run every 10 seconds on a laptop-class
machine. Anything that needs a subprocess or a network call belongs elsewhere.
"""

from __future__ import annotations

import time
from typing import Any

import psutil

# Pseudo filesystems that report meaningless usage and would drown the real disks.
_SKIP_FSTYPES = {"squashfs", "tmpfs", "devtmpfs", "overlay", "ramfs", "autofs"}


def _disks() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for part in psutil.disk_partitions(all=False):
        if part.fstype.lower() in _SKIP_FSTYPES:
            continue
        try:
            usage = psutil.disk_usage(part.mountpoint)
        except (PermissionError, OSError):
            # Empty optical drives and unreadable mounts, not worth an error.
            continue
        out.append(
            {
                "mount": part.mountpoint,
                "fstype": part.fstype,
                "total_bytes": usage.total,
                "used_bytes": usage.used,
                "percent": usage.percent,
            }
        )
    return out


def _temperatures() -> list[dict[str, Any]]:
    getter = getattr(psutil, "sensors_temperatures", None)
    if getter is None:
        return []
    try:
        raw = getter()
    except (OSError, NotImplementedError):
        return []
    out: list[dict[str, Any]] = []
    for chip, entries in (raw or {}).items():
        for entry in entries:
            if entry.current is None:
                continue
            out.append(
                {
                    "chip": chip,
                    "label": entry.label or chip,
                    "celsius": round(entry.current, 1),
                    "high": entry.high,
                    "critical": entry.critical,
                }
            )
    return out


def _load() -> list[float] | None:
    getter = getattr(psutil, "getloadavg", None)
    if getter is None:
        return None
    try:
        return [round(value, 2) for value in getter()]
    except (OSError, NotImplementedError):
        return None


def collect() -> dict[str, Any]:
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    net = psutil.net_io_counters()
    boot = psutil.boot_time()

    return {
        "cpu": {
            # Non-blocking call: the value covers the interval since the previous
            # call, which the collection loop makes exactly one of per cycle.
            "percent": psutil.cpu_percent(interval=None),
            "count_logical": psutil.cpu_count(logical=True),
            "load_avg": _load(),
        },
        "memory": {
            "total_bytes": memory.total,
            "available_bytes": memory.available,
            "used_bytes": memory.used,
            "percent": memory.percent,
        },
        "swap": {
            "total_bytes": swap.total,
            "used_bytes": swap.used,
            "percent": swap.percent,
        },
        "disks": _disks(),
        "network": {
            "bytes_sent": net.bytes_sent,
            "bytes_recv": net.bytes_recv,
            "errin": net.errin,
            "errout": net.errout,
            "dropin": net.dropin,
            "dropout": net.dropout,
        },
        "temperatures": _temperatures(),
        "uptime_seconds": round(time.time() - boot, 1),
        "boot_time": boot,
        "process_count": len(psutil.pids()),
    }
