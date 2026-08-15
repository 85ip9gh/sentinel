"""Container inventory, read from the docker CLI rather than the socket.

Using the CLI keeps the collector free of a docker SDK dependency and free of
any need for socket permissions beyond what the invoking user already has. A
host without docker reports an empty list, which is a fact rather than an error.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from typing import Any, Callable

log = logging.getLogger(__name__)

Runner = Callable[[list[str]], str]

_FIELDS = ("ID", "Names", "Image", "State", "Status", "RunningFor")


def _default_runner(command: list[str]) -> str:
    result = subprocess.run(
        command, capture_output=True, text=True, timeout=15, check=True
    )
    return result.stdout


def available() -> bool:
    # Looked up through the module rather than bound as a default argument, so
    # the lookup happens at call time and stays patchable in tests.
    return shutil.which("docker") is not None


def collect(runner: Runner | None = None, check_available: bool = True) -> list[dict[str, Any]]:
    if check_available and not available():
        return []

    runner = runner or _default_runner
    try:
        raw = runner(["docker", "ps", "--all", "--no-trunc", "--format", "{{json .}}"])
    except Exception as exc:  # noqa: BLE001 - docker being down is an observation
        log.debug("docker ps failed: %s", exc)
        return []

    containers: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        containers.append({field.lower(): entry.get(field) for field in _FIELDS})
    return containers
