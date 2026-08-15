"""Reachability checks against the public sites.

`fetch` is injected so the check can be tested without a network, and so a
future version can swap in a session with retries or custom headers.
"""

from __future__ import annotations

import time
from typing import Any, Callable

Fetch = Callable[[str, float], Any]


def _default_fetch(url: str, timeout: float) -> Any:
    import requests

    return requests.get(url, timeout=timeout, allow_redirects=True)


def check(url: str, timeout: float = 10.0, fetch: Fetch | None = None) -> dict[str, Any]:
    fetch = fetch or _default_fetch
    started = time.perf_counter()
    try:
        response = fetch(url, timeout)
    except Exception as exc:  # noqa: BLE001 - any failure is the observation
        return {
            "url": url,
            "ok": False,
            "status": None,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "error": f"{type(exc).__name__}: {exc}",
        }

    status = getattr(response, "status_code", None)
    return {
        "url": url,
        "ok": bool(status is not None and 200 <= status < 400),
        "status": status,
        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        "error": None,
    }


def check_all(
    urls: tuple[str, ...], timeout: float = 10.0, fetch: Fetch | None = None
) -> list[dict[str, Any]]:
    return [check(url, timeout, fetch) for url in urls]
