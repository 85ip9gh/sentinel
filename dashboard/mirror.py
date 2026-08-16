"""A copy of the public view, served from a host that is always on.

The archive lives on a workstation that gets switched off. When it was also the
thing serving the page, the whole site went dark with it, and a dark site is a
statement about a building rather than about a computer: anyone watching the
domain learned when its owner was home.

So the page moves to the always-on server and pulls the document across the
tailnet instead. What changes is the failure mode. The site now stays up when
the workstation goes away, says plainly that the archive is unreachable, and
lets the cards age out on their own. One card going quiet is a much weaker
signal than a domain that stops answering.

The mirror deliberately holds no archive of its own. It fetches the already
redacted `/api/status`, so the projection is still decided in exactly one place
and this process cannot widen it. Storing readings here would mean a second
copy of the same telemetry on a second machine, which is more exposure, not
less.

Ages are rebased on the way out. A cached document carries the ages that were
true when it was fetched, and serving those unchanged would show a machine that
has been off for an hour as fresh, which is the one lie a monitoring page must
not tell.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests
from flask import Flask, Response, jsonify, render_template

from .app import _freshness
from .store import _parse_ts

log = logging.getLogger(__name__)

# Past this, the page says so rather than presenting a cached document as if it
# were current. Three sink batches: long enough to ride out a restart of the
# upstream, short enough that a real outage is visible within minutes.
DEFAULT_STALE_SECONDS = 300.0


@dataclass(frozen=True)
class MirrorConfig:
    upstream: str = "http://127.0.0.1:8088/api/status"
    interval: float = 20.0
    timeout: float = 5.0
    cache_path: Path = Path("var/mirror-status.json")
    stale_seconds: float = DEFAULT_STALE_SECONDS

    @classmethod
    def from_env(cls, env: dict | None = None) -> "MirrorConfig":
        env = dict(os.environ if env is None else env)

        def number(name: str, default: float) -> float:
            raw = env.get(name, "").strip()
            try:
                value = float(raw) if raw else default
            except ValueError:
                return default
            return value if value > 0 else default

        return cls(
            upstream=env.get("SENTINEL_MIRROR_UPSTREAM", "").strip() or cls.upstream,
            interval=number("SENTINEL_MIRROR_INTERVAL", cls.interval),
            timeout=number("SENTINEL_MIRROR_TIMEOUT", cls.timeout),
            cache_path=Path(
                env.get("SENTINEL_MIRROR_CACHE", "").strip() or cls.cache_path
            ),
            stale_seconds=number("SENTINEL_MIRROR_STALE_SECONDS", cls.stale_seconds),
        )


def rebase(
    document: dict[str, Any], now: datetime, stale_seconds: float, error: str | None
) -> dict[str, Any]:
    """Age a cached document forward to the moment it is being served.

    Every age in the document was measured against the upstream's own clock at
    `generated_at`, so the elapsed time since then is added to all of them and
    the freshness words are recomputed from the result. A host that stopped
    reporting an hour ago reads as an hour stale, whether the archive went away
    or the host did.
    """
    generated = _parse_ts(document.get("generated_at", ""))
    elapsed = 0.0 if generated is None else max(0.0, (now - generated).total_seconds())

    def aged(row: dict[str, Any]) -> dict[str, Any]:
        age = row.get("age_seconds")
        if age is None:
            return dict(row)
        return {**row, "age_seconds": round(age + elapsed)}

    hosts = []
    for host in document.get("hosts", []):
        row = aged(host)
        row["freshness"] = _freshness(row.get("age_seconds"))
        hosts.append(row)

    return {
        **document,
        "hosts": hosts,
        "checks": [aged(check) for check in document.get("checks", [])],
        "upstream": {
            "age_seconds": round(elapsed),
            "stale": elapsed > stale_seconds or error is not None,
            "error": error,
        },
    }


class Upstream:
    """The last good document, and how old it is.

    A cache on disk rather than in memory only, because the reason this process
    exists is to survive the other end being unavailable, and a restart while
    the workstation is off would otherwise leave an empty page.
    """

    def __init__(
        self,
        config: MirrorConfig,
        fetch: Callable[[], dict[str, Any]] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._fetch = fetch or self._http_fetch
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.Lock()
        self._document: dict[str, Any] | None = self._load()
        self._error: str | None = None if self._document else "no reading yet"

    def _http_fetch(self) -> dict[str, Any]:
        response = requests.get(self._config.upstream, timeout=self._config.timeout)
        response.raise_for_status()
        document = response.json()
        if not isinstance(document, dict):
            raise ValueError("upstream did not return an object")
        return document

    def _load(self) -> dict[str, Any] | None:
        try:
            return json.loads(self._config.cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _save(self, document: dict[str, Any]) -> None:
        path = self._config.cache_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Written through a temporary file: a crash mid-write would
            # otherwise leave a truncated cache and an empty page on restart.
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps(document), encoding="utf-8")
            temp.replace(path)
        except OSError as exc:
            log.warning("could not write cache: %s", exc)

    def refresh(self) -> bool:
        try:
            document = self._fetch()
        except Exception as exc:  # noqa: BLE001 - any failure means serve the cache
            # The class name only. A requests exception carries the URL it
            # failed on, and that URL is the tailnet address of the archive
            # host, which has no business in a document served to the internet.
            # The detail goes to the log, where the operator can read it.
            with self._lock:
                self._error = type(exc).__name__
            log.warning("upstream unreachable: %s: %s", type(exc).__name__, exc)
            return False
        with self._lock:
            self._document = document
            self._error = None
        self._save(document)
        return True

    def status(self) -> dict[str, Any]:
        with self._lock:
            document, error = self._document, self._error
        if document is None:
            return {
                "generated_at": self._clock().isoformat().replace("+00:00", "Z"),
                "public": True,
                "hosts": [],
                "checks": [],
                "containers": [],
                "archive": [],
                "upstream": {"age_seconds": None, "stale": True, "error": error},
            }
        return rebase(document, self._clock(), self._config.stale_seconds, error)


def create_mirror_app(upstream: Upstream) -> Flask:
    app = Flask(__name__)

    @app.after_request
    def _headers(response: Response) -> Response:
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; "
            "form-action 'none'; frame-ancestors 'none'"
        )
        return response

    @app.get("/robots.txt")
    def robots():
        return Response("User-agent: *\nDisallow: /\n", mimetype="text/plain")

    @app.get("/healthz")
    def healthz():
        # The mirror is healthy when it is serving, even with a stale document.
        # Tying this to the upstream would take the site down for the exact
        # outage the mirror exists to absorb.
        return {"ok": True}

    @app.get("/api/status")
    def api_status():
        return jsonify(upstream.status())

    @app.get("/")
    def index():
        return render_template("index.html", **upstream.status())

    return app


def poll_forever(upstream: Upstream, interval: float, stop: threading.Event) -> None:
    while not stop.wait(interval):
        upstream.refresh()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    config = MirrorConfig.from_env()
    upstream = Upstream(config)
    upstream.refresh()

    stop = threading.Event()
    threading.Thread(
        target=poll_forever, args=(upstream, config.interval, stop), daemon=True
    ).start()

    host = os.environ.get("SENTINEL_MIRROR_HOST", "127.0.0.1")
    port = int(os.environ.get("SENTINEL_MIRROR_PORT", "8088"))

    from waitress import serve

    log.info("mirror on %s:%d following %s", host, port, config.upstream)
    serve(create_mirror_app(upstream), host=host, port=port, threads=4)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
