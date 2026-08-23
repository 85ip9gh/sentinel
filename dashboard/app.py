"""A read-only view over the archive."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from flask import Flask, Response, jsonify, render_template

from .redact import PublicView
from .store import Archive, age_seconds

# A reading older than this is stale enough to mean something is wrong, given
# system readings arrive every 10 seconds and the sink batches on top of that.
FRESH_SECONDS = 180.0
STALE_SECONDS = 600.0


def _freshness(age: float | None) -> str:
    if age is None:
        return "unknown"
    if age <= FRESH_SECONDS:
        return "fresh"
    if age <= STALE_SECONDS:
        return "late"
    return "stale"


def _gb(value: Any) -> float | None:
    return round(value / 1e9, 1) if isinstance(value, (int, float)) else None


def host_view(record: dict[str, Any], now: datetime) -> dict[str, Any]:
    data = record.get("data", {})
    disks = data.get("disks") or []
    temps = data.get("temperatures") or []
    age = age_seconds(record, now)
    uptime_days = round(data.get("uptime_seconds", 0) / 86400, 2)

    return {
        "host": record.get("host"),
        "role": record.get("role"),
        "ts": record.get("ts"),
        "age_seconds": None if age is None else round(age),
        "freshness": _freshness(age),
        "cpu_percent": data.get("cpu", {}).get("percent"),
        "load_avg": data.get("cpu", {}).get("load_avg"),
        "memory_percent": data.get("memory", {}).get("percent"),
        "memory_available_gb": _gb(data.get("memory", {}).get("available_bytes")),
        "disk_worst": max(disks, key=lambda d: d.get("percent", 0)) if disks else None,
        "temperature_max": max((t.get("celsius", 0) for t in temps), default=None),
        "uptime_days": uptime_days,
        # The template renders the label, so the public view can coarsen the
        # figure without the page having to know which mode it is in.
        "uptime_label": f"{uptime_days} d",
        "process_count": data.get("process_count"),
    }


def build_status(archive: Archive, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)

    system = archive.latest_by_host("system", not_after=now)
    hosts = sorted(
        (host_view(record, now) for record in system.values()),
        key=lambda h: (h["host"] or ""),
    )

    # One reading covers one URL, so the newest reading per host is not enough.
    # The day's records get folded down to the newest per host and URL.
    checks = []
    http_records = archive.records("http", now.date().isoformat(), not_after=now)
    newest_by_url: dict[tuple[str, str], dict[str, Any]] = {}
    for record in http_records:
        data = record.get("data", {})
        key = (record.get("host", ""), data.get("url", ""))
        held = newest_by_url.get(key)
        if held is None or record.get("ts", "") > held.get("ts", ""):
            newest_by_url[key] = record
    for (host, url), record in sorted(newest_by_url.items()):
        data = record.get("data", {})
        age = age_seconds(record, now)
        checks.append(
            {
                "host": host,
                "url": url,
                "ok": data.get("ok"),
                "status": data.get("status"),
                "latency_ms": data.get("latency_ms"),
                "error": data.get("error"),
                "age_seconds": None if age is None else round(age),
            }
        )

    # Each container is its own reading, so a host's inventory is rebuilt by
    # keeping the newest record per container id and discarding anything that
    # has not been seen recently. A container that is removed simply stops being
    # reported, and nothing emits a tombstone for it.
    newest_by_container: dict[tuple[str, str], dict[str, Any]] = {}
    for record in archive.records(
        "container", now.date().isoformat(), newest_files=6, not_after=now
    ):
        data = record.get("data", {})
        if not isinstance(data, dict):
            continue
        key = (record.get("host", ""), data.get("id", ""))
        held = newest_by_container.get(key)
        if held is None or record.get("ts", "") > held.get("ts", ""):
            newest_by_container[key] = record

    per_host: dict[str, dict[str, int]] = {}
    for (host, _), record in newest_by_container.items():
        age = age_seconds(record, now)
        if age is None or age > STALE_SECONDS:
            continue
        counts = per_host.setdefault(host, {"running": 0, "total": 0})
        counts["total"] += 1
        if record.get("data", {}).get("state") == "running":
            counts["running"] += 1
    containers = [
        {"host": host, "running": counts["running"], "total": counts["total"]}
        for host, counts in sorted(per_host.items())
    ]

    return {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "hosts": hosts,
        "checks": checks,
        "containers": containers,
        "archive": archive.summary(),
    }


def create_app(archive: Archive, view: PublicView | None = None) -> Flask:
    # No view supplied means the defaults, and the defaults redact. A dashboard
    # constructed by a caller that has not thought about it must not be the one
    # that publishes hostnames.
    view = view or PublicView()
    app = Flask(__name__)

    def status() -> dict[str, Any]:
        """The single place a status document is produced for a request.

        Both the page and the API go through here, so the projection cannot be
        applied to one and forgotten on the other.
        """
        now = datetime.now(timezone.utc) - timedelta(seconds=view.lag_seconds)
        return view.apply(build_status(archive, now))

    @app.after_request
    def _headers(response: Response) -> Response:
        # Indexable since 2026-08-23, reversing the earlier noindex. The rest
        # of the set stays: no-store keeps a cached copy of telemetry from
        # outliving the reading it came from, which being listed does not.
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
        return Response("User-agent: *\nAllow: /\n", mimetype="text/plain")

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/api/status")
    def api_status():
        return jsonify(status())

    @app.get("/")
    def index():
        return render_template("index.html", **status())

    return app
