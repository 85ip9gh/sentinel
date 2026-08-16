import json
from datetime import datetime, timezone

from dashboard.app import build_status, create_app
from dashboard.redact import PublicView
from dashboard.store import Archive

from .fakes import FakeWriter

NOW = datetime(2026, 8, 15, 17, 5, 0, tzinfo=timezone.utc)
ROOT = "/sentinel/raw"


def _write(writer, kind, date, records, name="part-a.ndjson"):
    body = "\n".join(json.dumps(r) for r in records) + "\n"
    writer.write(f"{ROOT}/{kind}/dt={date}/{name}", body)


def _system(host, ts, cpu=5.0, role="server"):
    return {
        "schema": 1,
        "host": host,
        "role": role,
        "kind": "system",
        "ts": ts,
        "data": {
            "cpu": {"percent": cpu, "load_avg": [0.4, 0.3, 0.2]},
            "memory": {"percent": 31.8, "available_bytes": 5_460_000_000},
            "disks": [{"mount": "/", "percent": 8.1}, {"mount": "/boot", "percent": 62.0}],
            "temperatures": [{"label": "Package id 0", "celsius": 66.0}],
            "uptime_seconds": 779_000,
            "process_count": 267,
        },
    }


def _archive(writer):
    return Archive(writer, ROOT)


def test_the_newest_reading_per_host_wins():
    writer = FakeWriter()
    _write(
        writer,
        "system",
        "2026-08-15",
        [
            _system("host-b", "2026-08-15T17:04:00Z", cpu=1.0),
            _system("host-b", "2026-08-15T17:04:50Z", cpu=9.0),
            _system("host-a", "2026-08-15T17:04:30Z", cpu=4.0),
        ],
    )

    status = build_status(_archive(writer), NOW)

    assert [h["host"] for h in status["hosts"]] == ["host-a", "host-b"]
    assert status["hosts"][1]["cpu_percent"] == 9.0


def test_a_host_card_reports_its_own_age_and_freshness():
    writer = FakeWriter()
    _write(writer, "system", "2026-08-15", [_system("host-b", "2026-08-15T17:04:30Z")])

    host = build_status(_archive(writer), NOW)["hosts"][0]

    assert host["age_seconds"] == 30
    assert host["freshness"] == "fresh"


def test_a_reading_that_stopped_arriving_reads_as_stale():
    writer = FakeWriter()
    _write(writer, "system", "2026-08-15", [_system("host-b", "2026-08-15T16:00:00Z")])

    host = build_status(_archive(writer), NOW)["hosts"][0]

    assert host["age_seconds"] == 3900
    assert host["freshness"] == "stale"


def test_the_worst_disk_is_the_one_shown():
    writer = FakeWriter()
    _write(writer, "system", "2026-08-15", [_system("host-b", "2026-08-15T17:04:30Z")])

    host = build_status(_archive(writer), NOW)["hosts"][0]

    assert host["disk_worst"]["mount"] == "/boot"
    assert host["temperature_max"] == 66.0


def test_yesterday_is_used_when_today_has_no_partition_yet():
    """Just past midnight UTC the current day can be empty."""
    writer = FakeWriter()
    _write(writer, "system", "2026-08-14", [_system("host-b", "2026-08-14T23:59:00Z")])

    status = build_status(_archive(writer), NOW)

    assert [h["host"] for h in status["hosts"]] == ["host-b"]


def test_site_checks_keep_the_newest_row_per_url():
    writer = FakeWriter()
    records = [
        {
            "host": "host-b",
            "kind": "http",
            "ts": "2026-08-15T17:00:00Z",
            "data": {"url": "https://pesanth.com", "ok": False, "status": 502, "latency_ms": 900.0},
        },
        {
            "host": "host-b",
            "kind": "http",
            "ts": "2026-08-15T17:04:00Z",
            "data": {"url": "https://pesanth.com", "ok": True, "status": 200, "latency_ms": 120.0},
        },
        {
            "host": "host-a",
            "kind": "http",
            "ts": "2026-08-15T17:04:00Z",
            "data": {"url": "https://pesanth.com", "ok": True, "status": 200, "latency_ms": 310.0},
        },
    ]
    _write(writer, "http", "2026-08-15", records)

    checks = build_status(_archive(writer), NOW)["checks"]

    assert len(checks) == 2
    assert {c["host"] for c in checks} == {"host-a", "host-b"}
    assert all(c["status"] == 200 for c in checks)


def test_containers_are_counted_from_the_newest_record_per_container():
    writer = FakeWriter()
    records = [
        {
            "host": "host-a",
            "kind": "container",
            "ts": "2026-08-15T17:00:00Z",
            "data": {"id": "aaa", "names": "sentinel-kafka", "state": "running"},
        },
        {
            "host": "host-a",
            "kind": "container",
            "ts": "2026-08-15T17:04:00Z",
            "data": {"id": "aaa", "names": "sentinel-kafka", "state": "running"},
        },
        {
            "host": "host-a",
            "kind": "container",
            "ts": "2026-08-15T17:04:00Z",
            "data": {"id": "bbb", "names": "sentinel-topics", "state": "exited"},
        },
    ]
    _write(writer, "container", "2026-08-15", records)

    containers = build_status(_archive(writer), NOW)["containers"]

    assert containers == [{"host": "host-a", "running": 1, "total": 2}]


def test_the_archive_summary_counts_days_and_files():
    writer = FakeWriter()
    _write(writer, "system", "2026-08-14", [_system("host-b", "2026-08-14T12:00:00Z")])
    _write(writer, "system", "2026-08-15", [_system("host-b", "2026-08-15T12:00:00Z")], "part-a.ndjson")
    _write(writer, "system", "2026-08-15", [_system("host-b", "2026-08-15T13:00:00Z")], "part-b.ndjson")

    row = next(r for r in build_status(_archive(writer), NOW)["archive"] if r["kind"] == "system")

    assert row["days"] == 2
    assert row["oldest"] == "2026-08-14"
    assert row["newest"] == "2026-08-15"
    assert row["files_newest_day"] == 2


def test_an_empty_archive_renders_rather_than_raising():
    status = build_status(_archive(FakeWriter()), NOW)

    assert status["hosts"] == []
    assert status["checks"] == []
    assert all(row["days"] == 0 for row in status["archive"])


def test_the_page_and_the_api_both_serve():
    writer = FakeWriter()
    _write(writer, "system", "2026-08-15", [_system("host-b", "2026-08-15T17:04:30Z")])
    client = create_app(_archive(writer), PublicView(enabled=False)).test_client()

    page = client.get("/")
    assert page.status_code == 200
    assert b"host-b" in page.data

    api = client.get("/api/status")
    assert api.status_code == 200
    assert api.get_json()["hosts"][0]["host"] == "host-b"

    assert client.get("/healthz").get_json() == {"ok": True}


def test_a_reading_stamped_in_the_future_is_not_shown():
    """A host whose clock has run ahead would otherwise win every comparison."""
    writer = FakeWriter()
    _write(
        writer,
        "system",
        "2026-08-15",
        [
            _system("host-b", "2026-08-15T17:04:30Z", cpu=3.0),
            _system("host-b", "2026-08-15T23:00:00Z", cpu=99.0),
        ],
    )

    host = build_status(_archive(writer), NOW)["hosts"][0]

    assert host["cpu_percent"] == 3.0
