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


def _system(host, ts, role="workstation", uptime=779_000):
    return {
        "schema": 1,
        "host": host,
        "role": role,
        "kind": "system",
        "ts": ts,
        "data": {
            "cpu": {"percent": 27.5, "load_avg": [0.4, 0.3, 0.2]},
            "memory": {"percent": 91.3, "available_bytes": 3_000_000_000},
            "disks": [
                {"device": "G:\\", "mount": "G:\\", "fstype": "FAT32",
                 "percent": 46.0, "total_bytes": 16_106_127_360},
            ],
            "temperatures": [{"label": "Package id 0", "celsius": 66.0}],
            "uptime_seconds": uptime,
            "process_count": 381,
        },
    }


def _status(writer, view=None):
    return (view or PublicView()).apply(build_status(Archive(writer, ROOT), NOW))


def _one_host(**kwargs):
    writer = FakeWriter()
    _write(writer, "system", "2026-08-15", [_system("cubebox", "2026-08-15T17:04:50Z")])
    return _status(writer, **kwargs)["hosts"][0]


def test_the_real_hostname_never_reaches_the_public_document():
    host = _one_host()

    assert host["host"] != "cubebox"
    assert host["host"].startswith("host-")


def test_a_configured_alias_is_used_instead_of_the_digest():
    view = PublicView(aliases={"cubebox": "workstation-01"})

    assert _one_host(view=view)["host"] == "workstation-01"


def test_an_alias_is_stable_across_calls_so_a_host_can_be_followed():
    assert _one_host()["host"] == _one_host()["host"]


def test_two_hosts_do_not_collapse_into_one_alias():
    view = PublicView()

    assert view.alias("cubebox") != view.alias("g7-server")


def test_the_salt_changes_the_digest_so_a_guessed_hostname_proves_nothing():
    assert PublicView(salt="a").alias("cubebox") != PublicView(salt="b").alias("cubebox")


def test_machine_identifying_detail_is_withheld():
    host = _one_host()

    assert host["disk_worst"] == {"mount": None, "percent": 46.0}
    assert host["memory_available_gb"] is None
    assert host["process_count"] is None
    # The percentages are the point of the page and say nothing about hardware.
    assert host["memory_percent"] == 91.3
    assert host["cpu_percent"] == 27.5


def test_uptime_is_bucketed_rather_than_exact():
    host = _one_host()

    assert host["uptime_days"] is None
    assert host["uptime_label"] == "7 to 30 days"


def test_checks_and_containers_are_aliased_with_the_same_name_as_the_card():
    writer = FakeWriter()
    _write(writer, "system", "2026-08-15", [_system("cubebox", "2026-08-15T17:04:50Z")])
    _write(
        writer,
        "http",
        "2026-08-15",
        [{"host": "cubebox", "kind": "http", "ts": "2026-08-15T17:04:00Z",
          "data": {"url": "https://pesanth.com", "ok": True, "status": 200,
                   "latency_ms": 120.0}}],
    )
    _write(
        writer,
        "container",
        "2026-08-15",
        [{"host": "cubebox", "kind": "container", "ts": "2026-08-15T17:04:00Z",
          "data": {"id": "aaa", "state": "running"}}],
    )

    status = _status(writer)
    alias = status["hosts"][0]["host"]

    assert status["checks"][0]["host"] == alias
    assert status["containers"][0]["host"] == alias
    assert "cubebox" not in json.dumps(status)


def test_the_private_view_keeps_everything():
    host = _one_host(view=PublicView(enabled=False))

    assert host["host"] == "cubebox"
    assert host["process_count"] == 381
    assert host["uptime_label"] == "9.02 d"


def test_the_default_is_redacted_because_a_missing_setting_must_fail_closed():
    assert PublicView.from_env({}).enabled is True
    assert PublicView.from_env({"SENTINEL_PUBLIC": "0"}).enabled is False
    assert PublicView.from_env({"SENTINEL_PUBLIC": "1"}).enabled is True


def test_aliases_parse_from_one_environment_string():
    view = PublicView.from_env(
        {"SENTINEL_HOST_ALIASES": "cubebox=workstation-01, G7-Server=edge-01, junk"}
    )

    assert view.alias("cubebox") == "workstation-01"
    assert view.alias("g7-server") == "edge-01"


def test_an_unreadable_lag_serves_live_rather_than_refusing_to_serve():
    assert PublicView.from_env({"SENTINEL_PUBLIC_LAG_SECONDS": "soon"}).lag_seconds == 0.0
    assert PublicView.from_env({"SENTINEL_PUBLIC_LAG_SECONDS": "-5"}).lag_seconds == 0.0
    assert PublicView.from_env({"SENTINEL_PUBLIC_LAG_SECONDS": "3600"}).lag_seconds == 3600.0


def test_both_the_page_and_the_api_are_redacted_by_default():
    writer = FakeWriter()
    _write(writer, "system", "2026-08-15", [_system("cubebox", "2026-08-15T17:04:50Z")])
    client = create_app(Archive(writer, ROOT)).test_client()

    page = client.get("/")
    api = client.get("/api/status")

    assert b"cubebox" not in page.data
    assert b"cubebox" not in api.data
    assert api.get_json()["public"] is True


def test_the_public_page_is_indexable_but_not_cacheable():
    client = create_app(Archive(FakeWriter(), ROOT)).test_client()

    page = client.get("/")

    assert "X-Robots-Tag" not in page.headers
    assert page.headers["Cache-Control"] == "no-store"
    assert client.get("/robots.txt").data == b"User-agent: *\nAllow: /\n"
