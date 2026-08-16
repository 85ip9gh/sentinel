import json
from datetime import datetime, timedelta, timezone

from dashboard.mirror import MirrorConfig, Upstream, create_mirror_app, rebase

NOW = datetime(2026, 8, 16, 12, 0, 0, tzinfo=timezone.utc)


def _document(generated="2026-08-16T12:00:00Z", age=20):
    return {
        "generated_at": generated,
        "public": True,
        "hosts": [
            {
                "host": "workstation-01",
                "role": "workstation",
                "age_seconds": age,
                "freshness": "fresh",
                "cpu_percent": 12.0,
                "uptime_label": "1 to 7 days",
            }
        ],
        "checks": [{"host": "edge-01", "url": "https://pesanth.com", "age_seconds": age}],
        "containers": [{"host": "workstation-01", "running": 5, "total": 7}],
        "archive": [{"kind": "system", "days": 2}],
    }


def _config(tmp_path, **kwargs):
    return MirrorConfig(cache_path=tmp_path / "status.json", **kwargs)


def _upstream(tmp_path, fetch, now=NOW, **kwargs):
    return Upstream(_config(tmp_path, **kwargs), fetch=fetch, clock=lambda: now)


def test_ages_are_rebased_onto_the_moment_the_page_is_served():
    """A cached document served unchanged would show an hour-old host as fresh."""
    later = NOW + timedelta(hours=1)

    status = rebase(_document(age=20), later, 300.0, None)

    assert status["hosts"][0]["age_seconds"] == 3620
    assert status["hosts"][0]["freshness"] == "stale"
    assert status["checks"][0]["age_seconds"] == 3620


def test_a_fresh_document_is_left_alone():
    status = rebase(_document(age=20), NOW + timedelta(seconds=5), 300.0, None)

    assert status["hosts"][0]["age_seconds"] == 25
    assert status["hosts"][0]["freshness"] == "fresh"
    assert status["upstream"]["stale"] is False


def test_an_old_document_is_marked_stale_so_the_page_can_say_so():
    status = rebase(_document(), NOW + timedelta(minutes=30), 300.0, None)

    assert status["upstream"]["stale"] is True
    assert status["upstream"]["age_seconds"] == 1800


def test_a_current_document_with_a_failing_fetch_still_reads_stale():
    status = rebase(_document(), NOW, 300.0, "ConnectionError: refused")

    assert status["upstream"]["stale"] is True
    assert status["upstream"]["error"] == "ConnectionError: refused"


def test_the_last_good_document_survives_the_upstream_going_away(tmp_path):
    documents = [_document()]

    def fetch():
        if not documents:
            raise ConnectionError("refused")
        return documents.pop()

    upstream = _upstream(tmp_path, fetch)
    assert upstream.refresh() is True
    assert upstream.refresh() is False

    status = upstream.status()

    assert status["hosts"][0]["host"] == "workstation-01"
    assert status["upstream"]["error"] == "ConnectionError"


def test_a_failure_never_publishes_the_address_it_failed_to_reach(tmp_path):
    """A requests exception quotes the URL, which is the archive host's tailnet address."""

    def refuse():
        raise ConnectionError(
            "HTTPConnectionPool(host='100.125.224.124', port=8088): timed out"
        )

    upstream = _upstream(tmp_path, refuse)
    upstream.refresh()

    assert "100.125.224.124" not in json.dumps(upstream.status())


def test_the_cache_survives_a_restart_while_the_upstream_is_down(tmp_path):
    _upstream(tmp_path, lambda: _document()).refresh()

    def refuse():
        raise ConnectionError("refused")

    restarted = _upstream(tmp_path, refuse)
    restarted.refresh()

    assert restarted.status()["hosts"][0]["host"] == "workstation-01"


def test_a_first_start_with_nothing_cached_serves_an_empty_page_rather_than_failing(tmp_path):
    def refuse():
        raise ConnectionError("refused")

    upstream = _upstream(tmp_path, refuse)
    upstream.refresh()
    status = upstream.status()

    assert status["hosts"] == []
    assert status["upstream"]["stale"] is True


def test_a_corrupt_cache_file_is_ignored(tmp_path):
    (tmp_path / "status.json").write_text("{ not json", encoding="utf-8")

    upstream = _upstream(tmp_path, lambda: _document())

    assert upstream.status()["hosts"] == []
    assert upstream.refresh() is True
    assert upstream.status()["hosts"][0]["host"] == "workstation-01"


def test_the_mirror_serves_the_page_the_api_and_the_same_headers(tmp_path):
    upstream = _upstream(tmp_path, lambda: _document())
    upstream.refresh()
    client = create_mirror_app(upstream).test_client()

    page = client.get("/")
    api = client.get("/api/status")

    assert page.status_code == 200
    assert b"workstation-01" in page.data
    assert api.get_json()["hosts"][0]["host"] == "workstation-01"
    assert page.headers["X-Robots-Tag"] == "noindex, nofollow"
    assert client.get("/robots.txt").status_code == 200


def test_the_page_says_when_the_archive_host_is_unreachable(tmp_path):
    upstream = _upstream(tmp_path, lambda: _document(), now=NOW + timedelta(hours=2))
    upstream.refresh()

    page = create_mirror_app(upstream).test_client().get("/")

    assert b"not answering" in page.data


def test_the_mirror_stays_healthy_through_an_outage_it_exists_to_absorb(tmp_path):
    def refuse():
        raise ConnectionError("refused")

    upstream = _upstream(tmp_path, refuse)
    upstream.refresh()

    assert create_mirror_app(upstream).test_client().get("/healthz").get_json() == {"ok": True}


def test_the_mirror_holds_no_archive_of_its_own_beyond_the_last_document(tmp_path):
    """Whatever the upstream withheld stays withheld: nothing here re-derives it."""
    upstream = _upstream(tmp_path, lambda: _document())
    upstream.refresh()

    cached = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

    assert cached == _document()


def test_configuration_falls_back_to_working_defaults():
    config = MirrorConfig.from_env(
        {"SENTINEL_MIRROR_UPSTREAM": "http://10.0.0.1:8088/api/status",
         "SENTINEL_MIRROR_INTERVAL": "not-a-number",
         "SENTINEL_MIRROR_TIMEOUT": "0"}
    )

    assert config.upstream == "http://10.0.0.1:8088/api/status"
    assert config.interval == 20.0
    assert config.timeout == 5.0
