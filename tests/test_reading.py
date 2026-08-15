import json
from datetime import datetime, timezone

from collector.config import SCHEMA_VERSION
from collector.reading import Reading


def _fixed_clock():
    return datetime(2026, 8, 15, 18, 30, 5, tzinfo=timezone.utc)


def test_serializes_with_schema_and_utc_timestamp():
    reading = Reading.now("g7-server", "server", "system", {"cpu": {"percent": 4.2}}, _fixed_clock)
    payload = json.loads(reading.to_json())

    assert payload["schema"] == SCHEMA_VERSION
    assert payload["host"] == "g7-server"
    assert payload["role"] == "server"
    assert payload["kind"] == "system"
    assert payload["ts"] == "2026-08-15T18:30:05Z"
    assert payload["data"]["cpu"]["percent"] == 4.2


def test_key_is_the_host_so_a_machine_keeps_its_own_order():
    reading = Reading.now("cubebox", "workstation", "system", {}, _fixed_clock)
    assert reading.key() == b"cubebox"


def test_json_is_stable_for_identical_readings():
    first = Reading.now("cubebox", "workstation", "http", {"b": 1, "a": 2}, _fixed_clock)
    second = Reading.now("cubebox", "workstation", "http", {"a": 2, "b": 1}, _fixed_clock)
    assert first.to_json() == second.to_json()
