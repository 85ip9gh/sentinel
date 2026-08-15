from datetime import datetime, timezone

import pytest

from sink.partition import UNDATED, event_date, file_name, kind_from_topic, partition_dir


@pytest.mark.parametrize(
    "topic,expected",
    [
        ("sentinel.system", "system"),
        ("sentinel.http", "http"),
        ("sentinel.container", "container"),
        ("plain", "plain"),
    ],
)
def test_kind_comes_from_the_topic_name(topic, expected):
    assert kind_from_topic(topic) == expected


def test_the_partition_is_the_reading_own_date_not_arrival():
    payload = '{"ts":"2026-08-14T23:59:59Z","host":"g7-server"}'
    assert event_date(payload) == "2026-08-14"


def test_a_non_utc_timestamp_is_converted_before_the_date_is_taken():
    # 00:30 on the 15th in Atlantic time is still the 14th in UTC.
    payload = '{"ts":"2026-08-15T00:30:00-03:00"}'
    assert event_date(payload) == "2026-08-15"

    payload = '{"ts":"2026-08-14T21:30:00-03:00"}'
    assert event_date(payload) == "2026-08-15"


@pytest.mark.parametrize(
    "payload",
    ['{"host":"a"}', "not json at all", '{"ts":12345}', '{"ts":"yesterday"}', ""],
)
def test_a_reading_that_cannot_be_placed_in_time_is_marked_not_dropped(payload):
    assert event_date(payload) == UNDATED


def test_partition_dir_is_hive_style():
    assert partition_dir("/sentinel/raw", "system", "2026-08-15") == (
        "/sentinel/raw/system/dt=2026-08-15"
    )


def test_partition_dir_tolerates_a_trailing_slash():
    assert partition_dir("/sentinel/raw/", "http", "2026-08-15") == (
        "/sentinel/raw/http/dt=2026-08-15"
    )


def test_file_names_sort_by_time_and_do_not_collide():
    clock = lambda: datetime(2026, 8, 15, 17, 4, 5, tzinfo=timezone.utc)  # noqa: E731
    first = file_name(clock, token="aaaaaaaa")
    second = file_name(clock, token="bbbbbbbb")

    assert first == "part-20260815T170405Z-aaaaaaaa.ndjson"
    assert first != second
    assert file_name(lambda: datetime(2026, 8, 15, 18, 0, 0, tzinfo=timezone.utc)) > first
