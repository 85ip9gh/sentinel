import json

import pytest

from sink.config import SinkConfig
from sink.runner import SinkRunner

from .fakes import FakeConsumer, FakeMessage, FakeWriter


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


def _config(**overrides):
    env = {"SENTINEL_SINK_BATCH_RECORDS": "2", "SENTINEL_SINK_BATCH_SECONDS": "60"}
    env.update(overrides)
    return SinkConfig.from_env(env)


def _reading(host="g7-server", ts="2026-08-15T17:00:00Z", kind="system"):
    return json.dumps({"schema": 1, "host": host, "role": "server", "kind": kind, "ts": ts, "data": {}})


def test_a_full_batch_is_written_to_its_day_partition():
    consumer = FakeConsumer(
        [
            FakeMessage("sentinel.system", _reading(ts="2026-08-15T17:00:00Z")),
            FakeMessage("sentinel.system", _reading(ts="2026-08-15T17:00:10Z")),
        ]
    )
    writer = FakeWriter()
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()
    assert writer.files == {}
    runner.tick()

    (path,) = writer.files
    assert path.startswith("/sentinel/raw/system/dt=2026-08-15/part-")
    assert path.endswith(".ndjson")
    assert len(writer.files[path].strip().splitlines()) == 2


def test_one_batch_spanning_two_days_writes_two_files():
    """Routine at midnight, and whenever a spooled backlog replays."""
    consumer = FakeConsumer(
        [
            FakeMessage("sentinel.system", _reading(ts="2026-08-14T23:59:55Z")),
            FakeMessage("sentinel.system", _reading(ts="2026-08-15T00:00:05Z")),
        ]
    )
    writer = FakeWriter()
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()
    runner.tick()

    days = sorted(path.split("/dt=")[1].split("/")[0] for path in writer.files)
    assert days == ["2026-08-14", "2026-08-15"]


def test_each_topic_writes_under_its_own_kind():
    consumer = FakeConsumer(
        [
            FakeMessage("sentinel.system", _reading()),
            FakeMessage("sentinel.http", _reading(kind="http")),
        ]
    )
    writer = FakeWriter()
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()
    runner.tick()

    kinds = sorted(path.split("/")[3] for path in writer.files)
    assert kinds == ["http", "system"]


def test_offsets_are_committed_only_after_the_write():
    consumer = FakeConsumer([FakeMessage("sentinel.system", _reading())] * 2)
    writer = FakeWriter()
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()
    assert consumer.commits == 0
    runner.tick()
    assert consumer.commits == 1


def test_a_failed_write_leaves_the_offsets_uncommitted():
    """A commit here would turn a crash into a hole in the archive."""
    consumer = FakeConsumer([FakeMessage("sentinel.system", _reading())] * 2)
    writer = FakeWriter(fail_on="/system/")
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()
    with pytest.raises(OSError):
        runner.tick()

    assert consumer.commits == 0
    assert writer.files == {}


def test_a_reading_with_no_usable_timestamp_is_filed_not_dropped():
    consumer = FakeConsumer(
        [
            FakeMessage("sentinel.system", '{"host":"g7-server"}'),
            FakeMessage("sentinel.system", _reading()),
        ]
    )
    writer = FakeWriter()
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()
    runner.tick()

    assert runner.undated == 1
    assert any("dt=unknown" in path for path in writer.files)
    assert any("dt=2026-08-15" in path for path in writer.files)


def test_an_empty_poll_writes_nothing_and_commits_nothing():
    consumer = FakeConsumer([])
    writer = FakeWriter()
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()

    assert writer.files == {}
    assert consumer.commits == 0
    assert runner.flush() == 0


def test_a_quiet_topic_still_flushes_on_the_time_limit():
    clock = Clock()
    consumer = FakeConsumer([FakeMessage("sentinel.http", _reading(kind="http"))])
    writer = FakeWriter()
    runner = SinkRunner(_config(SENTINEL_SINK_BATCH_RECORDS="500"), consumer, writer, clock)

    runner.tick()
    assert writer.files == {}

    clock.advance(60)
    runner.tick()
    assert len(writer.files) == 1


def test_a_consumer_error_is_logged_and_skipped():
    consumer = FakeConsumer([FakeMessage("sentinel.system", _reading(), err="broker down")])
    writer = FakeWriter()
    runner = SinkRunner(_config(), consumer, writer, Clock())

    runner.tick()

    assert runner.batch.empty
    assert writer.files == {}
