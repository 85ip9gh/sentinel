import json

from collector.config import Config
from collector.runner import Runner, Task

from .fakes import FakePublisher


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


def _config(**overrides):
    env = {"SENTINEL_HOST": "cubebox", "SENTINEL_ROLE": "workstation"}
    env.update(overrides)
    return Config.from_env(env)


def test_a_cycle_publishes_one_message_per_item():
    publisher = FakePublisher()
    tasks = [Task("system", 10.0, lambda: [{"cpu": 1}]), Task("http", 60.0, lambda: [{"a": 1}, {"b": 2}])]
    runner = Runner(_config(), publisher, tasks, Clock())

    produced = runner.cycle()

    assert produced == 3
    assert [topic for topic, _, _ in publisher.sent] == [
        "sentinel.system",
        "sentinel.http",
        "sentinel.http",
    ]
    assert publisher.cycles_begun == 1
    assert publisher.flushes == 1


def test_readings_carry_the_host_identity():
    publisher = FakePublisher()
    runner = Runner(_config(), publisher, [Task("system", 10.0, lambda: [{"cpu": 1}])], Clock())

    runner.cycle()
    _, key, payload = publisher.sent[0]

    assert key == b"cubebox"
    assert json.loads(payload)["role"] == "workstation"


def test_a_task_is_skipped_until_its_interval_elapses():
    clock = Clock()
    publisher = FakePublisher()
    tasks = [Task("system", 10.0, lambda: [{"cpu": 1}]), Task("http", 60.0, lambda: [{"a": 1}])]
    runner = Runner(_config(), publisher, tasks, clock)

    runner.cycle()
    clock.advance(10.0)
    runner.cycle()

    kinds = [topic for topic, _, _ in publisher.sent]
    assert kinds.count("sentinel.system") == 2
    assert kinds.count("sentinel.http") == 1


def test_one_failing_source_does_not_stop_the_others():
    def boom():
        raise RuntimeError("sensor unplugged")

    publisher = FakePublisher()
    tasks = [Task("system", 10.0, boom), Task("container", 30.0, lambda: [{"id": "abc"}])]
    runner = Runner(_config(), publisher, tasks, Clock())

    produced = runner.cycle()

    assert produced == 1
    assert publisher.sent[0][0] == "sentinel.container"


def test_the_spool_is_only_drained_after_a_clean_flush():
    tasks = [Task("system", 10.0, lambda: [{"cpu": 1}])]

    clean = FakePublisher(clean=True)
    Runner(_config(), clean, tasks, Clock()).cycle()
    assert clean.drained == ["sentinel.system", "sentinel.http", "sentinel.container"]

    dirty = FakePublisher(clean=False)
    Runner(_config(), dirty, list(tasks), Clock()).cycle()
    assert dirty.drained == []


def test_sleep_is_bounded_by_the_soonest_task():
    clock = Clock()
    publisher = FakePublisher()
    tasks = [Task("system", 10.0, lambda: [{"cpu": 1}]), Task("http", 60.0, lambda: [{"a": 1}])]
    runner = Runner(_config(), publisher, tasks, clock)

    runner.cycle()
    clock.advance(4.0)

    assert runner.sleep_seconds() == 6.0
