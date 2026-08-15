from sink.batch import Batch


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


def test_a_new_batch_is_empty_and_never_full():
    batch = Batch(Clock())
    assert batch.empty
    assert not batch.is_full(max_records=1, max_seconds=0.0)


def test_readings_are_grouped_by_kind_and_day():
    batch = Batch(Clock())
    batch.add("system", "2026-08-15", "a")
    batch.add("system", "2026-08-15", "b")
    batch.add("system", "2026-08-14", "c")
    batch.add("http", "2026-08-15", "d")

    assert batch.count == 4
    assert batch.drain() == [
        ("http", "2026-08-15", ["d"]),
        ("system", "2026-08-14", ["c"]),
        ("system", "2026-08-15", ["a", "b"]),
    ]


def test_the_record_limit_closes_the_batch():
    batch = Batch(Clock())
    batch.add("system", "2026-08-15", "a")
    assert not batch.is_full(max_records=2, max_seconds=999)
    batch.add("system", "2026-08-15", "b")
    assert batch.is_full(max_records=2, max_seconds=999)


def test_the_time_limit_closes_a_quiet_batch():
    clock = Clock()
    batch = Batch(clock)
    batch.add("http", "2026-08-15", "a")

    assert not batch.is_full(max_records=999, max_seconds=60)
    clock.advance(60)
    assert batch.is_full(max_records=999, max_seconds=60)


def test_draining_resets_the_count_and_the_clock():
    clock = Clock()
    batch = Batch(clock)
    batch.add("system", "2026-08-15", "a")
    clock.advance(100)
    batch.drain()

    assert batch.empty
    assert batch.age() == 0
    assert batch.drain() == []
