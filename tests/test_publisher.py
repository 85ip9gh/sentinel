from collector.publisher import Publisher
from collector.spool import Spool

from .fakes import FakeProducer

TOPIC = "sentinel.system"


def _publisher(tmp_path, **kwargs):
    spool = Spool(tmp_path, max_bytes=100_000)
    producer = FakeProducer(**kwargs)
    return Publisher("unused:9092", spool, producer=producer), spool, producer


def test_delivered_readings_are_not_spooled(tmp_path):
    publisher, spool, producer = _publisher(tmp_path)

    publisher.begin_cycle()
    publisher.send(TOPIC, b"host-a", '{"n":1}')
    assert publisher.flush() is True

    assert producer.produced[0][0] == TOPIC
    assert publisher.delivered == 1
    assert spool.pending(TOPIC) == 0


def test_a_failed_delivery_lands_on_disk(tmp_path):
    publisher, spool, _ = _publisher(tmp_path, deliver=False)

    publisher.begin_cycle()
    publisher.send(TOPIC, b"host-a", '{"n":1}')
    assert publisher.flush() is False

    assert publisher.failed == 1
    assert spool.pending(TOPIC) == 1


def test_a_full_local_queue_lands_on_disk(tmp_path):
    publisher, spool, _ = _publisher(tmp_path, raise_buffer_error=True)

    publisher.begin_cycle()
    publisher.send(TOPIC, b"host-a", '{"n":1}')

    assert publisher.failed == 1
    assert spool.pending(TOPIC) == 1


def test_the_spool_replays_once_the_broker_is_back(tmp_path):
    publisher, spool, producer = _publisher(tmp_path, deliver=False)

    publisher.begin_cycle()
    for i in range(3):
        publisher.send(TOPIC, b"host-a", f'{{"n":{i}}}')
    publisher.flush()
    assert spool.pending(TOPIC) == 3

    producer.deliver = True
    publisher.begin_cycle()
    replayed = publisher.drain_spool(TOPIC, b"host-a")
    publisher.flush()

    assert replayed == 3
    assert spool.pending(TOPIC) == 0
    assert [value for _, value, _ in producer.produced[-3:]] == [
        b'{"n":0}',
        b'{"n":1}',
        b'{"n":2}',
    ]


def test_a_replay_that_fails_again_stays_on_disk(tmp_path):
    """The claim-then-replay path must not delete readings it could not deliver."""
    publisher, spool, producer = _publisher(tmp_path, deliver=False)

    publisher.begin_cycle()
    publisher.send(TOPIC, b"host-a", '{"n":0}')
    publisher.flush()
    assert spool.pending(TOPIC) == 1

    publisher.begin_cycle()
    publisher.drain_spool(TOPIC, b"host-a")
    publisher.flush()

    assert spool.pending(TOPIC) == 1
    assert not spool.path_for(TOPIC).with_suffix(".inflight").exists()
