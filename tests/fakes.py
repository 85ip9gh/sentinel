"""Test doubles. No Kafka, no network, no docker."""

from __future__ import annotations


class FakeError:
    def __init__(self, message: str = "broker unavailable") -> None:
        self.message = message

    def __str__(self) -> str:  # pragma: no cover - convenience only
        return self.message


class FakeProducer:
    """Mimics the asynchronous shape of confluent_kafka.Producer.

    Delivery callbacks fire on `flush`, never on `produce`, which is the detail
    the publisher has to get right.
    """

    def __init__(
        self,
        deliver: bool = True,
        raise_buffer_error: bool = False,
        stall: bool = False,
    ) -> None:
        self.deliver = deliver
        self.raise_buffer_error = raise_buffer_error
        # `stall` models the case that matters at shutdown: messages are still
        # in flight, so flush returns a non-zero count and no callback has run.
        self.stall = stall
        self.purged = False
        self.produced: list[tuple[str, bytes, bytes]] = []
        self._pending: list = []

    def produce(self, topic, value, key, on_delivery):
        if self.raise_buffer_error:
            raise BufferError("queue full")
        self.produced.append((topic, value, key))
        self._pending.append(on_delivery)

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        if self.stall:
            return len(self._pending)
        pending, self._pending = self._pending, []
        for callback in pending:
            callback(None if self.deliver else FakeError(), None)
        return 0

    def purge(self, in_flight=True):
        """Mirrors librdkafka: pending callbacks fire with an error."""
        self.purged = True
        self.stall = False
        self.deliver = False


class FakePublisher:
    """Records what the runner asks to publish."""

    def __init__(self, clean: bool = True) -> None:
        self.clean = clean
        self.sent: list[tuple[str, bytes, str]] = []
        self.cycles_begun = 0
        self.flushes = 0
        self.drained: list[str] = []
        self.delivered = 0
        self.failed = 0

    def begin_cycle(self):
        self.cycles_begun += 1

    def send(self, topic, key, payload, spool_on_failure=True):
        self.sent.append((topic, key, payload))
        self.delivered += 1

    def flush(self):
        self.flushes += 1
        return self.clean

    def drain_spool(self, topic, key):
        self.drained.append(topic)
        return 0
