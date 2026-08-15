"""Kafka producer wrapper that falls back to the disk spool on delivery failure.

`produce` is asynchronous, so a message is not delivered when the call returns.
Success or failure arrives later on the delivery callback, and that callback is
the only honest place to decide whether a reading needs spooling.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from .spool import Spool

log = logging.getLogger(__name__)


class ProducerLike(Protocol):
    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def build_producer(bootstrap: str) -> ProducerLike:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "sentinel-collector",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            # Fail fast rather than blocking the collection loop. A message that
            # cannot be delivered inside this window belongs on disk.
            "message.timeout.ms": 20000,
            "socket.keepalive.enable": True,
        }
    )


class Publisher:
    def __init__(
        self,
        bootstrap: str,
        spool: Spool,
        flush_timeout: float = 5.0,
        producer: ProducerLike | None = None,
    ) -> None:
        self._producer = producer if producer is not None else build_producer(bootstrap)
        self._spool = spool
        self._flush_timeout = flush_timeout
        self.delivered = 0
        self.failed = 0
        self._cycle_failures = 0

    def send(self, topic: str, key: bytes, payload: str, spool_on_failure: bool = True) -> None:
        def on_delivery(err: Any, _msg: Any) -> None:
            if err is None:
                self.delivered += 1
                return
            self._cycle_failures += 1
            self.failed += 1
            if spool_on_failure:
                self._spool.append(topic, payload)

        try:
            self._producer.produce(
                topic, value=payload.encode("utf-8"), key=key, on_delivery=on_delivery
            )
        except BufferError:
            # The local queue is full, which means the broker has been unhealthy
            # for a while. Straight to disk.
            self._cycle_failures += 1
            self.failed += 1
            if spool_on_failure:
                self._spool.append(topic, payload)
        self._producer.poll(0)

    def drain_spool(self, topic: str, key: bytes) -> int:
        """Replay one topic's spool. Only worth calling when the last cycle was clean.

        Handing a payload to `produce` is not delivery, so every replayed reading
        counts as sent here and a later failure re-spools it through the normal
        delivery callback. `Spool.drain` claims the file first, so those
        re-spooled readings survive rather than being deleted underneath us.
        They land at the back of the queue, which trades strict ordering for not
        losing data. HDFS partitions by the reading's own timestamp from step 2,
        so arrival order does not decide where a reading ends up.
        """

        def send(payload: str) -> bool:
            self.send(topic, key, payload)
            return True

        return self._spool.drain(topic, send)

    def begin_cycle(self) -> None:
        """Reset the per-cycle failure count. Call once before producing."""
        self._cycle_failures = 0

    def flush(self) -> bool:
        """Block until outstanding messages resolve. Returns True if all delivered.

        Delivery callbacks fire inside `flush`, so the failure count is only
        meaningful after it returns.
        """
        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            log.warning("%d messages still queued after flush", remaining)
        return remaining == 0 and self._cycle_failures == 0

    def close(self) -> None:
        """Flush, then force every still-undelivered reading onto the spool.

        `flush` returning a non-zero count means messages are still in flight,
        and their delivery callbacks have not run. Exiting there would drop them
        silently, which is the exact hole the spool exists to close. `purge`
        fires those callbacks with an error, so the normal failure path writes
        them to disk instead.
        """
        remaining = self._producer.flush(self._flush_timeout)
        if not remaining:
            return

        purge = getattr(self._producer, "purge", None)
        if purge is None:
            log.error("%d readings dropped: producer cannot be purged", remaining)
            return
        purge(in_flight=True)
        self._producer.flush(self._flush_timeout)

    @property
    def healthy(self) -> bool:
        return self._cycle_failures == 0
