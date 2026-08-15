"""The sink loop: Kafka in, HDFS day partitions out.

Delivery is at-least-once and deliberately so. Offsets are committed only after
the partition files land, so a crash between the write and the commit replays
those readings and writes them again under a new file name. Duplicates are
removable later by (host, kind, ts); a gap in the archive is not.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Protocol

from .batch import Batch
from .config import SinkConfig
from .partition import UNDATED, event_date, file_name, kind_from_topic, partition_dir
from .writer import Writer, body_for

log = logging.getLogger(__name__)


class Consumer(Protocol):
    def poll(self, timeout: float) -> Any: ...

    def commit(self, asynchronous: bool = False) -> Any: ...

    def close(self) -> None: ...


class SinkRunner:
    def __init__(
        self,
        config: SinkConfig,
        consumer: Consumer,
        writer: Writer,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.consumer = consumer
        self.writer = writer
        self.batch = Batch(clock)
        self.files_written = 0
        self.records_written = 0
        self.undated = 0

    def handle(self, message: Any) -> None:
        payload = message.value().decode("utf-8")
        date = event_date(payload)
        if date == UNDATED:
            self.undated += 1
            log.warning("reading with no usable timestamp on %s", message.topic())
        self.batch.add(kind_from_topic(message.topic()), date, payload)

    def flush(self) -> int:
        """Write every pending group, then commit. Returns files written."""
        if self.batch.empty:
            return 0

        groups = self.batch.drain()
        written = 0
        for kind, date, payloads in groups:
            path = f"{partition_dir(self.config.root, kind, date)}/{file_name()}"
            self.writer.write(path, body_for(payloads))
            written += 1
            self.records_written += len(payloads)
            log.info("wrote %d readings to %s", len(payloads), path)

        # Only now. An offset committed before the write would turn a crash into
        # a hole in the archive.
        self.consumer.commit(asynchronous=False)
        self.files_written += written
        return written

    def tick(self) -> None:
        message = self.consumer.poll(self.config.poll_timeout)
        if message is not None:
            if message.error():
                log.error("consumer error: %s", message.error())
            else:
                self.handle(message)

        if self.batch.is_full(self.config.batch_max_records, self.config.batch_max_seconds):
            self.flush()

    def run(self, stop: threading.Event) -> None:
        log.info(
            "sink started topics=%s root=%s hdfs=%s",
            ",".join(self.config.topics),
            self.config.root,
            self.config.hdfs_url,
        )
        try:
            while not stop.is_set():
                self.tick()
        finally:
            # Whatever is pending belongs in the archive, not in memory.
            self.flush()
            self.consumer.close()
            log.info(
                "sink stopped after %d files, %d readings, %d undated",
                self.files_written,
                self.records_written,
                self.undated,
            )
