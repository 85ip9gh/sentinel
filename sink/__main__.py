"""Sink entry point: `python -m sink`."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time

from .config import SinkConfig
from .runner import SinkRunner
from .writer import WebHdfsWriter


def _build_consumer(config: SinkConfig):
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap,
            "group.id": config.group_id,
            "auto.offset.reset": "earliest",
            # Offsets are committed by hand, after the data is durable.
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
        }
    )
    consumer.subscribe(list(config.topics))
    return consumer


def _wait_for_hdfs(writer: WebHdfsWriter, attempts: int = 60, delay: float = 5.0) -> None:
    """The NameNode leaves safe mode a while after the container reports up."""
    for attempt in range(1, attempts + 1):
        try:
            writer.status("/")
            return
        except Exception as exc:  # noqa: BLE001 - retry is the whole point
            logging.info("waiting for HDFS (%d/%d): %s", attempt, attempts, exc)
            time.sleep(delay)
    raise SystemExit("HDFS did not become available")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sink", description="Sentinel Kafka to HDFS sink")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = SinkConfig.from_env()
    writer = WebHdfsWriter(config.hdfs_url, config.hdfs_user)
    _wait_for_hdfs(writer)

    runner = SinkRunner(config, _build_consumer(config), writer)

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    runner.run(stop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
