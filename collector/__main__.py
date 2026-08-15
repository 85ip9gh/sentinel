"""Collector entry point: `python -m collector`."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading

from .config import Config
from .publisher import Publisher
from .runner import Runner, default_tasks
from .spool import Spool


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="collector", description="Sentinel host collector")
    parser.add_argument(
        "--once", action="store_true", help="run a single collection cycle and exit"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print one round of readings as JSON and exit, without touching Kafka",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _dry_run(config: Config) -> int:
    for task in default_tasks(config):
        for item in task.collect():
            print(json.dumps({"kind": task.kind, "topic": task.topic, "data": item}, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = Config.from_env()
    if args.dry_run:
        return _dry_run(config)

    spool = Spool(config.spool_dir, config.spool_max_bytes)
    publisher = Publisher(config.bootstrap, spool, config.flush_timeout)
    runner = Runner(config, publisher)

    if args.once:
        produced = runner.cycle()
        publisher.close()
        logging.info(
            "one cycle: %d readings, %d delivered, %d failed",
            produced,
            publisher.delivered,
            publisher.failed,
        )
        return 0 if publisher.failed == 0 else 1

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        runner.run(stop)
    finally:
        publisher.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
