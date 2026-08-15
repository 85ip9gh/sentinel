"""The collection loop.

Each source runs on its own interval: host metrics are cheap and frequent, HTTP
checks cost a round trip and are rarer, container state sits in between. The
loop is one thread, because at these intervals concurrency would buy nothing and
cost the ability to reason about ordering.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .config import TOPICS, Config
from .publisher import Publisher
from .reading import Reading
from .sources import containers, http_check, system

log = logging.getLogger(__name__)

Collect = Callable[[], list[dict[str, Any]]]


@dataclass
class Task:
    """One source, its topic, and when it is next due."""

    kind: str
    interval: float
    collect: Collect
    next_due: float = 0.0

    @property
    def topic(self) -> str:
        return TOPICS[self.kind]


def default_tasks(config: Config) -> list[Task]:
    tasks = [
        Task("system", config.system_interval, lambda: [system.collect()]),
        Task("container", config.container_interval, lambda: containers.collect()),
    ]
    if config.http_targets:
        tasks.append(
            Task(
                "http",
                config.http_interval,
                lambda: http_check.check_all(config.http_targets, config.http_timeout),
            )
        )
    return tasks


class Runner:
    def __init__(
        self,
        config: Config,
        publisher: Publisher,
        tasks: list[Task] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.publisher = publisher
        self.tasks = tasks if tasks is not None else default_tasks(config)
        self.clock = clock
        self.cycles = 0
        self.readings_sent = 0

    def _reading(self, kind: str, data: dict[str, Any]) -> Reading:
        return Reading.now(self.config.host, self.config.role, kind, data)

    def cycle(self) -> int:
        """Run every due task once. Returns the number of readings produced."""
        now = self.clock()
        self.publisher.begin_cycle()
        produced = 0

        for task in self.tasks:
            if now < task.next_due:
                continue
            task.next_due = now + task.interval
            try:
                items = task.collect()
            except Exception:  # noqa: BLE001 - one bad source must not stop the loop
                log.exception("source %s failed", task.kind)
                continue
            for item in items:
                reading = self._reading(task.kind, item)
                self.publisher.send(task.topic, reading.key(), reading.to_json())
                produced += 1

        delivered_clean = self.publisher.flush()
        self.cycles += 1
        self.readings_sent += produced

        if delivered_clean:
            self._drain_spools()
        return produced

    def _drain_spools(self) -> None:
        key = self.config.host.encode("utf-8")
        for topic in TOPICS.values():
            replayed = self.publisher.drain_spool(topic, key)
            if replayed:
                log.info("replayed %d spooled readings to %s", replayed, topic)
                self.publisher.flush()

    def sleep_seconds(self) -> float:
        now = self.clock()
        due = min((task.next_due for task in self.tasks), default=now + 1.0)
        return max(0.0, due - now)

    def run(self, stop: threading.Event) -> None:
        log.info(
            "collector started host=%s role=%s bootstrap=%s",
            self.config.host,
            self.config.role,
            self.config.bootstrap,
        )
        while not stop.is_set():
            self.cycle()
            stop.wait(min(self.sleep_seconds(), 5.0))
        log.info(
            "collector stopped after %d cycles, %d readings, %d delivered, %d failed",
            self.cycles,
            self.readings_sent,
            self.publisher.delivered,
            self.publisher.failed,
        )
