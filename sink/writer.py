"""Writing partition files to HDFS over WebHDFS.

WebHDFS rather than a native client, because the alternative is a JVM and
libhdfs inside every service that touches storage. The cost is that a write is
redirected from the NameNode to a DataNode by hostname, so the sink has to run
somewhere that can resolve DataNode names. Inside the compose network it can.
"""

from __future__ import annotations

import logging
from typing import Protocol

log = logging.getLogger(__name__)


class Writer(Protocol):
    def write(self, path: str, body: str) -> None: ...

    def listing(self, path: str) -> list[str]: ...

    def read(self, path: str) -> str: ...


class WebHdfsWriter:
    def __init__(self, url: str, user: str) -> None:
        from hdfs import InsecureClient

        self._client = InsecureClient(url, user=user)

    def write(self, path: str, body: str) -> None:
        # Files are written once under a unique name, so overwrite is off: a
        # collision would mean two sinks are running, and failing loudly is
        # better than one of them quietly winning.
        self._client.write(path, data=body, encoding="utf-8", overwrite=False)

    def listing(self, path: str) -> list[str]:
        try:
            return sorted(self._client.list(path))
        except Exception:  # noqa: BLE001 - a missing partition is not an error
            return []

    def read(self, path: str) -> str:
        with self._client.read(path, encoding="utf-8") as handle:
            return handle.read()

    def status(self, path: str = "/") -> dict:
        return self._client.status(path)


def body_for(payloads: list[str]) -> str:
    """One reading per line, which is what makes the file readable by anything."""
    return "\n".join(payload.rstrip("\n") for payload in payloads) + "\n"
