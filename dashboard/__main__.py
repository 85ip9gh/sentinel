"""Dashboard entry point: `python -m dashboard`."""

from __future__ import annotations

import logging
import os
import sys

from sink.config import SinkConfig
from sink.writer import WebHdfsWriter

from .app import create_app
from .store import Archive


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    config = SinkConfig.from_env()
    archive = Archive(WebHdfsWriter(config.hdfs_url, config.hdfs_user), config.root)
    app = create_app(archive)

    host = os.environ.get("SENTINEL_DASHBOARD_HOST", "0.0.0.0")
    port = int(os.environ.get("SENTINEL_DASHBOARD_PORT", "8088"))

    from waitress import serve

    logging.info("dashboard on %s:%d reading %s", host, port, config.root)
    serve(app, host=host, port=port, threads=4)
    return 0


if __name__ == "__main__":
    sys.exit(main())
