"""Logging helpers. Library code fetches loggers, the host owns the handlers."""

from __future__ import annotations

import logging
import os
import sys

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Module logger. Under Airflow the task handler already captures and formats output."""
    return logging.getLogger(name)


def configure_logging(level: str | None = None) -> None:
    """Attach a stdout handler for standalone runs. A no-op once any handler exists."""
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root.addHandler(handler)
    root.setLevel(level or os.environ.get("CHEMBL_LOG_LEVEL", "INFO"))
