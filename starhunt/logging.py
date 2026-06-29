"""Logging utilities."""

from collections.abc import Mapping
from datetime import datetime
from datetime import timezone
import json
import logging
import math
import os
import sys
from typing import Any

# extra log fields are stored under `context`.
_STANDARD_RECORD_FIELDS = frozenset(
    logging.LogRecord(
        name="",
        level=0,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    ).__dict__
) | {"asctime", "message"}


def _safe_string(value: object) -> str:
    try:
        return str(value)
    except Exception:
        return f"<{type(value).__name__}>"


def _json_safe(value: Any, seen: set[int] | None = None) -> Any:
    """Convert a value to data accepted by `json.dumps()`, recursively."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else _safe_string(value)

    if seen is None:
        seen = set()

    if isinstance(value, Mapping):
        value_id = id(value)
        if value_id in seen:
            return "<recursive>"
        seen.add(value_id)
        try:
            return {_safe_string(key): _json_safe(item, seen) for key, item in value.items()}
        finally:
            seen.remove(value_id)

    if isinstance(value, (list, tuple)):
        value_id = id(value)
        if value_id in seen:
            return "<recursive>"
        seen.add(value_id)
        try:
            return [_json_safe(item, seen) for item in value]
        finally:
            seen.remove(value_id)

    return _safe_string(value)


class JsonFormatter(logging.Formatter):
    """Format each log record as one compact JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, timezone.utc)
        payload: dict[str, Any] = {
            "timestamp": timestamp.isoformat(timespec="milliseconds").replace(
                "+00:00",
                "Z",
            ),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
            "source": {
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            },
        }

        context = {
            key: _json_safe(value) for key, value in record.__dict__.items() if key not in _STANDARD_RECORD_FIELDS
        }
        if context:
            payload["context"] = context

        if record.exc_info is not None:
            exception = record.exc_info[1]
            payload["exception"] = {
                "type": type(exception).__name__,
                "message": _safe_string(exception),
                "stacktrace": self.formatException(record.exc_info),
            }

        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )


def _log_level() -> int:
    value = os.getenv("STARHUNT_LOG_LEVEL", "INFO").upper()
    level = logging.getLevelNamesMapping().get(value)
    if not isinstance(level, int):
        raise ValueError(f"Invalid STARHUNT_LOG_LEVEL: {value}")
    return level


def configure_logging(service: str) -> None:
    """Configure JSON logging for Starhunt modules."""
    logger = logging.getLogger("starhunt")
    logger.setLevel(_log_level())
    logger.propagate = False

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    # keep setup idempotent for CLI entrypoints.
    # comes with a side-effect: potential user-defined handlers are purged here.
    # this was deemed not a problem as we do not expect downstream users but still.
    logger.handlers.clear()
    logger.addHandler(handler)

    def log_uncaught_exception(exception_type, exception, traceback):
        if issubclass(exception_type, KeyboardInterrupt):
            sys.__excepthook__(exception_type, exception, traceback)
            return
        logging.getLogger(f"starhunt.{service}").critical(
            "Uncaught exception",
            exc_info=(exception_type, exception, traceback),
        )

    sys.excepthook = log_uncaught_exception
    logging.getLogger(f"starhunt.{service}").info("Service started")
