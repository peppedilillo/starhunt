from contextlib import nullcontext
from datetime import datetime
from datetime import timezone
import json
import logging
import math
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest

from starhunt import consumer
from starhunt import worker
from starhunt.db import RowJob
from starhunt.logging import configure_logging
from starhunt.logging import JsonFormatter


def make_record(
    message="A message",
    args=(),
    *,
    extra=None,
    exc_info=None,
):
    record = logging.LogRecord(
        name="starhunt.worker",
        level=logging.INFO,
        pathname="/app/starhunt/worker.py",
        lineno=42,
        msg=message,
        args=args,
        exc_info=exc_info,
        func="run_job",
    )
    for key, value in (extra or {}).items():
        setattr(record, key, value)
    record.created = 1782304200.123
    return record


def format_record(record):
    return JsonFormatter().format(record)


def test_json_formatter_emits_core_fields_and_source():
    output = format_record(make_record("Job %s completed", (17,)))
    data = json.loads(output)

    assert data == {
        "timestamp": "2026-06-24T12:30:00.123Z",
        "level": "info",
        "logger": "starhunt.worker",
        "message": "Job 17 completed",
        "source": {
            "module": "worker",
            "function": "run_job",
            "line": 42,
        },
    }
    assert output.count("\n") == 0
    assert ": " not in output


def test_json_formatter_uses_utc_rfc3339_timestamp():
    data = json.loads(format_record(make_record()))
    timestamp = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))

    assert timestamp.utcoffset().total_seconds() == 0
    assert data["timestamp"].endswith("Z")


def test_json_formatter_nests_extra_fields_as_context():
    data = json.loads(
        format_record(
            make_record(
                extra={
                    "job_id": 17,
                    "details": {"attempt": 2},
                }
            )
        )
    )

    assert data["context"] == {
        "job_id": 17,
        "details": {"attempt": 2},
    }
    assert "pathname" not in data["context"]
    assert "created" not in data["context"]


def test_json_formatter_includes_exception_details():
    try:
        raise ValueError("invalid job")
    except ValueError:
        record = make_record("Job failed", exc_info=__import__("sys").exc_info())

    data = json.loads(format_record(record))

    assert data["exception"]["type"] == "ValueError"
    assert data["exception"]["message"] == "invalid job"
    assert "ValueError: invalid job" in data["exception"]["stacktrace"]


def test_json_formatter_preserves_unicode():
    output = format_record(make_record("Transient α"))

    assert "Transient α" in output
    assert json.loads(output)["message"] == "Transient α"


def test_json_formatter_safely_converts_unsupported_values():
    recursive = []
    recursive.append(recursive)
    data = json.loads(
        format_record(
            make_record(
                extra={
                    "path": object(),
                    "values": (1, math.nan, math.inf),
                    "recursive": recursive,
                }
            )
        )
    )

    assert isinstance(data["context"]["path"], str)
    assert data["context"]["values"] == [1, "nan", "inf"]
    assert data["context"]["recursive"] == ["<recursive>"]


@pytest.fixture
def isolated_logging():
    logger = logging.getLogger("starhunt")
    original_handlers = logger.handlers[:]
    original_level = logger.level
    original_propagate = logger.propagate
    original_excepthook = sys.excepthook
    logger.handlers.clear()
    yield
    logger.handlers[:] = original_handlers
    logger.setLevel(original_level)
    logger.propagate = original_propagate
    sys.excepthook = original_excepthook


def test_configure_logging_emits_json(isolated_logging, monkeypatch, capsys):
    monkeypatch.setenv("STARHUNT_LOG_LEVEL", "INFO")

    configure_logging("worker")
    logging.getLogger("starhunt.worker").info("Job ready", extra={"job_id": 17})

    records = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert records[0]["message"] == "Service started"
    assert records[0]["logger"] == "starhunt.worker"
    assert "context" not in records[0]
    assert records[1]["message"] == "Job ready"
    assert records[1]["context"] == {"job_id": 17}


def test_configure_logging_honors_level_and_is_idempotent(isolated_logging, monkeypatch, capsys):
    monkeypatch.setenv("STARHUNT_LOG_LEVEL", "WARNING")

    configure_logging("worker")
    configure_logging("worker")
    logging.getLogger("starhunt.worker").info("Hidden")
    logging.getLogger("starhunt.worker").warning("Visible")

    records = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert [record["message"] for record in records] == ["Visible"]


def test_configure_logging_rejects_invalid_level(isolated_logging, monkeypatch):
    monkeypatch.setenv("STARHUNT_LOG_LEVEL", "LOUD")

    with pytest.raises(ValueError, match="Invalid STARHUNT_LOG_LEVEL: LOUD"):
        configure_logging("worker")


def test_configure_logging_formats_uncaught_exceptions(isolated_logging, capsys):
    configure_logging("consumer")
    capsys.readouterr()

    try:
        raise RuntimeError("consumer stopped")
    except RuntimeError:
        sys.excepthook(*sys.exc_info())

    record = json.loads(capsys.readouterr().err)
    assert record["level"] == "critical"
    assert record["message"] == "Uncaught exception"
    assert record["logger"] == "starhunt.consumer"
    assert "context" not in record
    assert record["exception"]["type"] == "RuntimeError"


class FakeConnection:
    def cursor(self):
        return nullcontext(object())

    def commit(self):
        pass

    def rollback(self):
        pass


class StopConsumer(Exception):
    pass


class FakeMessage:
    def __init__(self, error=None):
        self._error = error

    def error(self):
        return self._error

    def topic(self):
        return "gcn.test"

    def partition(self):
        return 2

    def offset(self):
        return 41


class FakeConsumer:
    def __init__(self):
        self.calls = 0

    def subscribe(self, topics):
        pass

    def consume(self, timeout):
        self.calls += 1
        if self.calls == 1:
            return [FakeMessage(error="broker error"), FakeMessage()]
        raise StopConsumer

    def commit(self, **kwargs):
        pass

    def close(self):
        pass


def make_job(*, attempt_count=1, max_attempts=3, job_type=consumer.ZTF_CONESEARCH_JOB_TYPE):
    now = datetime.now(timezone.utc)
    return RowJob(
        job_id=17,
        event_id=23,
        job_type=job_type,
        scheduled_at=now,
        run_after=now,
        subject_time_start=now,
        subject_time_end=now,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
    )


def test_consumer_logs_message_errors_and_commits(monkeypatch, tmp_path):
    logger = Mock()
    monkeypatch.setattr(consumer, "logger", logger)
    monkeypatch.setattr(consumer, "init_consumer", lambda *args: FakeConsumer())
    monkeypatch.setattr(consumer, "init_db_conn", FakeConnection)
    monkeypatch.setattr(consumer, "write_message", lambda **kwargs: Path(tmp_path / "notice.xml"))
    monkeypatch.setattr(consumer, "insert_message", lambda **kwargs: None)

    with pytest.raises(StopConsumer):
        consumer.main(tmp_path)

    assert logger.warning.call_args.args == ("Kafka message error",)
    assert logger.warning.call_args.kwargs["extra"]["error"] == "broker error"
    assert logger.info.call_args.args == ("Kafka message committed",)
    assert logger.info.call_args.kwargs["extra"]["offset"] == 41


def test_worker_logs_success(monkeypatch):
    logger = Mock()
    monkeypatch.setattr(worker, "logger", logger)
    monkeypatch.setattr(worker, "execute_ztf_fink_conesearch", lambda *args, **kwargs: 31)
    monkeypatch.setattr(worker, "mark_job_succeeded", lambda *args, **kwargs: None)

    worker.run_job(
        db_conn=FakeConnection(),
        job=make_job(),
        outdir=None,
        retry_delay=None,
        timeout=None,
    )

    assert logger.info.call_args_list[0].args == ("Job started",)
    assert logger.info.call_args_list[1].args == ("Job succeeded",)
    assert logger.info.call_args_list[1].kwargs["extra"]["conesearch_id"] == 31


@pytest.mark.parametrize(
    ("attempt_count", "expected_method", "expected_status"),
    [
        (1, "warning", "failed"),
        (3, "error", "dead"),
    ],
)
def test_worker_logs_retryable_and_terminal_failures(
    monkeypatch,
    attempt_count,
    expected_method,
    expected_status,
):
    logger = Mock()
    monkeypatch.setattr(worker, "logger", logger)
    monkeypatch.setattr(
        worker,
        "execute_ztf_fink_conesearch",
        Mock(side_effect=RuntimeError("query failed")),
    )
    monkeypatch.setattr(worker, "mark_job_failed", lambda *args, **kwargs: None)

    worker.run_job(
        db_conn=FakeConnection(),
        job=make_job(attempt_count=attempt_count),
        outdir=None,
        retry_delay=None,
        timeout=None,
    )

    call = getattr(logger, expected_method).call_args
    assert call.args == ("Job failed",)
    assert call.kwargs["extra"]["status"] == expected_status
    assert call.kwargs["exc_info"] is True


def test_worker_logs_unsupported_job_as_dead(monkeypatch):
    logger = Mock()
    monkeypatch.setattr(worker, "logger", logger)
    monkeypatch.setattr(worker, "mark_job_dead", lambda *args, **kwargs: None)

    worker.run_job(
        db_conn=FakeConnection(),
        job=make_job(job_type="unsupported"),
        outdir=None,
        retry_delay=None,
        timeout=None,
    )

    assert logger.error.call_args.args == ("Unsupported job type",)
    assert logger.error.call_args.kwargs["extra"]["status"] == "dead"


def test_worker_logs_recovered_leases(monkeypatch):
    logger = Mock()
    monkeypatch.setattr(worker, "logger", logger)
    monkeypatch.setattr(worker, "claim_expired_jobs", lambda *args, **kwargs: 2)
    monkeypatch.setattr(worker, "pick_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "sleep", Mock(side_effect=RuntimeError("stop")))

    with pytest.raises(RuntimeError, match="stop"):
        worker.run_worker(
            db_conn=FakeConnection(),
            outdir=None,
            worker_id="worker-1",
            poll_interval=0,
            retry_delay=None,
            timeout=None,
        )

    assert logger.warning.call_args.args == ("Expired worker leases recovered",)
    assert logger.warning.call_args.kwargs["extra"] == {
        "worker_id": "worker-1",
        "recovered_jobs": 2,
    }
