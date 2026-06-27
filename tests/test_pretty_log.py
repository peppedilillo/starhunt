from datetime import timedelta
from datetime import datetime
from datetime import timezone
from io import StringIO
import json

from click.testing import CliRunner
from rich.console import Console

from scripts import pretty
from scripts.pretty import DURATION
from scripts.pretty import pretty_print
from scripts.pretty import render_line


def compose_line(service, record):
    return f"{service}  | {json.dumps(record)}\n"


def record(**overrides):
    value = {
        "timestamp": "2026-06-24T12:30:00.123Z",
        "level": "info",
        "logger": "starhunt.worker",
        "message": "Job started",
    }
    value.update(overrides)
    return value


def styles_for(text):
    return [(span.style, text.plain[span.start : span.end]) for span in text.spans]


def test_preserves_docker_prefix_exactly():
    line = compose_line("worker-2   ", record()).rstrip()
    rendered = render_line(line)

    assert rendered.plain.startswith("worker-2     | ")
    assert rendered.plain.split("|", 1)[0] == line.split("|", 1)[0]
    assert not any("worker-2" in styled for _, styled in styles_for(rendered))


def test_keeps_exact_json_timestamp():
    rendered = render_line(compose_line("consumer-1", record()).rstrip())

    assert "2026-06-24T12:30:00.123Z" in rendered.plain


def test_renders_selected_context_in_stable_order():
    rendered = render_line(
        compose_line(
            "worker-1",
            record(
                context={
                    "max_attempts": 3,
                    "artifact_path": "/data/private/result.json",
                    "event_id": 23,
                    "attempt_count": 2,
                    "job_id": 17,
                    "artifact_id": 31,
                    "unknown": "omitted",
                }
            ),
        ).rstrip()
    )

    assert "job_id=17 event_id=23 artifact_id=31 attempt=2/3" in rendered.plain
    assert "artifact_path" not in rendered.plain
    assert "unknown" not in rendered.plain


def test_colors_warning_message_and_context_yellow():
    rendered = render_line(
        compose_line(
            "consumer-1",
            record(
                level="warning",
                message="Kafka message error",
                context={"topic": "gcn.test", "error": "broker unavailable"},
            ),
        ).rstrip()
    )

    assert "[WARNING] Kafka message error" in rendered.plain
    assert rendered.plain.startswith("consumer-1  | ")
    assert ("yellow", "2026-06-24T12:30:00.123Z [WARNING] Kafka message error  topic=gcn.test error=broker unavailable") in styles_for(
        rendered
    )


def test_shows_explicit_debug_level():
    rendered = render_line(
        compose_line(
            "worker-1",
            record(level="debug", message="Polling jobs"),
        ).rstrip()
    )

    assert "[DEBUG] Polling jobs" in rendered.plain


def test_colors_error_and_shows_exception_type_only():
    rendered = render_line(
        compose_line(
            "worker-1",
            record(
                level="error",
                message="Job failed",
                context={"job_id": 17},
                exception={
                    "type": "RuntimeError",
                    "message": "query failed",
                    "stacktrace": "long traceback",
                },
            ),
        ).rstrip()
    )

    assert "[ERROR] Job failed  job_id=17  RuntimeError" in rendered.plain
    assert "query failed" not in rendered.plain
    assert "long traceback" not in rendered.plain
    assert ("red", "2026-06-24T12:30:00.123Z [ERROR] Job failed  job_id=17  RuntimeError") in styles_for(rendered)


def test_dims_non_json_payload_without_styling_docker_prefix():
    postgres = render_line("postgres-1  | checkpoint complete")
    unprefixed = render_line("plain startup failure")

    assert postgres.plain == "postgres-1  | checkpoint complete"
    assert unprefixed.plain == "plain startup failure"
    assert ("dim", "checkpoint complete") in styles_for(postgres)
    assert not any("postgres-1  |" in styled for _, styled in styles_for(postgres))
    assert unprefixed.spans == []


def test_streams_lines_in_input_order_without_terminal_colors():
    source = StringIO(
        compose_line("consumer-1", record(message="First"))
        + compose_line("worker-1", record(message="Second"))
    )
    output = StringIO()
    console = Console(
        file=output,
        force_terminal=False,
        color_system=None,
        width=200,
        markup=False,
        highlight=False,
    )

    pretty_print(source, console)

    lines = output.getvalue().splitlines()
    assert "First" in lines[0]
    assert "Second" in lines[1]
    assert "\x1b[" not in output.getvalue()


def test_filters_structured_records_below_minimum_level():
    source = StringIO(
        compose_line("worker-1", record(level="debug", message="Debug"))
        + compose_line("worker-1", record(level="info", message="Info"))
        + compose_line("worker-1", record(level="warning", message="Warning"))
        + compose_line("worker-1", record(level="error", message="Error"))
    )
    output = StringIO()
    console = Console(
        file=output,
        force_terminal=False,
        color_system=None,
        width=200,
        markup=False,
        highlight=False,
    )

    pretty_print(source, console, minimum_level="warning")

    assert "Debug" not in output.getvalue()
    assert "Info" not in output.getvalue()
    assert "Warning" in output.getvalue()
    assert "Error" in output.getvalue()


def test_level_filter_keeps_non_json_lines():
    rendered = render_line(
        "postgres-1  | checkpoint complete",
        minimum_level="critical",
    )

    assert rendered.plain == "postgres-1  | checkpoint complete"


def test_cli_rejects_invalid_level():
    result = CliRunner().invoke(pretty.main, ["--level", "verbose"])

    assert result.exit_code != 0
    assert "Invalid value for '--level'" in result.output


def test_last_filter_includes_cutoff_and_newer_records():
    cutoff = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)

    at_cutoff = render_line(
        compose_line(
            "worker-1",
            record(timestamp="2026-06-24T12:00:00Z", message="At cutoff"),
        ),
        cutoff=cutoff,
    )
    newer = render_line(
        compose_line(
            "worker-1",
            record(timestamp="2026-06-24T12:00:01Z", message="Newer"),
        ),
        cutoff=cutoff,
    )

    assert "At cutoff" in at_cutoff.plain
    assert "Newer" in newer.plain


def test_last_filter_excludes_older_records():
    cutoff = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)

    rendered = render_line(
        compose_line(
            "worker-1",
            record(timestamp="2026-06-24T11:59:59.999Z"),
        ),
        cutoff=cutoff,
    )

    assert rendered is None


def test_last_filter_hides_unstructured_and_invalid_timestamps():
    cutoff = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)

    assert render_line("postgres-1  | checkpoint complete", cutoff=cutoff) is None
    assert render_line("plain startup failure", cutoff=cutoff) is None
    assert render_line(
        compose_line("worker-1", record(timestamp="not-a-date")),
        cutoff=cutoff,
    ) is None


def test_last_and_level_filters_are_combined():
    cutoff = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)
    source = StringIO(
        compose_line(
            "worker-1",
            record(
                timestamp="2026-06-24T11:59:59Z",
                level="error",
                message="Old error",
            ),
        )
        + compose_line(
            "worker-1",
            record(
                timestamp="2026-06-24T12:00:01Z",
                level="info",
                message="Recent info",
            ),
        )
        + compose_line(
            "worker-1",
            record(
                timestamp="2026-06-24T12:00:02Z",
                level="error",
                message="Recent error",
            ),
        )
    )
    output = StringIO()
    console = Console(
        file=output,
        force_terminal=False,
        color_system=None,
        width=200,
        markup=False,
        highlight=False,
    )

    pretty_print(
        source,
        console,
        minimum_level="error",
        cutoff=cutoff,
    )

    assert "Old error" not in output.getvalue()
    assert "Recent info" not in output.getvalue()
    assert "Recent error" in output.getvalue()


def test_duration_parses_minutes_hours_and_days():
    assert DURATION.convert("15m", None, None) == timedelta(minutes=15)
    assert DURATION.convert("3h", None, None) == timedelta(hours=3)
    assert DURATION.convert("1d", None, None) == timedelta(days=1)


def test_cli_rejects_invalid_last_duration():
    for value in ("3", "0h", "-1h", "1.5h", "1H", "1w", " 1h", "1h "):
        result = CliRunner().invoke(pretty.main, ["--last", value])

        assert result.exit_code != 0
        assert "must be a positive integer followed by m, h, or d" in result.output


def test_cli_help_describes_duration_format():
    result = CliRunner().invoke(pretty.main, ["--help"])

    assert result.exit_code == 0
    assert "--last DURATION" in result.output
    assert "15m, 3h, or 1d" in result.output
