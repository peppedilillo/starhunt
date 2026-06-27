"""Render Docker Compose JSON logs for interactive use."""

LOGO = r"""
                                                      ..
                    .                 .               .   . 
      .           .               .               .. .  .  *
             *          .                   ..        .
                           .            .     . :  .   .    
            .                        .   .  .  .   .
                                        . .  *:. . .
            __              __  *    .        __   
     .-----|  |_.---.-.----|  |--.--.--.-----|  |_ 
     |__ --|   _|  _  |   _|     |  |  |     |   _|
     |_____|____|___._|__| |__|__|_____|__|__|____| ~p26 
    .                   :.  .           .
                 .   *    .    .
             .  .  .    ./|\
            .  .. :.    . |             .               .
     .   ... .            |
 .    :.  . .   *.   You are here.               .
   .  *.             
 . .    .               .             *.                         
                                
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
import json
import re
import sys
from typing import Any, TextIO

import click
from rich.console import Console
from rich.text import Text


COMPOSE_LINE = re.compile(r"^(?P<prefix>.*?\|\s?)(?P<payload>.*)$")
LEVELS = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "critical": 50,
}


class Duration(click.ParamType):
    name = "duration"

    def convert(self, value, param, ctx):
        match = re.fullmatch(r"([1-9]\d*)([mhd])", value)
        if match is None:
            self.fail("must be a positive integer followed by m, h, or d", param, ctx)

        amount = int(match.group(1))
        unit = match.group(2)
        return {
            "m": timedelta(minutes=amount),
            "h": timedelta(hours=amount),
            "d": timedelta(days=amount),
        }[unit]


DURATION = Duration()


def _context_items(context: Any) -> list[str]:
    if not isinstance(context, dict):
        return []

    items = []
    for key in (
        "topic",
        "partition",
        "offset",
        "job_id",
        "event_id",
        "worker_id",
        "artifact_id",
        "recovered_jobs",
        "error",
    ):
        value = context.get(key)
        if value is not None:
            items.append(f"{key}={value}")

    attempt = context.get("attempt_count")
    maximum = context.get("max_attempts")
    if attempt is not None and maximum is not None:
        items.append(f"attempt={attempt}/{maximum}")

    return items


def _exception_summary(exception: Any) -> str | None:
    if not isinstance(exception, dict):
        return None
    exception_type = exception.get("type")
    if exception_type:
        return str(exception_type)
    return None


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        return None
    return timestamp.astimezone(timezone.utc)


def render_line(
    line: str,
    *,
    minimum_level: str = "debug",
    cutoff: datetime | None = None,
) -> Text | None:
    """Convert one Docker Compose log line into styled terminal text."""
    line = line.rstrip("\r\n")
    match = COMPOSE_LINE.match(line)
    if match is None:
        if cutoff is None:
            return Text(line, style="dim")
        return None

    prefix = match.group("prefix")
    payload = match.group("payload")
    try:
        record = json.loads(payload)
    except json.JSONDecodeError:
        # our logs are json, we want to highlight them so we dim the rest
        if cutoff is None:
            output = Text(prefix)
            output.append(payload, style="dim")
            return output
        return None

    if not isinstance(record, dict):
        # our logs are json maps
        if cutoff is None:
            output = Text(prefix)
            output.append(payload, style="dim")
            return output
        return None

    timestamp = _timestamp(record.get("timestamp"))
    if cutoff is not None and (timestamp is None or timestamp < cutoff):
        return None

    level = str(record.get("level", "info")).lower()
    if LEVELS.get(level, LEVELS["info"]) < LEVELS[minimum_level]:
        return None
    message = str(record.get("message", ""))

    output = Text()
    output.append(prefix)

    content = Text()
    timestamp_text = record.get("timestamp")
    if isinstance(timestamp_text, str):
        content.append(timestamp_text, style="dim")
        content.append(" ")
    content.append(f"[{level.upper()}] ")
    content.append(message)

    context = _context_items(record.get("context"))
    if context:
        content.append("  ")
        content.append(" ".join(context), style="dim" if level == "info" else None)

    exception = _exception_summary(record.get("exception"))
    if exception:
        content.append("  ")
        content.append(exception)

    if level == "warning":
        content.stylize("yellow")
    elif level in {"error", "critical"}:
        content.stylize("red")

    output.append_text(content)
    return output


def pretty_print(
    source: TextIO,
    console: Console,
    *,
    minimum_level: str = "debug",
    cutoff: datetime | None = None,
) -> None:
    """Stream formatted log lines from source to a Rich console."""
    for line in source:
        rendered = render_line(
            line,
            minimum_level=minimum_level,
            cutoff=cutoff,
        )
        if rendered is not None:
            console.print(rendered)


@click.command()
@click.option(
    "--level",
    "minimum_level",
    type=click.Choice(list(LEVELS), case_sensitive=False),
    default="debug",
    show_default=True,
    help="Minimum structured log level to display.",
)
@click.option(
    "--last",
    type=DURATION,
    metavar="DURATION",
    help="Show structured logs from the last duration, e.g. 15m, 3h, or 1d.",
)
def main(minimum_level: str, last: timedelta | None) -> None:
    console = Console(stderr=False, markup=False, highlight=False)
    console.print(LOGO)
    cutoff = None
    if last is not None:
        cutoff = datetime.now(timezone.utc) - last
    try:
        pretty_print(
            sys.stdin,
            console,
            minimum_level=minimum_level.lower(),
            cutoff=cutoff,
        )
    except BrokenPipeError:
        pass


if __name__ == "__main__":
    main()
