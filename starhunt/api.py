"""HTTP API for querying Starhunt data."""

from collections.abc import Iterator
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from urllib.parse import unquote
from urllib.parse import urlparse

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from psycopg import Connection

from .db import get_event
from .db import get_event_conesearches
from .db import get_event_notices
from .db import get_notice
from .db import init_db_conn
from .db import list_events
from .db import RowEvent
from .notices import parse_notice
from .timeline import build_event_milestones
from .timeline import Milestone
from .utils import is_tz_aware

app = FastAPI()


def get_db_conn() -> Iterator[Connection]:
    """Yield a database connection for one request.

    Returns:
        A database connection that is closed after the request completes.
    """
    conn = init_db_conn()
    try:
        yield conn
    finally:
        conn.close()


def _validate_utc_datetime(value: datetime | None, name: str) -> datetime | None:
    """Validate and normalize an optional UTC datetime query parameter.

    Args:
        value: Parsed datetime value, or None when the query parameter is absent.
        name: Query parameter name used in validation errors.

    Returns:
        The value normalized to UTC, or None when absent.
    """
    if value is None:
        return None
    if not is_tz_aware(value):
        raise HTTPException(status_code=422, detail=f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise HTTPException(status_code=422, detail=f"{name} must use UTC")
    return value.astimezone(timezone.utc)


def _file_uri_path(uri: str) -> Path:
    """Return the local filesystem path for a stored file URI."""
    parsed = urlparse(uri)
    return Path(unquote(parsed.path))


@app.get("/events", response_model=list[RowEvent])
def events(
    tstart: datetime | None = None,
    tstop: datetime | None = None,
    db_conn: Connection = Depends(get_db_conn),
):
    """Return events sorted by creation time, newest first.

    Optional UTC datetime query parameters constrain the event ``created_at``
    interval before rows are loaded from the database.

    Args:
        tstart: Optional inclusive UTC lower bound for event creation time.
        tstop: Optional exclusive UTC upper bound for event creation time.
        db_conn: Database connection supplied by dependency injection.

    Returns:
        Event rows matching the requested creation-time interval.

    Raises:
        HTTPException: 422 when a datetime bound is naive, non-UTC, or the
            interval is inverted.
    """
    tstart_utc = _validate_utc_datetime(tstart, "tstart")
    tstop_utc = _validate_utc_datetime(tstop, "tstop")
    if tstart_utc is not None and tstop_utc is not None and tstart_utc > tstop_utc:
        raise HTTPException(status_code=422, detail="tstart must be before or equal to tstop")

    with db_conn.cursor() as cursor:
        return list_events(cursor, tstart=tstart_utc, tstop=tstop_utc)


@app.get("/timeline/{event_id}", response_model=list[Milestone])
def timeline(
    event_id: str,
    db_conn: Connection = Depends(get_db_conn),
):
    """Return milestones for an event external id.

    The event id is resolved to the local event row before loading notice and
    cone-search rows used to build the timeline.

    Args:
        event_id: Event external id.
        db_conn: Database connection supplied by dependency injection.

    Returns:
        Notice and cone-search milestones ordered oldest first.

    Raises:
        HTTPException: 404 when the event external id does not exist.
    """
    with db_conn.cursor() as cursor:
        event = get_event(cursor, external_id=event_id)
        if event is None:
            raise HTTPException(status_code=404, detail="Event not found")
        notices = get_event_notices(cursor, event.id)
        conesearches = get_event_conesearches(cursor, event.id)
        return build_event_milestones(notices, conesearches)


@app.get("/notice/{notice_id}")
def notice(
    notice_id: int,
    db_conn: Connection = Depends(get_db_conn),
):
    """Return one notice with metadata and parsed raw payload.

    The notice row supplies the response metadata, the raw payload URI, and the
    topic-specific parser used to decode the payload from disk.

    Args:
        notice_id: Notice primary key.
        db_conn: Database connection supplied by dependency injection.

    Returns:
        A mapping with ``metadata`` from the notice row and ``payload`` from
        parsing the stored raw notice file.

    Raises:
        HTTPException: 404 when the notice row does not exist.
        HTTPException: 500 when the stored raw payload file is missing.
    """
    with db_conn.cursor() as cursor:
        row = get_notice(cursor, notice_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Notice not found")

    payload_path = _file_uri_path(row.raw_uri)
    if not payload_path.exists():
        raise HTTPException(status_code=500, detail="Notice payload file not found")

    payload = parse_notice(payload_path.read_bytes(), row.topic)
    return {
        "metadata": {
            "event_id": row.event_id,
            "format": row.format,
            "topic": row.topic,
            "instrument": row.instrument,
            "mission": row.mission,
            "published_at": row.published_at,
            "is_retraction": row.is_retraction,
        },
        "payload": payload.model_dump(mode="json"),
    }
