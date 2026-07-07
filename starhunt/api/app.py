"""HTTP API for querying Starhunt data."""

from collections.abc import Iterator
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import json
from pathlib import Path
from typing import Annotated
from urllib.parse import unquote
from urllib.parse import urlparse

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Path as ApiPath
from fastapi import Query
from psycopg import Connection

from ..db import get_conesearch
from ..db import get_event_by_id
from ..db import get_event_conesearches
from ..db import get_event_notices
from ..db import get_events_summary
from ..db import get_notice
from ..db import init_db_conn
from ..notices import parse_notice
from ..timeline import build_event_milestones
from ..utils import is_tz_aware
from .responses import EventSummary
from .responses import Milestone

app = FastAPI(title="Starhunt API")


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


@app.get(
    "/events",
    response_model=list[EventSummary],
    tags=["events"],
    summary="List events",
    description=(
        "Return event summaries sorted by creation time, oldest first.\n\n"
        "Use the optional UTC datetime bounds to restrict the event creation interval."
    ),
    response_description="Event summaries matching the requested creation-time interval.",
    responses={422: {"description": "Invalid datetime bound or inverted interval."}},
)
def events(
    tstart: Annotated[
        datetime | None,
        Query(description="Inclusive UTC lower bound for event creation time."),
    ] = None,
    tstop: Annotated[
        datetime | None,
        Query(description="Exclusive UTC upper bound for event creation time."),
    ] = None,
    db_conn: Connection = Depends(get_db_conn),
):
    """Return event summaries for the requested creation-time interval."""
    tstart_utc = _validate_utc_datetime(tstart, "tstart")
    tstop_utc = _validate_utc_datetime(tstop, "tstop")
    if tstart_utc is not None and tstop_utc is not None and tstart_utc > tstop_utc:
        raise HTTPException(status_code=422, detail="tstart must be before or equal to tstop")

    with db_conn.cursor() as cursor:
        return get_events_summary(cursor, tstart=tstart_utc, tstop=tstop_utc)


@app.get(
    "/timeline/{event_id}",
    response_model=list[Milestone],
    tags=["events"],
    summary="Get event timeline",
    description=(
        "Return notice and survey cone-search milestones for one event, ordered oldest first. "
        "Notice milestones use published_at; cone-search milestones use queried_at."
    ),
    response_description="Timeline milestones for the event.",
    responses={404: {"description": "Event not found."}},
)
def timeline(
    event_id: Annotated[int, ApiPath(description="Event primary key.")],
    db_conn: Connection = Depends(get_db_conn),
):
    """Return timeline milestones for one event."""
    with db_conn.cursor() as cursor:
        event = get_event_by_id(cursor, event_id=event_id)
        if event is None:
            raise HTTPException(status_code=404, detail="Event not found")
        notices = get_event_notices(cursor, event.id)
        conesearches = get_event_conesearches(cursor, event.id)
        return build_event_milestones(notices, conesearches)


@app.get(
    "/notice/{notice_id}",
    tags=["artifacts"],
    summary="Get notice payload",
    description="Return one notice with selected metadata and the parsed raw notice payload.",
    response_description="Notice metadata and parsed payload.",
    responses={
        404: {"description": "Notice not found."},
        500: {"description": "Notice payload file not found."},
    },
)
def notice(
    notice_id: Annotated[int, ApiPath(description="Notice primary key.")],
    db_conn: Connection = Depends(get_db_conn),
):
    """Return one parsed notice payload."""
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


@app.get(
    "/conesearch/{conesearch_id}",
    tags=["artifacts"],
    summary="Get cone-search result",
    description=(
        "Return one cone-search with selected metadata and the parsed JSON result payload. "
        "Zero-alert searches return an empty payload list."
    ),
    response_description="Cone-search metadata and parsed result payload.",
    responses={
        404: {"description": "Conesearch not found."},
        500: {"description": "Conesearch result file not found."},
    },
)
def conesearch(
    conesearch_id: Annotated[int, ApiPath(description="Cone-search primary key.")],
    db_conn: Connection = Depends(get_db_conn),
):
    """Return one parsed cone-search result."""
    with db_conn.cursor() as cursor:
        row = get_conesearch(cursor, conesearch_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Conesearch not found")

    payload = []
    if row.result_uri is not None:
        result_path = _file_uri_path(row.result_uri)
        if not result_path.exists():
            raise HTTPException(status_code=500, detail="Conesearch result file not found")
        payload = json.loads(result_path.read_bytes())

    return {
        "metadata": {
            "event_id": row.event_id,
            "job_id": row.job_id,
            "broker": row.broker,
            "survey": row.survey,
            "subject_time_start": row.subject_time_start,
            "subject_time_end": row.subject_time_end,
            "queried_at": row.queried_at,
            "ra": row.ra,
            "dec": row.dec,
            "radius_arcsec": row.radius_arcsec,
            "alert_count": row.alert_count,
        },
        "payload": payload,
    }
