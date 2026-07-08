"""Database rows and SQL helpers for events."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class EventRow:
    """Database event row."""

    id: int
    external_id: str
    created_at: datetime


def get_event(cursor, external_id: str) -> EventRow | None:
    """Return an event by external id.

    Args:
        cursor: Database cursor.
        external_id: Stable mission-qualified event id. The mission tag only
            disambiguates ids; mission metadata lives on notices.

    Returns:
        The event row, or None when absent.
    """
    cursor.execute(
        """
        SELECT id, external_id, created_at
        FROM events
        WHERE external_id = %s
        """,
        (external_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return EventRow(*row)


def get_event_by_id(cursor, event_id: int) -> EventRow | None:
    """Return an event by primary key.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.

    Returns:
        The event row, or None when absent.
    """
    cursor.execute(
        """
        SELECT id, external_id, created_at
        FROM events
        WHERE id = %s
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return EventRow(*row)


def list_events(
    cursor,
    *,
    tstart: datetime | None = None,
    tstop: datetime | None = None,
) -> list[EventRow]:
    """Return events ordered by creation time.

    Args:
        cursor: Database cursor.
        tstart: Inclusive created_at lower bound.
        tstop: Exclusive created_at upper bound.

    Returns:
        Event rows in creation order.
    """
    where_clauses = []
    params = {}
    if tstart is not None:
        where_clauses.append("created_at >= %(tstart)s")
        params["tstart"] = tstart
    if tstop is not None:
        where_clauses.append("created_at < %(tstop)s")
        params["tstop"] = tstop
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    cursor.execute(
        f"""
        SELECT id, external_id, created_at
        FROM events
        {where_sql}
        ORDER BY created_at ASC, id ASC
        """,
        params,
    )
    return [EventRow(*row) for row in cursor.fetchall()]


def insert_event(cursor, external_id: str) -> int:
    """Insert an event and return its primary key.

    Args:
        cursor: Database cursor.
        external_id: Stable mission-qualified event id. The mission tag only
            disambiguates ids; mission metadata lives on notices.

    Returns:
        Event primary key.
    """

    cursor.execute(
        """
        INSERT INTO events (external_id)
        VALUES (%s)
        RETURNING id
        """,
        (external_id,),
    )
    return cursor.fetchone()[0]
