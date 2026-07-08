"""Database rows and SQL helpers for conesearches."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ConesearchRow:
    """Database cone-search row."""

    id: int
    event_id: int
    job_id: int
    broker: str
    survey: str
    subject_time_start: datetime
    subject_time_end: datetime
    queried_at: datetime
    ra: float
    dec: float
    radius: float
    alert_count: int
    result_uri: str | None
    created_at: datetime


_CONESEARCH_COLUMNS = """
    id,
    event_id,
    job_id,
    broker,
    survey,
    subject_time_start,
    subject_time_end,
    queried_at,
    ra,
    dec,
    radius,
    alert_count,
    result_uri,
    created_at
"""


def get_event_conesearches(cursor, event_id: int) -> list[ConesearchRow]:
    """Return cone-search rows for an event, ordered by subject time.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.

    Returns:
        Cone-search rows in chronological order.
    """
    cursor.execute(
        f"""
        SELECT
            {_CONESEARCH_COLUMNS}
        FROM conesearches
        WHERE event_id = %s
        ORDER BY subject_time_start ASC, id ASC
        """,
        (event_id,),
    )
    return [ConesearchRow(*row) for row in cursor.fetchall()]


def get_conesearch(cursor, conesearch_id: int) -> ConesearchRow | None:
    """Return a cone-search row by primary key.

    Args:
        cursor: Database cursor.
        conesearch_id: Cone-search primary key.

    Returns:
        The cone-search row, or None when absent.
    """
    cursor.execute(
        f"""
        SELECT
            {_CONESEARCH_COLUMNS}
        FROM conesearches
        WHERE id = %s
        """,
        (conesearch_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return ConesearchRow(*row)


def insert_conesearch(
    cursor,
    *,
    event_id: int,
    job_id: int,
    broker: str,
    survey: str,
    subject_time_start: datetime,
    subject_time_end: datetime,
    queried_at: datetime,
    ra: float,
    dec: float,
    radius: float,
    alert_count: int,
    result_uri: str | None,
) -> int:
    """Return a conesearch id, inserting the conesearch if needed.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.
        job_id: Job primary key that produced the query.
        broker: Broker queried.
        survey: Survey queried through the broker.
        subject_time_start: Inclusive lower bound of the query time window.
        subject_time_end: Exclusive upper bound of the query time window.
        queried_at: Time the query was recorded.
        ra: Right ascension queried in degrees.
        dec: Declination queried in degrees.
        radius: Query radius in degrees.
        alert_count: Number of returned alerts.
        result_uri: URI of the raw result file for non-empty results.

    Returns:
        Conesearch primary key.
    """
    cursor.execute(
        """
        INSERT INTO conesearches (
            event_id,
            job_id,
            broker,
            survey,
            subject_time_start,
            subject_time_end,
            queried_at,
            ra,
            dec,
            radius,
            alert_count,
            result_uri
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_id) DO NOTHING
        RETURNING id
        """,
        (
            event_id,
            job_id,
            broker,
            survey,
            subject_time_start,
            subject_time_end,
            queried_at,
            ra,
            dec,
            radius,
            alert_count,
            result_uri,
        ),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        SELECT id
        FROM conesearches
        WHERE job_id = %s
        """,
        (job_id,),
    )
    return cursor.fetchone()[0]
