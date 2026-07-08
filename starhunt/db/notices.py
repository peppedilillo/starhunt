"""Database rows and SQL helpers for notices."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class NoticeRow:
    """Database notice row, including storage and Kafka fields."""

    id: int
    event_id: int
    format: str
    topic: str
    kafka_partition: int
    kafka_offset: int
    mission: str
    instrument: str
    published_at: datetime
    burst_datetime: datetime
    ra: float | None
    dec: float | None
    err_radius: float | None
    raw_uri: str
    is_retraction: bool
    retracted_by: int | None
    created_at: datetime


_NOTICE_COLUMNS = """
    notices.id,
    notices.event_id,
    notices.format,
    notices.topic,
    notices.kafka_partition,
    notices.kafka_offset,
    notices.mission,
    notices.instrument,
    notices.published_at,
    notices.burst_datetime,
    notices.ra,
    notices.dec,
    notices.err_radius,
    notices.raw_uri,
    notices.is_retraction,
    notices.retracted_by,
    notices.created_at
"""


def get_event_notices(cursor, event_id: int) -> list[NoticeRow]:
    """Return notice rows for an event, ordered by publication time.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.

    Returns:
        Notice rows in chronological order.
    """
    cursor.execute(
        f"""
        SELECT
            {_NOTICE_COLUMNS}
        FROM notices
        WHERE event_id = %s
        ORDER BY published_at ASC, id ASC
        """,
        (event_id,),
    )
    return [NoticeRow(*row) for row in cursor.fetchall()]


def get_notice(cursor, notice_id: int) -> NoticeRow | None:
    """Return a notice row by primary key.

    Args:
        cursor: Database cursor.
        notice_id: Notice primary key.

    Returns:
        The notice row, or None when absent.
    """
    cursor.execute(
        f"""
        SELECT
            {_NOTICE_COLUMNS}
        FROM notices
        WHERE id = %s
        """,
        (notice_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return NoticeRow(*row)


def _insert_notice(
    cursor,
    *,
    event_id: int,
    notice_format: str,
    topic: str,
    kafka_partition: int,
    kafka_offset: int,
    mission: str,
    instrument: str,
    is_retraction: bool,
    published_at: datetime,
    burst_datetime: datetime,
    raw_uri: str,
    ra: float | None = None,
    dec: float | None = None,
    err_radius: float | None = None,
) -> int:
    """Return a common notice id, inserting the notice envelope if needed."""
    cursor.execute(
        """
        INSERT INTO notices (
            event_id,
            format,
            topic,
            kafka_partition,
            kafka_offset,
            mission,
            instrument,
            is_retraction,
            published_at,
            burst_datetime,
            ra,
            dec,
            err_radius,
            raw_uri
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
        RETURNING id
        """,
        (
            event_id,
            notice_format,
            topic,
            kafka_partition,
            kafka_offset,
            mission,
            instrument,
            is_retraction,
            published_at,
            burst_datetime,
            ra,
            dec,
            err_radius,
            raw_uri,
        ),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        SELECT id
        FROM notices
        WHERE topic = %s
            AND kafka_partition = %s
            AND kafka_offset = %s
        """,
        (topic, kafka_partition, kafka_offset),
    )
    return cursor.fetchone()[0]


def insert_notice_voevent(
    cursor,
    *,
    event_id: int,
    ivorn: str,
    topic: str,
    kafka_partition: int,
    kafka_offset: int,
    mission: str,
    instrument: str,
    is_retraction: bool,
    published_at: datetime,
    burst_datetime: datetime,
    raw_uri: str,
    ra: float | None = None,
    dec: float | None = None,
    err_radius: float | None = None,
) -> int:
    """Return a VOEvent notice id, inserting the notice if needed."""
    notice_id = _insert_notice(
        cursor,
        event_id=event_id,
        notice_format="voevent",
        topic=topic,
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
        mission=mission,
        instrument=instrument,
        is_retraction=is_retraction,
        published_at=published_at,
        burst_datetime=burst_datetime,
        raw_uri=raw_uri,
        ra=ra,
        dec=dec,
        err_radius=err_radius,
    )
    cursor.execute(
        """
        INSERT INTO notice_voevents (notice_id, ivorn)
        VALUES (%s, %s)
        ON CONFLICT (notice_id) DO NOTHING
        """,
        (notice_id, ivorn),
    )
    return notice_id


def insert_notice_json(
    cursor,
    *,
    event_id: int,
    topic: str,
    kafka_partition: int,
    kafka_offset: int,
    mission: str,
    instrument: str,
    is_retraction: bool,
    published_at: datetime,
    burst_datetime: datetime,
    raw_uri: str,
    ra: float | None = None,
    dec: float | None = None,
    err_radius: float | None = None,
) -> int:
    """Return a JSON notice id, inserting the notice if needed."""
    return _insert_notice(
        cursor,
        event_id=event_id,
        notice_format="json",
        topic=topic,
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
        mission=mission,
        instrument=instrument,
        is_retraction=is_retraction,
        published_at=published_at,
        burst_datetime=burst_datetime,
        raw_uri=raw_uri,
        ra=ra,
        dec=dec,
        err_radius=err_radius,
    )


def mark_retracted_notices(
    cursor,
    *,
    event_id: int,
    retraction_notice_id: int,
    target_ivorns: tuple[str, ...],
) -> int:
    """Mark local notices cited by a retraction notice as retracted."""
    if not target_ivorns:
        return 0

    cursor.execute(
        """
        SELECT is_retraction
        FROM notices
        WHERE id = %s
            AND event_id = %s
        """,
        (retraction_notice_id, event_id),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Unknown retraction notice id: {retraction_notice_id}")
    if row[0] is not True:
        raise ValueError("retraction_notice_id must reference a retraction notice")

    cursor.execute(
        """
        UPDATE notices
        SET retracted_by = %s
        FROM notice_voevents
        WHERE event_id = %s
            AND notice_voevents.notice_id = notices.id
            AND notice_voevents.ivorn = ANY(%s)
            -- retraction-of-retraction is unsupported until examples require it.
            AND NOT is_retraction
        RETURNING id
        """,
        (retraction_notice_id, event_id, list(target_ivorns)),
    )
    return len(cursor.fetchall())


def find_best_localized_notice(cursor, event_id: int, cutoff_at: datetime) -> NoticeRow | None:
    """Find the latest localized notice at or before ``cutoff_at``.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.
        cutoff_at: Publication-time cutoff for candidate localizations.

    Returns:
        A localized notice row if available, else None.
    """
    cursor.execute(
        f"""
        SELECT
            {_NOTICE_COLUMNS}
        FROM notices
        LEFT JOIN notices AS retractor
            ON retractor.id = notices.retracted_by
        WHERE notices.event_id = %s
            AND notices.published_at <= %s
            AND (
                notices.retracted_by IS NULL
                OR retractor.published_at > %s
            )
            AND notices.ra IS NOT NULL
            AND notices.dec IS NOT NULL
            AND notices.err_radius IS NOT NULL
        ORDER BY notices.published_at DESC, notices.id DESC
        LIMIT 1
        """,
        (event_id, cutoff_at, cutoff_at),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return NoticeRow(*row)
