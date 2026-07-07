from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import os

import psycopg
from psycopg import Connection

from .astro import ConeRegion
from .events import EventSummary


@dataclass(frozen=True)
class RowEvent:
    """Database event row."""

    id: int
    external_id: str
    created_at: datetime


@dataclass(frozen=True)
class RowNotice:
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


@dataclass(frozen=True)
class RowConesearch:
    """Database cone-search row, including storage and job fields."""

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
    radius_arcsec: float
    alert_count: int
    result_uri: str | None
    created_at: datetime


@dataclass(frozen=True)
class Job:
    """Job metadata."""

    job_id: int
    event_id: int
    job_type: str
    subject_time_start: datetime
    subject_time_end: datetime
    scheduled_at: datetime
    run_after: datetime
    attempt_count: int
    max_attempts: int


def init_db_conn() -> Connection:
    """Create a database connection from environment variables.

    Returns:
        A psycopg database connection.
    """
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def get_event(cursor, external_id: str) -> RowEvent | None:
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
    return RowEvent(*row)


def get_event_by_id(cursor, event_id: int) -> RowEvent | None:
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
    return RowEvent(*row)


def list_events(
    cursor,
    *,
    tstart: datetime | None = None,
    tstop: datetime | None = None,
) -> list[RowEvent]:
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
    return [RowEvent(*row) for row in cursor.fetchall()]


def get_events_summary(
    cursor,
    *,
    tstart: datetime | None = None,
    tstop: datetime | None = None,
) -> list[EventSummary]:
    """Return API event summaries ordered by creation time.

    ``last_updated`` and ``conesearch_count`` follow timeline semantics:
    notices always count, while cone-searches count only when they returned
    alerts.

    Args:
        cursor: Database cursor.
        tstart: Inclusive created_at lower bound.
        tstop: Exclusive created_at upper bound.

    Returns:
        Event summaries in creation order, oldest first.
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
        WITH filtered_events AS (
            SELECT id, external_id, created_at
            FROM events
            {where_sql}
        ),
        milestones AS (
            SELECT
                notices.event_id,
                notices.published_at AS happened_at,
                'notice' AS milestone_type
            FROM notices
            JOIN filtered_events
                ON filtered_events.id = notices.event_id

            UNION ALL

            SELECT
                conesearches.event_id,
                conesearches.queried_at AS happened_at,
                'conesearch' AS milestone_type
            FROM conesearches
            JOIN filtered_events
                ON filtered_events.id = conesearches.event_id
            WHERE conesearches.alert_count > 0
        ),
        milestone_summary AS (
            SELECT
                event_id,
                max(happened_at) AS last_updated,
                count(*) FILTER (WHERE milestone_type = 'notice') AS notice_count,
                count(*) FILTER (WHERE milestone_type = 'conesearch') AS conesearch_count
            FROM milestones
            GROUP BY event_id
        )
        SELECT
            filtered_events.id,
            filtered_events.external_id,
            filtered_events.created_at,
            milestone_summary.last_updated,
            coalesce(milestone_summary.notice_count, 0) AS notice_count,
            coalesce(milestone_summary.conesearch_count, 0) AS conesearch_count,
            latest_notice.burst_datetime AS latest_burst_datetime,
            latest_localization.ra,
            latest_localization.dec,
            latest_localization.err_radius
        FROM filtered_events
        LEFT JOIN milestone_summary
            ON milestone_summary.event_id = filtered_events.id
        LEFT JOIN LATERAL (
            SELECT burst_datetime
            FROM notices
            WHERE notices.event_id = filtered_events.id
            ORDER BY published_at DESC, id DESC
            LIMIT 1
        ) AS latest_notice ON true
        LEFT JOIN LATERAL (
            SELECT ra, dec, err_radius
            FROM notices
            WHERE notices.event_id = filtered_events.id
                AND retracted_by IS NULL
                AND ra IS NOT NULL
                AND dec IS NOT NULL
                AND err_radius IS NOT NULL
            ORDER BY published_at DESC, id DESC
            LIMIT 1
        ) AS latest_localization ON true
        ORDER BY filtered_events.created_at ASC, filtered_events.id ASC
        """,
        params,
    )
    summaries = []
    for (
        event_id,
        external_id,
        created_at,
        last_updated,
        notice_count,
        conesearch_count,
        latest_burst_datetime,
        ra,
        dec,
        err_radius,
    ) in cursor.fetchall():
        latest_localization = None
        if ra is not None:
            latest_localization = ConeRegion(ra=ra, dec=dec, err_radius=err_radius)
        summaries.append(
            EventSummary(
                id=event_id,
                external_id=external_id,
                created_at=created_at,
                last_updated=last_updated,
                notice_count=notice_count,
                conesearch_count=conesearch_count,
                latest_burst_datetime=latest_burst_datetime,
                latest_localization=latest_localization,
            )
        )
    return summaries


def get_event_notices(cursor, event_id: int) -> list[RowNotice]:
    """Return notice rows for an event, ordered by publication time.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.

    Returns:
        Notice rows in chronological order.
    """
    cursor.execute(
        """
        SELECT
            id,
            event_id,
            format,
            topic,
            kafka_partition,
            kafka_offset,
            mission,
            instrument,
            published_at,
            burst_datetime,
            ra,
            dec,
            err_radius,
            raw_uri,
            is_retraction,
            retracted_by,
            created_at
        FROM notices
        WHERE event_id = %s
        ORDER BY published_at ASC, id ASC
        """,
        (event_id,),
    )
    return [RowNotice(*row) for row in cursor.fetchall()]


def get_notice(cursor, notice_id: int) -> RowNotice | None:
    """Return a notice row by primary key.

    Args:
        cursor: Database cursor.
        notice_id: Notice primary key.

    Returns:
        The notice row, or None when absent.
    """
    cursor.execute(
        """
        SELECT
            id,
            event_id,
            format,
            topic,
            kafka_partition,
            kafka_offset,
            mission,
            instrument,
            published_at,
            burst_datetime,
            ra,
            dec,
            err_radius,
            raw_uri,
            is_retraction,
            retracted_by,
            created_at
        FROM notices
        WHERE id = %s
        """,
        (notice_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return RowNotice(*row)


def get_event_conesearches(cursor, event_id: int) -> list[RowConesearch]:
    """Return cone-search rows for an event, ordered by subject time.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.

    Returns:
        Cone-search rows in chronological order.
    """
    cursor.execute(
        """
        SELECT
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
            radius_arcsec,
            alert_count,
            result_uri,
            created_at
        FROM conesearches
        WHERE event_id = %s
        ORDER BY subject_time_start ASC, id ASC
        """,
        (event_id,),
    )
    return [RowConesearch(*row) for row in cursor.fetchall()]


def get_conesearch(cursor, conesearch_id: int) -> RowConesearch | None:
    """Return a cone-search row by primary key.

    Args:
        cursor: Database cursor.
        conesearch_id: Cone-search primary key.

    Returns:
        The cone-search row, or None when absent.
    """
    cursor.execute(
        """
        SELECT
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
            radius_arcsec,
            alert_count,
            result_uri,
            created_at
        FROM conesearches
        WHERE id = %s
        """,
        (conesearch_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return RowConesearch(*row)


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
    radius_arcsec: float,
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
        radius_arcsec: Query radius in arcseconds.
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
            radius_arcsec,
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
            radius_arcsec,
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


def pick_job(
    cursor,
    worker_id: str,
) -> Job | None:
    """Claim the next runnable job.

    ``run_after`` controls queue eligibility. ``scheduled_at`` is the stable
    localization cutoff.

    Args:
        cursor: Database cursor.
        worker_id: Identifier for the worker claiming the job.

    Returns:
        The claimed job, or None if no job is available.
    """
    cursor.execute(
        """
        WITH selected_job AS (
            SELECT id
            FROM jobs
            WHERE status IN ('pending', 'failed')
                AND run_after <= now()
            ORDER BY run_after
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        ) UPDATE jobs AS job
        SET status = 'running',
            worker_id = %(worker_id)s,
            started_at = now(),
            lease_until = now() + interval '15 minutes',
            attempt_count = attempt_count + 1
        FROM selected_job
        WHERE job.id = selected_job.id
        RETURNING
            job.id,
            job.event_id,
            job.job_type,
            subject_time_start,
            subject_time_end,
            job.scheduled_at,
            job.run_after,
            job.attempt_count,
            job.max_attempts;
        """,
        {"worker_id": worker_id},
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return Job(*row)


def claim_expired_jobs(
    cursor,
    *,
    retry_delay: timedelta,
) -> int:
    """Reclaim jobs whose worker lease expired.

    Retryable jobs move ``run_after`` forward. ``scheduled_at`` is unchanged.

    Args:
        cursor: Database cursor.
        retry_delay: Delay before retryable expired jobs are eligible to run.

    Returns:
        Number of expired jobs reclaimed.
    """
    cursor.execute(
        """
        WITH expired_jobs AS (
            SELECT
                id,
                worker_id,
                lease_until,
                attempt_count,
                max_attempts
            FROM jobs
            WHERE status = 'running'
                AND lease_until IS NOT NULL
                AND lease_until <= now()
            ORDER BY lease_until, id
            FOR UPDATE SKIP LOCKED
        ) UPDATE jobs AS job
        SET status = CASE
                WHEN expired_jobs.attempt_count >= expired_jobs.max_attempts THEN 'dead'
                ELSE 'failed'
            END,
            run_after = CASE
                WHEN expired_jobs.attempt_count >= expired_jobs.max_attempts THEN job.run_after
                ELSE now() + %(retry_delay)s
            END,
            completed_at = CASE
                WHEN expired_jobs.attempt_count >= expired_jobs.max_attempts THEN now()
                ELSE NULL
            END,
            lease_until = NULL,
            last_error = 'Worker lease expired before completion; worker_id='
                || COALESCE(expired_jobs.worker_id, '<unknown>')
                || '; lease_until='
                || expired_jobs.lease_until::text
        FROM expired_jobs
        WHERE job.id = expired_jobs.id
        RETURNING job.id
        """,
        {"retry_delay": retry_delay},
    )
    return len(cursor.fetchall())


def find_best_localization(cursor, event_id: int, cutoff_at: datetime) -> ConeRegion | None:
    """Find the latest notice localization at or before ``cutoff_at``.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.
        cutoff_at: Publication-time cutoff for candidate localizations.

    Returns:
        A cone region if available, else None.
    """
    cursor.execute(
        """
        SELECT
            notices.ra,
            notices.dec,
            notices.err_radius
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
    return ConeRegion(*row)


def mark_job_succeeded(cursor, job_id: int):
    """Mark a job as succeeded.

    Args:
        cursor: Database cursor.
        job_id: Job primary key.
    """
    cursor.execute(
        """
        UPDATE jobs
        SET status = 'succeeded',
            completed_at = now(),
            lease_until = NULL,
            last_error = NULL
        WHERE id = %s
        """,
        (job_id,),
    )


def mark_job_dead(cursor, job_id: int, message: str):
    """Mark a job as permanently failed.

    Args:
        cursor: Database cursor.
        job_id: Job primary key.
        message: Failure message to store on the job.
    """
    cursor.execute(
        """
        UPDATE jobs
        SET status = 'dead',
            completed_at = now(),
            lease_until = NULL,
            last_error = %s
        WHERE id = %s
        """,
        (message, job_id),
    )


def mark_job_failed(
    cursor,
    job: Job,
    message: str,
    *,
    retry_delay: timedelta,
):
    """Mark a job as failed, or dead if attempts are exhausted.

    Retry delay changes only ``run_after``, not ``scheduled_at``.

    Args:
        cursor: Database cursor.
        job: Job metadata with current attempt counters.
        message: Failure message to store on the job.
        retry_delay: Delay before the job is eligible to run again.
    """
    if job.attempt_count >= job.max_attempts:
        cursor.execute(
            """
            UPDATE jobs
            SET status = 'dead',
                completed_at = now(),
                lease_until = NULL,
                last_error = %s
            WHERE id = %s
            """,
            (message, job.job_id),
        )
        return

    cursor.execute(
        """
        UPDATE jobs
        SET status = 'failed',
            run_after = now() + %s,
            lease_until = NULL,
            last_error = %s
        WHERE id = %s
        """,
        (retry_delay, message, job.job_id),
    )
