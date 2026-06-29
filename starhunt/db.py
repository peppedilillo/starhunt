from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import os

import psycopg
from psycopg import Connection


@dataclass(frozen=True)
class EventInfo:
    """Result of an event lookup or insert.

    Attributes:
        event_id: Event primary key.
        is_new: Whether the event was inserted by the current operation.
    """

    event_id: int
    is_new: bool


@dataclass(frozen=True)
class Localization:
    """Sky position and error radius.

    Attributes:
        ra: Right ascension in degrees.
        dec: Declination in degrees.
        err_radius: Error radius in degrees.
    """

    ra: float
    dec: float
    err_radius: float


@dataclass(frozen=True)
class JobInfo:
    """Claimed job metadata.

    Attributes:
        job_id: Job primary key.
        event_id: Event primary key associated with the job.
        job_type: Type of work to perform.
        scheduled_at: Stable planned schedule time used as the localization cutoff.
        run_after: Mutable worker eligibility time used for retries.
        subject_time_start: Inclusive lower bound of the job subject window.
        subject_time_end: Exclusive upper bound of the job subject window.
        attempt_count: Number of attempts already started.
        max_attempts: Maximum number of attempts before the job is marked dead.
    """

    job_id: int
    event_id: int
    job_type: str
    scheduled_at: datetime
    run_after: datetime
    subject_time_start: datetime
    subject_time_end: datetime
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


def get_or_create_event(cursor, external_id: str) -> EventInfo:
    """Return an event id, inserting the event when absent.

    Args:
        cursor: Database cursor.
        external_id: Stable mission-qualified event id. The mission tag only
            disambiguates ids; mission metadata lives on notices.

    Returns:
        Event information including whether a new row was inserted.
    """
    cursor.execute(
        """
        INSERT INTO events (external_id)
        VALUES (%s)
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
        """,
        (external_id,),
    )
    row = cursor.fetchone()
    if row is not None:
        return EventInfo(event_id=row[0], is_new=True)

    cursor.execute(
        """
        SELECT id FROM events WHERE external_id = %s
        """,
        (external_id,),
    )
    return EventInfo(event_id=cursor.fetchone()[0], is_new=False)


def insert_notice(
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
    """Return a notice id, inserting the notice if needed.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.
        ivorn: Stable notice identifier.
        topic: Kafka topic carrying the notice.
        kafka_partition: Kafka partition that carried the notice.
        kafka_offset: Kafka offset that carried the notice.
        mission: Mission that produced the notice.
        instrument: Instrument that produced the notice.
        is_retraction: Whether the notice invalidates earlier cited notices.
        published_at: Time when the notice was published.
        burst_datetime: Event burst time reported by the notice.
        raw_uri: URI of the raw notice file.
        ra: Optional right ascension in degrees.
        dec: Optional declination in degrees.
        err_radius: Optional error radius in degrees.

    Returns:
        Notice primary key.
    """
    cursor.execute(
        """
        INSERT INTO notices (
            event_id,
            ivorn,
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
            ivorn,
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
        WHERE event_id = %s
            AND ivorn = ANY(%s)
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
) -> JobInfo | None:
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
            job.scheduled_at,
            job.run_after,
            subject_time_start,
            subject_time_end,
            job.attempt_count,
            job.max_attempts;
        """,
        {"worker_id": worker_id},
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return JobInfo(*row)


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


def find_best_localization(cursor, event_id: int, cutoff_at: datetime) -> Localization | None:
    """Find the latest notice localization at or before ``cutoff_at``.

    Args:
        cursor: Database cursor.
        event_id: Event primary key.
        cutoff_at: Publication-time cutoff for candidate localizations.

    Returns:
        A Localization if available, else None.
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
    return Localization(*row)


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
    job: JobInfo,
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
