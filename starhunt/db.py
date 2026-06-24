from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import os

import psycopg
from psycopg import Connection


@dataclass(frozen=True)
class EventInfo:
    """Information about an event lookup or insert.

    Attributes:
        event_id: Event primary key.
        is_new: Whether the event was inserted by the current operation.
    """

    event_id: int
    is_new: bool


@dataclass(frozen=True)
class Localization:
    """Sky localization coordinates for an event.

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
    """Database job selected for worker execution.

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


MilestoneID = int
ArtifactID = int


def init_db_conn() -> Connection:
    """Create DB connection from environment.

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


def get_or_create_event(cursor, external_id: str, mission: str, instrument: str) -> EventInfo:
    """Return an event id, inserting the event when absent.

    Args:
        cursor: Database cursor.
        external_id: Stable external event identifier.
        mission: Mission that detected the event.
        instrument: Instrument that detected the event.

    Returns:
        Event information including whether a new row was inserted.
    """
    cursor.execute(
        """
        INSERT INTO events (external_id, mission, instrument)
        VALUES (%s, %s, %s)
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
        """,
        (external_id, mission, instrument),
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


def get_or_create_milestone(
    cursor,
    event_id: int,
    external_id: str,
    subtype: str,
    published_at: datetime,
    subject_time_start: datetime,
    subject_time_end: datetime,
    milestone_type: str,
    ra: float | None = None,
    dec: float | None = None,
    err_radius: float | None = None,
) -> MilestoneID:
    """Return a milestone id, inserting the milestone when absent.

    Args:
        cursor: Database cursor.
        event_id: Event primary key for the milestone.
        external_id: Stable external milestone identifier.
        subtype: Milestone subtype.
        published_at: Time when the milestone was published.
        subject_time_start: Inclusive lower bound of the subject time window.
        subject_time_end: Exclusive upper bound of the subject time window.
        milestone_type: High-level milestone type.
        ra: Optional right ascension in degrees.
        dec: Optional declination in degrees.
        err_radius: Optional error radius in degrees.

    Returns:
        Milestone primary key.
    """
    cursor.execute(
        """
        INSERT INTO milestones (
            event_id,
            external_id,
            milestone_type,
            milestone_subtype,
            published_at,
            subject_time_start,
            subject_time_end,
            ra,
            dec,
            err_radius
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
        """,
        (
            event_id,
            external_id,
            milestone_type,
            subtype,
            published_at,
            subject_time_start,
            subject_time_end,
            ra,
            dec,
            err_radius,
        ),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        SELECT id FROM milestones WHERE external_id = %s
        """,
        (external_id,),
    )
    return cursor.fetchone()[0]


def insert_artifact(
    cursor,
    milestone_id: int,
    uri: str,
    artifact_type: str,
) -> ArtifactID:
    """Record an artifact URI for a milestone and return its id.

    Args:
        cursor: Database cursor.
        milestone_id: Milestone primary key for the artifact.
        uri: Artifact URI.
        artifact_type: Type of artifact being recorded.

    Returns:
        Artifact primary key.
    """
    cursor.execute(
        """
        INSERT INTO artifacts (milestone_id, artifact_type, uri)
        VALUES (%s, %s, %s)
        ON CONFLICT (milestone_id, artifact_type, uri)
        DO NOTHING
        RETURNING id
        """,
        (milestone_id, artifact_type, uri),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        SELECT id
        FROM artifacts
        WHERE milestone_id = %s
            AND artifact_type = %s
            AND uri = %s
        """,
        (milestone_id, artifact_type, uri),
    )
    return cursor.fetchone()[0]


def pick_job(
    cursor,
    worker_id: str,
) -> JobInfo | None:
    """Claim the next worker-eligible job.

    ``run_after`` controls queue eligibility and may move during retries.
    ``scheduled_at`` is returned unchanged for provenance/localization cutoff.

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
    """Reclaim running jobs whose worker lease has expired.

    Expired jobs are handled like ordinary execution failures. Retryable jobs
    move ``run_after`` forward; ``scheduled_at`` remains the stable science
    cutoff used by localization selection.

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
    """Find the latest localization published at or before ``cutoff_at``.

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
            ra,
            dec,
            err_radius
        FROM milestones
        WHERE milestones.event_id = %s
            AND milestones.published_at <= %s
            AND milestones.ra IS NOT NULL
            AND milestones.dec IS NOT NULL
            AND milestones.err_radius IS NOT NULL
        ORDER BY milestones.published_at DESC, milestones.id DESC
        LIMIT 1
        """,
        (event_id, cutoff_at),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return Localization(*row)


def mark_job_succeeded(cursor, job_id: int, artifact_id: int | None):
    """Mark a job as succeeded.

    Args:
        cursor: Database cursor.
        job_id: Job primary key.
        artifact_id: Optional artifact primary key produced by the job.
    """
    cursor.execute(
        """
        UPDATE jobs
        SET status = 'succeeded',
            completed_at = now(),
            lease_until = NULL,
            artifact_id = %s,
            last_error = NULL
        WHERE id = %s
        """,
        (artifact_id, job_id),
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

    Retry delay changes only ``run_after``. ``scheduled_at`` stays fixed as the
    localization cutoff for this job window.

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
