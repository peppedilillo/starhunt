"""Database rows and SQL helpers for jobs."""

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Any


@dataclass(frozen=True)
class JobRow:
    """Database job row."""

    id: int
    event_id: int
    job_type: str
    subject_time_start: datetime
    subject_time_end: datetime
    scheduled_at: datetime
    run_after: datetime
    status: str
    attempt_count: int
    max_attempts: int
    worker_id: str | None
    lease_until: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    payload: dict[str, Any]
    last_error: str | None
    created_at: datetime


_JOB_COLUMNS = """
    job.id,
    job.event_id,
    job.job_type,
    job.subject_time_start,
    job.subject_time_end,
    job.scheduled_at,
    job.run_after,
    job.status,
    job.attempt_count,
    job.max_attempts,
    job.worker_id,
    job.lease_until,
    job.started_at,
    job.completed_at,
    job.payload,
    job.last_error,
    job.created_at
"""


def pick_job(
    cursor,
    worker_id: str,
) -> JobRow | None:
    """Claim the next runnable job.

    ``run_after`` controls queue eligibility. ``scheduled_at`` is the stable
    localization cutoff.

    Args:
        cursor: Database cursor.
        worker_id: Identifier for the worker claiming the job.

    Returns:
        The claimed job row, or None if no job is available.
    """
    cursor.execute(
        f"""
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
            {_JOB_COLUMNS};
        """,
        {"worker_id": worker_id},
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return JobRow(*row)


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
    job: JobRow,
    message: str,
    *,
    retry_delay: timedelta,
):
    """Mark a job as failed, or dead if attempts are exhausted.

    Retry delay changes only ``run_after``, not ``scheduled_at``.

    Args:
        cursor: Database cursor.
        job: Job row with current attempt counters.
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
            (message, job.id),
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
        (retry_delay, message, job.id),
    )
