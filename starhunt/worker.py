"""
Worker process for scheduled jobs.

The worker claims one due job, executes it, and records success or failure
before moving on.
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
import logging
from pathlib import Path
from time import sleep
import uuid

from psycopg import Connection

from .astro import cone_region_from_coordinates
from .conesearches import conesearch_fink_ztf
from .consumer import ZTF_CONESEARCH_JOB_TYPE
from .db import claim_expired_jobs
from .db import find_best_localized_notice
from .db import init_db_conn
from .db import insert_conesearch
from .db import JobRow
from .db import mark_job_dead
from .db import mark_job_failed
from .db import mark_job_succeeded
from .db import pick_job

POLL_INTERVAL = 5
DEFAULT_JOB_RETRY_DELAY = timedelta(hours=12)
DEFAULT_CONESEARCH_TIMEOUT = 60


logger = logging.getLogger(__name__)


class MissingLocalization(Exception):
    """Raised when a job has no usable localization."""

    pass


def write_response(job: JobRow, content: bytes, outdir: Path) -> Path:
    """Write a broker response to disk.

    Args:
        job: Job that produced the response.
        content: Raw response bytes.
        outdir: Output directory.

    Returns:
        Path to the written result file.
    """
    filepath = outdir / f"{job.job_type}_{job.id}.json"
    filepath.write_bytes(content)
    return filepath


def execute_ztf_fink_conesearch(
    cursor,
    job: JobRow,
    outdir: Path,
    timeout: float | None,
    query_fn=conesearch_fink_ztf,
) -> int:
    """Execute a ZTF Fink conesearch job.

    Args:
        cursor: Database cursor.
        job: Claimed conesearch job to execute.
        outdir: Directory where non-empty query responses should be written.
        timeout: Maximum seconds to wait for the external query response.
        query_fn: Callable used to run the conesearch query.

    Returns:
        Conesearch primary key.

    Raises:
        MissingLocalization: If no usable localization is available.
    """
    # Use `scheduled_at` as the localization cutoff so retries use the same
    # localization as the original run.
    #
    # An example:
    #    * event arrives at t=0 with localization, job `scheduled_at` is set to t=+1,
    #    * at t=+1 the job fails. `run_after` is set at t=+3
    #    * a new localization arrives at t=+2
    #    * at t=3 the job runs again with the localization provided at t=0,
    #      ignoring the localization arrived at t=+2
    notice = find_best_localized_notice(cursor, job.event_id, job.scheduled_at)
    if notice is None:
        raise MissingLocalization("No usable localization available for event")
    localization = cone_region_from_coordinates(notice.ra, notice.dec, notice.err_radius)
    if localization is None:
        raise MissingLocalization("No usable localization available for event")

    result = query_fn(
        ra=localization.ra,
        dec=localization.dec,
        radius=localization.err_radius,
        startdate=job.subject_time_start,
        stopdate=job.subject_time_end,
        # Prevent a stalled Fink response from holding a transaction indefinitely.
        timeout=timeout,
    )
    alerts = result.json()
    result_uri = None
    if alerts:
        result_uri = write_response(job, result.content, outdir).resolve().as_uri()

    return insert_conesearch(
        cursor,
        event_id=job.event_id,
        job_id=job.id,
        broker="fink",
        survey="ztf",
        subject_time_start=job.subject_time_start,
        subject_time_end=job.subject_time_end,
        queried_at=datetime.now(timezone.utc),
        ra=localization.ra,
        dec=localization.dec,
        radius=localization.err_radius,
        alert_count=len(alerts),
        result_uri=result_uri,
    )


def run_job(
    db_conn: Connection,
    job: JobRow,
    outdir: Path,
    retry_delay: timedelta,
    timeout: float | None,
    query_fn=conesearch_fink_ztf,
):
    """Execute a claimed job and update its state.

    For ``ztf_fink_conesearch`` jobs, the worker resolves the best localization
    available at ``scheduled_at``, runs the Fink conesearch, stores a
    ``conesearch`` row, and marks the job as ``succeeded``.

    Failed jobs are retried until ``max_attempts`` is reached. Unsupported job
    types are marked ``dead``.

    Args:
        db_conn: Database connection used for job state transitions.
        job: Claimed job to execute.
        outdir: Directory for non-empty query responses.
        retry_delay: Delay before a failed job is eligible to run again.
        timeout: Maximum seconds to wait for the external query response.
        query_fn: Callable used to run the conesearch query. Intended for testing.
    """
    log_context = {
        "job_id": job.id,
        "event_id": job.event_id,
        "job_type": job.job_type,
        "attempt_count": job.attempt_count,
        "max_attempts": job.max_attempts,
    }
    logger.info("Job started", extra=log_context)
    try:
        if job.job_type == ZTF_CONESEARCH_JOB_TYPE:
            with db_conn.cursor() as cursor:
                result = execute_ztf_fink_conesearch(
                    cursor,
                    job=job,
                    outdir=outdir,
                    timeout=timeout,
                    query_fn=query_fn,
                )
                mark_job_succeeded(cursor, job.id)
            db_conn.commit()
            logger.info(
                "Job succeeded",
                extra=log_context | {"status": "succeeded", "conesearch_id": result},
            )
        else:
            with db_conn.cursor() as cursor:
                mark_job_dead(cursor, job.id, f"Unsupported job type: {job.job_type}")
            db_conn.commit()
            logger.error(
                "Unsupported job type",
                extra=log_context | {"status": "dead"},
            )
    except Exception as exc:
        # clear any aborted transaction before recording the job failure.
        db_conn.rollback()
        with db_conn.cursor() as cursor:
            mark_job_failed(
                cursor,
                job=job,
                message=str(exc),
                retry_delay=retry_delay,
            )
        db_conn.commit()
        exhausted = job.attempt_count >= job.max_attempts
        log = logger.error if exhausted else logger.warning
        log(
            "Job failed",
            extra=log_context | {"status": "dead" if exhausted else "failed"},
            exc_info=True,
        )


def run_worker(
    db_conn: Connection,
    outdir: Path,
    worker_id: str,
    poll_interval: int | float,
    retry_delay: timedelta,
    timeout: float | None,
):
    """Claim and execute jobs forever.

    Args:
        db_conn: Database connection.
        outdir: Directory for non-empty query responses.
        worker_id: Identifier for claimed jobs.
        poll_interval: Seconds to sleep when no job is available.
        retry_delay: Delay before retryable jobs are eligible again.
        timeout: Maximum seconds to wait for external query responses.
    """
    while True:
        with db_conn.cursor() as cursor:
            reclaimed = claim_expired_jobs(cursor, retry_delay=retry_delay)
            job = pick_job(cursor, worker_id)
        db_conn.commit()
        if reclaimed:
            logger.warning(
                "Expired worker leases recovered",
                extra={
                    "worker_id": worker_id,
                    "recovered_jobs": reclaimed,
                },
            )
        if job is None:
            sleep(poll_interval)
            continue
        run_job(
            db_conn=db_conn,
            job=job,
            outdir=outdir,
            retry_delay=retry_delay,
            timeout=timeout,
        )


def main(
    output_directory: Path,
    worker_id: str | None,
):
    """Start the worker.

    Args:
        output_directory: Directory for non-empty query responses.
        worker_id: Optional worker identifier. A UUID is generated when absent.
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    db_conn = init_db_conn()
    run_worker(
        db_conn=db_conn,
        outdir=output_directory,
        worker_id=worker_id if worker_id is not None else str(uuid.uuid4()),
        poll_interval=POLL_INTERVAL,
        retry_delay=DEFAULT_JOB_RETRY_DELAY,
        timeout=DEFAULT_CONESEARCH_TIMEOUT,
    )


if __name__ == "__main__":
    main()
