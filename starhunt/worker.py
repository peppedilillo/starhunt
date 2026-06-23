"""
Worker process for executing scheduled background jobs. In a loop:

- Poll for one due job in `pending` or retryable `failed` state.
- Sleep briefly when no job is available.
- Execute the claimed job in-process.
- Persist either success metadata or failure metadata before moving on.
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from time import sleep
import uuid

from psycopg import Connection

from .consumer import ZTF_CONESEARCH_JOB_TYPE
from .db import claim_expired_jobs
from .db import find_best_localization
from .db import get_or_create_milestone
from .db import init_db_conn
from .db import insert_artifact
from .db import JobInfo
from .db import mark_job_dead
from .db import mark_job_failed
from .db import mark_job_succeeded
from .db import pick_job
from .queries import conesearch_fink_ztf

POLL_INTERVAL = 5
DEFAULT_JOB_RETRY_DELAY = timedelta(hours=12)
DEFAULT_CONESEARCH_TIMEOUT = 60


ZTF_FINK_CONESEARCH_ARTIFACT_TYPE = "ztf.fink.conesearch.json"


class MissingLocalization(Exception):
    """Raised when a job has no usable localization."""

    pass


def write_response(job: JobInfo, content: bytes, outdir: Path) -> Path:
    """Write a job response to disk.

    Args:
        job: Job that produced the response.
        content: Raw response bytes to persist.
        outdir: Directory where the response file should be written.

    Returns:
        Path to the written response file.
    """
    filepath = outdir / f"{job.job_type}_{job.job_id}.json"
    filepath.write_bytes(content)
    return filepath


def execute_ztf_fink_conesearch(
    cursor,
    job: JobInfo,
    outdir: Path,
    timeout: float | None,
    query_fn=conesearch_fink_ztf,
) -> int | None:
    """Execute a ZTF Fink conesearch job.

    Args:
        cursor: Database cursor.
        job: Claimed conesearch job to execute.
        outdir: Directory where non-empty query responses should be written.
        timeout: Maximum seconds to wait for the external query response.
        query_fn: Callable used to run the conesearch query.

    Returns:
        Result artifact primary key for non-empty responses, else None.

    Raises:
        MissingLocalization: If no usable localization is available.
    """
    # use the planned schedule as the localization cutoff so retries and
    # backfills reproduce the information available at the intended run time.
    localization = find_best_localization(cursor, job.event_id, job.scheduled_at)
    if localization is None:
        raise MissingLocalization("No usable localization available for event")

    result = query_fn(
        ra=localization.ra,
        dec=localization.dec,
        radius=localization.err_radius * 3600,
        startdate=job.subject_time_start,
        stopdate=job.subject_time_end,
        # prevents a stalled Fink response from holding a job and transaction indefinitely.
        timeout=timeout,
    )
    if len(result.json()) == 0:
        return None

    filepath = write_response(job, result.content, outdir)
    milestone_id = get_or_create_milestone(
        cursor,
        event_id=job.event_id,
        external_id=f"{job.job_type}:{job.job_id}",
        subtype=job.job_type,
        published_at=datetime.now(timezone.utc),
        subject_time_start=job.subject_time_start,
        subject_time_end=job.subject_time_end,
        ra=localization.ra,
        dec=localization.dec,
        err_radius=localization.err_radius,
        milestone_type="conesearch",
    )
    return insert_artifact(
        cursor,
        milestone_id=milestone_id,
        uri=filepath.resolve().as_uri(),
        artifact_type=ZTF_FINK_CONESEARCH_ARTIFACT_TYPE,
    )


def run_job(
    db_conn: Connection,
    job: JobInfo,
    outdir: Path,
    retry_delay: timedelta,
    timeout: float | None,
    query_fn=conesearch_fink_ztf,
):
    """Execute a claimed job and persist its lifecycle state.

    Consumer code inserts jobs with a stable ``scheduled_at`` cutoff and a
    mutable ``run_after`` eligibility time. A worker claims one due job
    atomically before calling this function, marking it ``running``, assigning
    ``worker_id``, and incrementing ``attempt_count``.

    For ``ztf_fink_conesearch`` jobs, the worker resolves the best localization
    available for the event at the job schedule time, runs the Fink conesearch,
    writes the raw response to disk and, if the response is not empty, stores a
    ``conesearch`` milestone plus result artifact. Finally, the job is marked
    as ``succeeded``.

    If execution fails but retries remain, the job is marked ``failed``,
    ``last_error`` is stored, and ``run_after`` is moved later so it can be
    retried. If execution fails after attempts are exhausted, or if the job type
    is unsupported, the job is marked ``dead``.

    Args:
        db_conn: Database connection used for job state transitions.
        job: Claimed job to execute.
        outdir: Directory where query responses should be written.
        retry_delay: Delay before a failed job is eligible to run again.
        timeout: Maximum seconds to wait for the external query response.
        query_fn: Callable used to run the conesearch query. Intended for testing.
    """
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
                mark_job_succeeded(cursor, job.job_id, result)
            db_conn.commit()
        else:
            with db_conn.cursor() as cursor:
                mark_job_dead(cursor, job.job_id, f"Unsupported job type: {job.job_type}")
            db_conn.commit()
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


def run_worker(
    db_conn: Connection,
    outdir: Path,
    worker_id: str,
    poll_interval: int | float,
    retry_delay: timedelta,
    timeout: float | None,
):
    """Continuously claim and execute scheduled jobs.

    Args:
        db_conn: Database connection used to claim and run jobs.
        outdir: Directory where query responses should be written.
        worker_id: Identifier assigned to claimed jobs.
        poll_interval: Seconds to sleep when no job is available.
        retry_delay: Delay before retryable jobs are eligible again.
        timeout: Maximum seconds to wait for external query responses.
    """
    while True:
        with db_conn.cursor() as cursor:
            claim_expired_jobs(cursor, retry_delay=retry_delay)
            job = pick_job(cursor, worker_id)
        db_conn.commit()
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
    """Start a worker process.

    Args:
        output_directory: Directory where query responses should be written.
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
