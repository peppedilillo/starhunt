from datetime import datetime
from datetime import timedelta
from datetime import timezone
import json
from pathlib import Path

from conftest import alert_only_fixture
from conftest import insert_fixture
from conftest import localization_fixtures
from conftest import parsed_notice

from starhunt import worker
from starhunt.consumer import ZTF_CONESEARCH_JOB_TYPE
from starhunt.db import claim_expired_jobs
from starhunt.db import insert_event
from starhunt.db import pick_job
from starhunt.worker import run_job

TEST_RETRY_DELAY = timedelta(minutes=10)
TEST_QUERY_TIMEOUT = 7


def job_state(conn, job_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                status,
                scheduled_at,
                run_after,
                last_error,
                completed_at
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        return cur.fetchone()


def job_recovery_state(conn, job_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                status,
                scheduled_at,
                run_after,
                attempt_count,
                lease_until,
                last_error,
                completed_at
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        return cur.fetchone()


def create_running_job(
    conn,
    *,
    worker_id: str,
    max_attempts: int = 2,
    lease_delta: timedelta = -timedelta(minutes=1),
):
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        event_id = insert_event(
            cur,
            external_id=f"Fermi:{worker_id}",
        )
        cur.execute(
            """
            INSERT INTO jobs (
                event_id,
                job_type,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                run_after,
                max_attempts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                ZTF_CONESEARCH_JOB_TYPE,
                now - timedelta(hours=2),
                now - timedelta(hours=1),
                now - timedelta(minutes=1),
                now - timedelta(minutes=1),
                max_attempts,
            ),
        )
        job = pick_job(cur, worker_id)
        cur.execute(
            """
            UPDATE jobs
            SET lease_until = now() + %s
            WHERE id = %s
            """,
            (lease_delta, job.id),
        )
    conn.commit()
    return job


def conesearch_result_row(conn, job_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                broker,
                survey,
                subject_time_start,
                subject_time_end,
                ra,
                dec,
                radius,
                alert_count,
                result_uri
            FROM conesearches
            WHERE job_id = %s
            """,
            (job_id,),
        )
        return cur.fetchone()


class FakeQueryResult:
    def __init__(self, content: bytes):
        self.content = content

    def json(self):
        return json.loads(self.content)


def test_worker_main_passes_default_tuning(monkeypatch, tmp_path):
    calls = []
    db_conn = object()

    monkeypatch.setattr(worker, "init_db_conn", lambda: db_conn)
    monkeypatch.setattr(worker.uuid, "uuid4", lambda: "generated-worker-id")

    def fake_run_worker(*, db_conn, outdir, worker_id, poll_interval, retry_delay, timeout):
        calls.append(
            {
                "db_conn": db_conn,
                "outdir": outdir,
                "worker_id": worker_id,
                "poll_interval": poll_interval,
                "retry_delay": retry_delay,
                "timeout": timeout,
            }
        )

    monkeypatch.setattr(worker, "run_worker", fake_run_worker)

    worker.main(output_directory=tmp_path, worker_id=None)

    assert calls == [
        {
            "db_conn": db_conn,
            "outdir": tmp_path,
            "worker_id": "generated-worker-id",
            "poll_interval": worker.POLL_INTERVAL,
            "retry_delay": worker.DEFAULT_JOB_RETRY_DELAY,
            "timeout": worker.DEFAULT_CONESEARCH_TIMEOUT,
        }
    ]
    assert tmp_path.is_dir()


def test_claim_expired_jobs_requeues_retryable_running_job(db_conn):
    job = create_running_job(db_conn, worker_id="expired-retryable")
    retry_delay = timedelta(minutes=10)

    with db_conn.cursor() as cur:
        reclaimed = claim_expired_jobs(cur, retry_delay=retry_delay)
    db_conn.commit()

    (
        status,
        scheduled_at,
        run_after,
        attempt_count,
        lease_until,
        last_error,
        completed_at,
    ) = job_recovery_state(
        db_conn,
        job.id,
    )
    assert reclaimed == 1
    assert status == "failed"
    assert scheduled_at == job.scheduled_at
    assert run_after > job.run_after
    assert attempt_count == 1
    assert lease_until is None
    assert "Worker lease expired before completion" in last_error
    assert "worker_id=expired-retryable" in last_error
    assert completed_at is None


def test_claim_expired_jobs_dead_letters_exhausted_running_job(db_conn):
    job = create_running_job(db_conn, worker_id="expired-exhausted", max_attempts=1)

    with db_conn.cursor() as cur:
        reclaimed = claim_expired_jobs(cur, retry_delay=timedelta(minutes=10))
    db_conn.commit()

    (
        status,
        scheduled_at,
        run_after,
        attempt_count,
        lease_until,
        last_error,
        completed_at,
    ) = job_recovery_state(
        db_conn,
        job.id,
    )
    assert reclaimed == 1
    assert status == "dead"
    assert scheduled_at == job.scheduled_at
    assert run_after == job.run_after
    assert attempt_count == 1
    assert lease_until is None
    assert "Worker lease expired before completion" in last_error
    assert "worker_id=expired-exhausted" in last_error
    assert completed_at is not None


def test_claim_expired_jobs_leaves_active_running_job_untouched(db_conn):
    job = create_running_job(
        db_conn,
        worker_id="active-running",
        lease_delta=timedelta(minutes=10),
    )

    with db_conn.cursor() as cur:
        reclaimed = claim_expired_jobs(cur, retry_delay=timedelta(minutes=10))
    db_conn.commit()

    status, _, _, attempt_count, lease_until, last_error, completed_at = job_recovery_state(db_conn, job.id)
    assert reclaimed == 0
    assert status == "running"
    assert attempt_count == 1
    assert lease_until is not None
    assert last_error is None
    assert completed_at is None


def test_reclaimed_job_attempt_count_increments_only_when_picked_again(db_conn):
    job = create_running_job(db_conn, worker_id="expired-pick-again")

    with db_conn.cursor() as cur:
        claim_expired_jobs(cur, retry_delay=timedelta(0))
        status, _, _, attempt_count, _, _, _ = job_recovery_state(db_conn, job.id)
        next_job = pick_job(cur, "replacement-worker")
    db_conn.commit()

    assert status == "failed"
    assert attempt_count == 1
    assert next_job.id == job.id
    assert next_job.attempt_count == 2


def test_pick_job_uses_run_after_for_worker_eligibility(db_conn):
    now = datetime.now(timezone.utc)
    with db_conn.cursor() as cur:
        event_id = insert_event(
            cur,
            external_id="Fermi:future-run-after",
        )
        cur.execute(
            """
            INSERT INTO jobs (
                event_id,
                job_type,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                run_after,
                max_attempts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                ZTF_CONESEARCH_JOB_TYPE,
                now - timedelta(hours=2),
                now - timedelta(hours=1),
                now - timedelta(minutes=1),
                now + timedelta(minutes=10),
                2,
            ),
        )
        job = pick_job(cur, "worker-too-early")
    db_conn.commit()

    assert job is None


def test_run_job_executes_conesearch_and_persists_result(db_conn, tmp_path):
    path = localization_fixtures(1)[0]
    notice = parsed_notice(path)
    insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        job = pick_job(cur, "worker-success")
    db_conn.commit()

    calls = {}

    def fake_query_fn(*, ra, dec, radius, startdate, stopdate, timeout):
        calls.update(
            ra=ra,
            dec=dec,
            radius=radius,
            startdate=startdate,
            stopdate=stopdate,
            timeout=timeout,
        )
        return FakeQueryResult(b'[{"i:objectId":"ZTF-test"}]')

    run_job(
        db_conn,
        job,
        tmp_path,
        query_fn=fake_query_fn,
        retry_delay=TEST_RETRY_DELAY,
        timeout=TEST_QUERY_TIMEOUT,
    )

    assert calls == {
        "ra": notice.ra,
        "dec": notice.dec,
        "radius": notice.error_radius,
        "startdate": job.subject_time_start,
        "stopdate": job.subject_time_end,
        "timeout": TEST_QUERY_TIMEOUT,
    }

    status, scheduled_at, run_after, last_error, completed_at = job_state(db_conn, job.id)
    assert status == "succeeded"
    assert scheduled_at == job.scheduled_at
    assert run_after == job.run_after
    assert last_error is None
    assert completed_at is not None

    result_row = conesearch_result_row(db_conn, job.id)
    assert result_row[:8] == (
        "fink",
        "ztf",
        job.subject_time_start,
        job.subject_time_end,
        notice.ra,
        notice.dec,
        notice.error_radius,
        1,
    )
    result_path = Path(result_row[8].removeprefix("file://"))
    assert result_path.read_bytes() == b'[{"i:objectId":"ZTF-test"}]'


def test_run_job_missing_localization_marks_job_failed(db_conn, tmp_path):
    path = alert_only_fixture()
    insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        job = pick_job(cur, "worker-missing-localization")
    db_conn.commit()

    run_job(
        db_conn,
        job,
        tmp_path,
        retry_delay=TEST_RETRY_DELAY,
        timeout=TEST_QUERY_TIMEOUT,
    )

    status, scheduled_at, run_after, last_error, completed_at = job_state(db_conn, job.id)
    assert status == "failed"
    assert scheduled_at == job.scheduled_at
    assert run_after > job.run_after
    assert "No usable localization" in last_error
    assert completed_at is None


def test_run_job_empty_result_marks_success_without_persistence(db_conn, tmp_path):
    path = localization_fixtures(1)[0]
    insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        job = pick_job(cur, "worker-empty-result")
    db_conn.commit()

    run_job(
        db_conn,
        job,
        tmp_path,
        query_fn=lambda **kwargs: FakeQueryResult(b"[]"),
        retry_delay=TEST_RETRY_DELAY,
        timeout=TEST_QUERY_TIMEOUT,
    )

    status, scheduled_at, run_after, last_error, completed_at = job_state(db_conn, job.id)
    assert status == "succeeded"
    assert scheduled_at == job.scheduled_at
    assert run_after == job.run_after
    assert last_error is None
    assert completed_at is not None
    assert conesearch_result_row(db_conn, job.id) == (
        "fink",
        "ztf",
        job.subject_time_start,
        job.subject_time_end,
        parsed_notice(path).ra,
        parsed_notice(path).dec,
        parsed_notice(path).error_radius,
        0,
        None,
    )
    result_path = tmp_path / f"{job.job_type}_{job.id}.json"
    assert not result_path.exists()


def test_run_job_query_failure_can_dead_letter(db_conn, tmp_path):
    path = localization_fixtures(1)[0]
    insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        cur.execute("UPDATE jobs SET max_attempts = 1")
        job = pick_job(cur, "worker-query-failure")
    db_conn.commit()

    def failing_query_fn(**kwargs):
        raise RuntimeError("boom")

    run_job(
        db_conn,
        job,
        tmp_path,
        query_fn=failing_query_fn,
        retry_delay=TEST_RETRY_DELAY,
        timeout=TEST_QUERY_TIMEOUT,
    )

    status, _, _, last_error, completed_at = job_state(db_conn, job.id)
    assert status == "dead"
    assert last_error == "boom"
    assert completed_at is not None


def test_run_job_dead_letters_unsupported_job_type(db_conn, tmp_path):
    now = datetime.now(timezone.utc)
    with db_conn.cursor() as cur:
        event_id = insert_event(
            cur,
            external_id="Fermi:unsupported-job-event",
        )
        cur.execute(
            """
            INSERT INTO jobs (
                event_id,
                job_type,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                run_after,
                max_attempts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                "unsupported_job_type",
                now - timedelta(hours=2),
                now - timedelta(hours=1),
                now - timedelta(minutes=1),
                now - timedelta(minutes=1),
                2,
            ),
        )
        job = pick_job(cur, "worker-unsupported")
    db_conn.commit()

    run_job(
        db_conn,
        job,
        tmp_path,
        retry_delay=TEST_RETRY_DELAY,
        timeout=TEST_QUERY_TIMEOUT,
    )

    status, _, _, last_error, completed_at = job_state(db_conn, job.id)
    assert status == "dead"
    assert last_error == "Unsupported job type: unsupported_job_type"
    assert completed_at is not None
