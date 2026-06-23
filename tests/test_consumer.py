from datetime import datetime
from datetime import timedelta
from datetime import timezone

from conftest import event_fixture_groups
from conftest import fixture_paths
from conftest import fixture_topic
from conftest import insert_fixture
from conftest import parsed_notice
import pytest

from starhunt import consumer
from starhunt.consumer import DEFAULT_CONESEARCH_TOTAL
from starhunt.consumer import schedule_ztf_conesearch
from starhunt.consumer import ZTF_CONESEARCH_JOB_TYPE
from starhunt.db import get_or_create_event


def table_counts(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                (SELECT count(*) FROM events),
                (SELECT count(*) FROM milestones),
                (SELECT count(*) FROM artifacts),
                (SELECT count(*) FROM jobs)
            """)
        return cur.fetchone()


def job_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                event_id,
                job_type,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                run_after,
                status,
                attempt_count,
                max_attempts,
                payload
            FROM jobs
            ORDER BY subject_time_start
            """)
        return cur.fetchall()


def milestone_row_by_external_id(conn, external_id: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                milestone_subtype,
                ra,
                dec,
                err_radius
            FROM milestones
            WHERE external_id = %s
            """,
            (external_id,),
        )
        return cur.fetchone()


class StopConsumer(Exception):
    pass


class FakeConsumer:
    def __init__(self):
        self.closed = False
        self.consume_calls = 0
        self.subscribed_topics = None

    def subscribe(self, topics):
        self.subscribed_topics = topics

    def consume(self, *, timeout):
        self.consume_calls += 1
        if self.consume_calls > 1:
            raise StopConsumer
        return []

    def close(self):
        self.closed = True


def fixture_pair_from_same_event():
    for paths in event_fixture_groups().values():
        if len(paths) >= 2:
            return paths[:2]

    raise AssertionError("Expected at least one fixture pair from the same event.")


def test_consumer_main_initializes_and_closes_consumer(monkeypatch, tmp_path):
    fake_consumer = FakeConsumer()
    calls = {}

    def fake_init_consumer(offset, group_id):
        calls["offset"] = offset
        calls["group_id"] = group_id
        return fake_consumer

    def fake_init_db_conn():
        calls["db_initialized"] = True
        return object()

    monkeypatch.setattr(consumer, "init_consumer", fake_init_consumer)
    monkeypatch.setattr(consumer, "init_db_conn", fake_init_db_conn)

    with pytest.raises(StopConsumer):
        consumer.main(
            output_directory=tmp_path,
            group_id="group-1",
            offset="latest",
        )

    assert calls == {
        "offset": "latest",
        "group_id": "group-1",
        "db_initialized": True,
    }
    assert fake_consumer.subscribed_topics == [topic.topic for topic in consumer.TOPICS]
    assert fake_consumer.closed is True
    assert tmp_path.is_dir()


def test_schedule_ztf_conesearch_creates_time_window_jobs(db_conn):
    burst_datetime = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    offset = timedelta(hours=2)
    period = timedelta(hours=6)

    with db_conn.cursor() as cur:
        event_info = get_or_create_event(
            cur,
            external_id="Fermi.GBM:test-scheduled-event",
            mission="Fermi",
            instrument="GBM",
        )
        schedule_ztf_conesearch(
            cur,
            event_id=event_info.event_id,
            burst_datetime=burst_datetime,
            offset=offset,
            period=period,
            total=3,
            max_retry=4,
        )

    rows = job_rows(db_conn)
    assert len(rows) == 3
    for index, row in enumerate(rows):
        expected_start = burst_datetime + index * period
        expected_end = expected_start + period
        assert row == (
            event_info.event_id,
            ZTF_CONESEARCH_JOB_TYPE,
            expected_start,
            expected_end,
            expected_end + offset,
            expected_end + offset,
            "pending",
            0,
            4,
            {},
        )


def test_schedule_ztf_conesearch_is_idempotent(db_conn):
    burst_datetime = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_info = get_or_create_event(
            cur,
            external_id="Fermi.GBM:test-idempotent-schedule",
            mission="Fermi",
            instrument="GBM",
        )
        for _ in range(2):
            schedule_ztf_conesearch(
                cur,
                event_id=event_info.event_id,
                burst_datetime=burst_datetime,
                offset=timedelta(hours=2),
                period=timedelta(hours=6),
                total=3,
                max_retry=4,
            )

    assert len(job_rows(db_conn)) == 3


@pytest.mark.parametrize(
    ("burst_datetime", "offset", "period", "total", "max_retry", "message"),
    [
        (
            datetime(2026, 1, 1),
            timedelta(hours=1),
            timedelta(hours=1),
            1,
            1,
            "timezone-aware",
        ),
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            timedelta(hours=1),
            timedelta(0),
            1,
            1,
            "period",
        ),
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            -timedelta(seconds=1),
            timedelta(hours=1),
            1,
            1,
            "offset",
        ),
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            timedelta(hours=1),
            timedelta(hours=1),
            0,
            1,
            "total",
        ),
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            timedelta(hours=1),
            timedelta(hours=1),
            1,
            0,
            "max_retry",
        ),
    ],
)
def test_schedule_ztf_conesearch_rejects_invalid_inputs(
    db_conn,
    burst_datetime,
    offset,
    period,
    total,
    max_retry,
    message,
):
    with db_conn.cursor() as cur:
        event_info = get_or_create_event(
            cur,
            external_id="Fermi.GBM:test-invalid-schedule",
            mission="Fermi",
            instrument="GBM",
        )
        with pytest.raises(ValueError, match=message):
            schedule_ztf_conesearch(
                cur,
                event_id=event_info.event_id,
                burst_datetime=burst_datetime,
                offset=offset,
                period=period,
                total=total,
                max_retry=max_retry,
            )


def test_can_insert_all_fermi_fixtures(db_conn):
    paths = fixture_paths()
    expected_events = {parsed_notice(path).trig_id for path in paths}

    for path in paths:
        insert_fixture(db_conn, path)

    assert table_counts(db_conn) == (
        len(expected_events),
        len(paths),
        len(paths),
        len(expected_events) * DEFAULT_CONESEARCH_TOTAL,
    )


def test_reinserting_all_fixtures_is_idempotent(db_conn):
    paths = fixture_paths()
    for path in paths:
        insert_fixture(db_conn, path)

    before = table_counts(db_conn)
    for path in paths:
        insert_fixture(db_conn, path)

    assert table_counts(db_conn) == before


def test_new_event_creates_event_milestone_and_artifact(db_conn):
    insert_fixture(db_conn, fixture_paths()[0])

    assert table_counts(db_conn) == (1, 1, 1, DEFAULT_CONESEARCH_TOTAL)


def test_known_event_adds_milestone_and_artifact_only(db_conn):
    first, second = fixture_pair_from_same_event()

    insert_fixture(db_conn, first)
    assert table_counts(db_conn) == (1, 1, 1, DEFAULT_CONESEARCH_TOTAL)

    insert_fixture(db_conn, second)
    assert table_counts(db_conn) == (1, 2, 2, DEFAULT_CONESEARCH_TOTAL)


def test_alert_milestone_stores_null_localization(db_conn):
    path = next(path for path in fixture_paths() if fixture_topic(path) == "gcn.classic.voevent.FERMI_GBM_ALERT")
    notice = parsed_notice(path)

    insert_fixture(db_conn, path)

    assert milestone_row_by_external_id(db_conn, notice.ivorn) == (
        fixture_topic(path),
        None,
        None,
        None,
    )


def test_localized_milestone_stores_coordinates(db_conn):
    path = next(path for path in fixture_paths() if fixture_topic(path) == "gcn.classic.voevent.FERMI_GBM_FLT_POS")
    notice = parsed_notice(path)

    insert_fixture(db_conn, path)

    assert milestone_row_by_external_id(db_conn, notice.ivorn) == (
        fixture_topic(path),
        notice.ra,
        notice.dec,
        notice.error_radius,
    )
