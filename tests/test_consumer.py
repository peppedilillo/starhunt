from datetime import datetime
from datetime import timedelta
from datetime import timezone

from conftest import event_fixture_groups
from conftest import fixture_paths
from conftest import fixture_topic
from conftest import insert_fixture
from conftest import normalized_notice
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
                (SELECT count(*) FROM notices),
                (SELECT count(*) FROM conesearches),
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


def notice_row_by_ivorn(conn, ivorn: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                topic,
                mission,
                instrument,
                is_retraction,
                ra,
                dec,
                err_radius,
                raw_uri
            FROM notices
            WHERE ivorn = %s
            """,
            (ivorn,),
        )
        return cur.fetchone()


def retraction_state_by_ivorn(conn, ivorn: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                target.is_retraction,
                retractor.ivorn
            FROM notices AS target
            LEFT JOIN notices AS retractor
                ON retractor.id = target.retracted_by
            WHERE target.ivorn = %s
            """,
            (ivorn,),
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
            external_id="Fermi:test-scheduled-event",
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
            external_id="Fermi:test-idempotent-schedule",
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
            external_id="Fermi:test-invalid-schedule",
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


def test_can_insert_all_supported_fixtures(db_conn):
    paths = fixture_paths()
    expected_events = {f"{notice.mission}:{notice.burst_id}" for notice in (normalized_notice(path) for path in paths)}
    created_events = set()
    scheduled_events = set()
    for path in paths:
        notice = normalized_notice(path)
        event_id = f"{notice.mission}:{notice.burst_id}"
        if event_id in created_events:
            continue
        created_events.add(event_id)
        if not notice.retractions:
            scheduled_events.add(event_id)

    for path in paths:
        insert_fixture(db_conn, path)

    assert table_counts(db_conn) == (
        len(expected_events),
        len(paths),
        0,
        len(scheduled_events) * DEFAULT_CONESEARCH_TOTAL,
    )


def test_reinserting_all_fixtures_is_idempotent(db_conn):
    paths = fixture_paths()
    for path in paths:
        insert_fixture(db_conn, path)

    before = table_counts(db_conn)
    for path in paths:
        insert_fixture(db_conn, path)

    assert table_counts(db_conn) == before


def test_new_event_creates_event_notice_and_jobs(db_conn):
    insert_fixture(db_conn, fixture_paths()[0])

    assert table_counts(db_conn) == (1, 1, 0, DEFAULT_CONESEARCH_TOTAL)


def test_known_event_adds_notice_only(db_conn):
    first, second = fixture_pair_from_same_event()

    insert_fixture(db_conn, first)
    assert table_counts(db_conn) == (1, 1, 0, DEFAULT_CONESEARCH_TOTAL)

    insert_fixture(db_conn, second)
    assert table_counts(db_conn) == (1, 2, 0, DEFAULT_CONESEARCH_TOTAL)


def test_alert_notice_stores_null_localization(db_conn):
    path = next(path for path in fixture_paths() if fixture_topic(path) == "gcn.classic.voevent.FERMI_GBM_ALERT")
    notice = parsed_notice(path)

    insert_fixture(db_conn, path)

    row = notice_row_by_ivorn(db_conn, notice.ivorn)
    assert row[:7] == (
        fixture_topic(path),
        "Fermi",
        "GBM",
        False,
        None,
        None,
        None,
    )
    assert row[7] == path.resolve().as_uri()


def test_localized_notice_stores_coordinates(db_conn):
    path = next(path for path in fixture_paths() if fixture_topic(path) == "gcn.classic.voevent.FERMI_GBM_FLT_POS")
    notice = parsed_notice(path)

    insert_fixture(db_conn, path)

    row = notice_row_by_ivorn(db_conn, notice.ivorn)
    assert row[:7] == (
        fixture_topic(path),
        "Fermi",
        "GBM",
        False,
        notice.ra,
        notice.dec,
        notice.error_radius,
    )
    assert row[7] == path.resolve().as_uri()


def test_parse_message_normalizes_svom_retraction():
    path = next(path for path in fixture_paths() if normalized_notice(path).retractions)
    notice = normalized_notice(path)
    parsed = parsed_notice(path)

    assert notice.mission == "SVOM"
    assert notice.localization is None
    assert notice.ivorn == parsed.ivorn
    assert notice.burst_id == parsed.burst_id
    assert notice.retractions == parsed.retractions


def test_svom_retraction_marks_local_cited_notice(db_conn):
    trigger = next(
        path
        for path in fixture_paths()
        if normalized_notice(path).ivorn == "ivo://org.svom/fsc#sb26043009_grm-trigger"
    )
    retraction = next(
        path
        for path in fixture_paths()
        if normalized_notice(path).ivorn == "ivo://org.svom/fsc#sb26043009_retraction"
    )

    insert_fixture(db_conn, trigger)
    insert_fixture(db_conn, retraction)

    assert retraction_state_by_ivorn(db_conn, "ivo://org.svom/fsc#sb26043009_grm-trigger") == (
        False,
        "ivo://org.svom/fsc#sb26043009_retraction",
    )
    assert retraction_state_by_ivorn(db_conn, "ivo://org.svom/fsc#sb26043009_retraction") == (
        True,
        None,
    )
    assert table_counts(db_conn) == (1, 2, 0, DEFAULT_CONESEARCH_TOTAL)


def test_svom_notices_from_different_instruments_share_event(db_conn):
    paths = next(
        paths
        for paths in event_fixture_groups().values()
        if {normalized_notice(path).instrument for path in paths} >= {"GRM", "ECLAIRs", "MXT"}
    )

    for path in paths:
        insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        cur.execute("SELECT external_id FROM events")
        event_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT mission FROM notices")
        missions = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT instrument FROM notices ORDER BY instrument")
        instruments = [row[0] for row in cur.fetchall()]

    assert event_ids == [f"SVOM:{normalized_notice(paths[0]).burst_id}"]
    assert missions == ["SVOM"]
    assert instruments == ["ECLAIRs", "GRM", "MXT"]


def test_svom_grm_negative_radius_stores_null_localization(db_conn):
    path = next(
        path
        for path in fixture_paths()
        if fixture_topic(path) == "gcn.notices.svom.voevent.grm"
        and parsed_notice(path).error_radius is not None
        and parsed_notice(path).error_radius < 0
    )
    notice = parsed_notice(path)

    insert_fixture(db_conn, path)

    row = notice_row_by_ivorn(db_conn, notice.ivorn)
    assert row[:7] == (
        fixture_topic(path),
        "SVOM",
        "GRM",
        False,
        None,
        None,
        None,
    )
    assert row[7] == path.resolve().as_uri()
