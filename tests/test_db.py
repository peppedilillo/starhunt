from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import os
from pathlib import Path

import psycopg
import pytest

from starhunt.consumer import DEFAULT_CONESEARCH_TOTAL
from starhunt.consumer import get_or_create_event
from starhunt.consumer import insert_message
from starhunt.consumer import PARSERS
from starhunt.consumer import schedule_ztf_conesearch
from starhunt.consumer import ZTF_CONESEARCH_JOB_TYPE

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "notices"
SCHEMA = ROOT / "db" / "001_schema.sql"
TEST_DATABASE_URL = os.environ.get(
    "STARHUNT_TEST_DATABASE_URL",
    "postgresql://starhunt_test:starhunt_test@localhost:55432/starhunt_test",
)

TOPICS_BY_FILENAME_SUFFIX = {
    "gcn_classic_voevent_fermi_gbm_alert.xml": "gcn.classic.voevent.FERMI_GBM_ALERT",
    "gcn_classic_voevent_fermi_gbm_fin_pos.xml": "gcn.classic.voevent.FERMI_GBM_FIN_POS",
    "gcn_classic_voevent_fermi_gbm_flt_pos.xml": "gcn.classic.voevent.FERMI_GBM_FLT_POS",
    "gcn_classic_voevent_fermi_gbm_gnd_pos.xml": "gcn.classic.voevent.FERMI_GBM_GND_POS",
}


@dataclass
class FixtureMessage:
    topic_name: str
    payload: bytes

    def topic(self):
        return self.topic_name

    def value(self):
        return self.payload


@pytest.fixture
def db_conn():
    try:
        conn = psycopg.connect(TEST_DATABASE_URL)
    except psycopg.OperationalError as exc:
        pytest.fail(
            "Could not connect to the Starhunt test database. "
            "Start it with `docker compose --profile test up -d postgres-test`. "
            f"Connection error: {exc}"
        )

    with conn:
        reset_database(conn)
        yield conn


def reset_database(conn):
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute(SCHEMA.read_text())


def fixture_paths():
    return sorted(FIXTURES.glob("*.xml"))


def fixture_topic(path: Path):
    for suffix, topic in TOPICS_BY_FILENAME_SUFFIX.items():
        if path.name.endswith(suffix):
            return topic
    raise AssertionError(f"Unsupported fixture filename: {path.name}")


def parsed_notice(path: Path):
    return PARSERS[fixture_topic(path)](path.read_bytes())


def insert_fixture(conn, path: Path):
    message = FixtureMessage(fixture_topic(path), path.read_bytes())
    insert_message(message, path, conn)


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
        cur.execute(
            """
            SELECT
                event_id,
                job_type,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                status,
                attempt_count,
                max_attempts,
                payload
            FROM jobs
            ORDER BY subject_time_start
            """
        )
        return cur.fetchall()


def test_get_or_create_event_returns_new_event_info(db_conn):
    with db_conn.cursor() as cur:
        event_info = get_or_create_event(
            cur,
            external_id="Fermi.GBM:test-new-event",
            mission="Fermi",
            instrument="GBM",
        )

    assert event_info.is_new is True
    assert isinstance(event_info.event_id, int)


def test_get_or_create_event_returns_existing_event_info(db_conn):
    with db_conn.cursor() as cur:
        first = get_or_create_event(
            cur,
            external_id="Fermi.GBM:test-existing-event",
            mission="Fermi",
            instrument="GBM",
        )
        second = get_or_create_event(
            cur,
            external_id="Fermi.GBM:test-existing-event",
            mission="Fermi",
            instrument="GBM",
        )

    assert first.is_new is True
    assert second.is_new is False
    assert second.event_id == first.event_id


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
            "pending",
            0,
            2,
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
            )

    assert len(job_rows(db_conn)) == 3


@pytest.mark.parametrize(
    ("burst_datetime", "offset", "period", "total", "message"),
    [
        (
            datetime(2026, 1, 1),
            timedelta(hours=1),
            timedelta(hours=1),
            1,
            "timezone-aware",
        ),
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            timedelta(hours=1),
            timedelta(0),
            1,
            "period",
        ),
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            -timedelta(seconds=1),
            timedelta(hours=1),
            1,
            "offset",
        ),
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            timedelta(hours=1),
            timedelta(hours=1),
            0,
            "total",
        ),
    ],
)
def test_schedule_ztf_conesearch_rejects_invalid_inputs(
    db_conn,
    burst_datetime,
    offset,
    period,
    total,
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
            )


def fixture_pair_from_same_event():
    by_trigger = defaultdict(list)
    for path in fixture_paths():
        by_trigger[parsed_notice(path).trig_id].append(path)

    for paths in by_trigger.values():
        if len(paths) >= 2:
            return paths[:2]

    raise AssertionError("Expected at least one fixture pair from the same event.")


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
