from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import os

import psycopg
import pytest

from starhunt.main import PARSERS
from starhunt.main import insert_message

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
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
                (SELECT count(*) FROM artifacts)
            """)
        return cur.fetchone()


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

    assert table_counts(db_conn) == (len(expected_events), len(paths), len(paths))


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

    assert table_counts(db_conn) == (1, 1, 1)


def test_known_event_adds_milestone_and_artifact_only(db_conn):
    first, second = fixture_pair_from_same_event()

    insert_fixture(db_conn, first)
    assert table_counts(db_conn) == (1, 1, 1)

    insert_fixture(db_conn, second)
    assert table_counts(db_conn) == (1, 2, 2)
