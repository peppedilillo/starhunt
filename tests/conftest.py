from collections import defaultdict
from dataclasses import dataclass
import os
from pathlib import Path

import psycopg
import pytest

from starhunt.consumer import insert_message
from starhunt.consumer import PARSERS

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

LOCALIZATION_TOPICS = {
    "gcn.classic.voevent.FERMI_GBM_FIN_POS",
    "gcn.classic.voevent.FERMI_GBM_FLT_POS",
    "gcn.classic.voevent.FERMI_GBM_GND_POS",
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


def event_fixture_groups():
    by_trigger = defaultdict(list)
    for path in fixture_paths():
        by_trigger[parsed_notice(path).trig_id].append(path)
    return by_trigger


def alert_only_fixture():
    for paths in event_fixture_groups().values():
        topics = {fixture_topic(path) for path in paths}
        if topics == {"gcn.classic.voevent.FERMI_GBM_ALERT"}:
            return paths[0]
    raise AssertionError("Expected at least one alert-only fixture.")


def localization_fixtures(min_count: int):
    for paths in event_fixture_groups().values():
        localized = [path for path in paths if fixture_topic(path) in LOCALIZATION_TOPICS]
        if len(localized) >= min_count:
            return sorted(localized, key=lambda path: parsed_notice(path).alert_datetime)
    raise AssertionError(f"Expected at least one event with {min_count} localization notices.")


def pytest_addoption(parser):
    parser.addoption(
        "--smoke",
        action="store_true",
        default=False,
        help="run smoke tests against live external services",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--smoke"):
        return

    skip_smoke = pytest.mark.skip(reason="need --smoke option to run")
    for item in items:
        if "smoke" in item.keywords:
            item.add_marker(skip_smoke)
