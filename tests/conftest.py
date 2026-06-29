from collections import defaultdict
from dataclasses import dataclass
import os
from pathlib import Path

import psycopg
import pytest

from starhunt.consumer import insert_message
from starhunt.consumer import parse_message
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
    "gcn_notices_svom_voevent_eclairs.xml": "gcn.notices.svom.voevent.eclairs",
    "gcn_notices_svom_voevent_grm.xml": "gcn.notices.svom.voevent.grm",
    "gcn_notices_svom_voevent_mxt.xml": "gcn.notices.svom.voevent.mxt",
    "gcn_notices_einstein_probe_wxt_alert.json": "gcn.notices.einstein_probe.wxt.alert",
}


@dataclass
class FixtureMessage:
    topic_name: str
    payload: bytes
    kafka_partition: int = 0
    kafka_offset: int = 0

    def topic(self):
        return self.topic_name

    def value(self):
        return self.payload

    def partition(self):
        return self.kafka_partition

    def offset(self):
        return self.kafka_offset


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
    return sorted([*FIXTURES.glob("*.xml"), *FIXTURES.glob("*.json")])


def fixture_topic(path: Path):
    for suffix, topic in TOPICS_BY_FILENAME_SUFFIX.items():
        if path.name.endswith(suffix):
            return topic
    raise AssertionError(f"Unsupported fixture filename: {path.name}")


def fixture_kafka_coordinates(path: Path):
    partition, offset, _ = path.name.split("_", maxsplit=2)
    return int(partition), int(offset)


def parsed_notice(path: Path):
    return PARSERS[fixture_topic(path)](path.read_bytes())


def normalized_notice(path: Path):
    return parse_message(FixtureMessage(fixture_topic(path), path.read_bytes()))


def event_external_id(path: Path):
    notice = normalized_notice(path)
    return f"{notice.mission}:{notice.burst_id}"


def insert_fixture(conn, path: Path):
    kafka_partition, kafka_offset = fixture_kafka_coordinates(path)
    message = FixtureMessage(
        fixture_topic(path),
        path.read_bytes(),
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
    )
    insert_message(message, path, conn)


def event_fixture_groups():
    by_event = defaultdict(list)
    for path in fixture_paths():
        by_event[event_external_id(path)].append(path)
    return by_event


def alert_only_fixture():
    for paths in event_fixture_groups().values():
        notices = [normalized_notice(path) for path in paths]
        if all(notice.localization is None and not notice.retractions for notice in notices):
            return paths[0]
    raise AssertionError("Expected at least one alert-only fixture.")


def localization_fixtures(min_count: int):
    for paths in event_fixture_groups().values():
        localized = [path for path in paths if normalized_notice(path).localization is not None]
        if len(localized) >= min_count:
            return sorted(localized, key=lambda path: normalized_notice(path).published_at)
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
