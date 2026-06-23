from datetime import timedelta

from conftest import alert_only_fixture
from conftest import insert_fixture
from conftest import localization_fixtures
from conftest import parsed_notice

from starhunt.db import find_best_localization
from starhunt.db import get_or_create_event
from starhunt.db import Localization


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


def test_find_best_localization_returns_none_without_localization(db_conn):
    path = alert_only_fixture()
    notice = parsed_notice(path)
    insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM events WHERE external_id = %s",
            (f"Fermi.GBM:{notice.trig_id}",),
        )
        event_id = cur.fetchone()[0]
        localization = find_best_localization(
            cur,
            event_id=event_id,
            cutoff_at=notice.alert_datetime + timedelta(hours=1),
        )

    assert localization is None


def test_find_best_localization_returns_latest_usable_localization(db_conn):
    paths = localization_fixtures(2)
    for path in paths[:2]:
        insert_fixture(db_conn, path)

    latest_notice = parsed_notice(paths[1])
    earlier_notice = parsed_notice(paths[0])

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM events WHERE external_id = %s",
            (f"Fermi.GBM:{latest_notice.trig_id}",),
        )
        event_id = cur.fetchone()[0]
        localization = find_best_localization(
            cur,
            event_id=event_id,
            cutoff_at=latest_notice.alert_datetime + timedelta(seconds=1),
        )

    assert localization == Localization(
        ra=latest_notice.ra,
        dec=latest_notice.dec,
        err_radius=latest_notice.error_radius,
    )
    assert localization != Localization(
        ra=earlier_notice.ra,
        dec=earlier_notice.dec,
        err_radius=earlier_notice.error_radius,
    )


def test_find_best_localization_ignores_future_publications(db_conn):
    paths = localization_fixtures(2)
    for path in paths[:2]:
        insert_fixture(db_conn, path)

    earlier_notice = parsed_notice(paths[0])
    later_notice = parsed_notice(paths[1])

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM events WHERE external_id = %s",
            (f"Fermi.GBM:{earlier_notice.trig_id}",),
        )
        event_id = cur.fetchone()[0]
        localization = find_best_localization(
            cur,
            event_id=event_id,
            cutoff_at=later_notice.alert_datetime - timedelta(seconds=1),
        )

    assert localization == Localization(
        ra=earlier_notice.ra,
        dec=earlier_notice.dec,
        err_radius=earlier_notice.error_radius,
    )
