from datetime import datetime
from datetime import timedelta
from datetime import timezone

from conftest import alert_only_fixture
from conftest import event_external_id
from conftest import insert_fixture
from conftest import localization_fixtures
from conftest import normalized_notice
from conftest import parsed_notice

from starhunt.db import find_best_localization
from starhunt.db import get_or_create_event
from starhunt.db import insert_notice
from starhunt.db import Localization
from starhunt.db import mark_retracted_notices


def test_get_or_create_event_returns_new_event_info(db_conn):
    with db_conn.cursor() as cur:
        event_info = get_or_create_event(
            cur,
            external_id="Fermi:test-new-event",
        )

    assert event_info.is_new is True
    assert isinstance(event_info.event_id, int)


def test_get_or_create_event_returns_existing_event_info(db_conn):
    with db_conn.cursor() as cur:
        first = get_or_create_event(
            cur,
            external_id="Fermi:test-existing-event",
        )
        second = get_or_create_event(
            cur,
            external_id="Fermi:test-existing-event",
        )

    assert first.is_new is True
    assert second.is_new is False
    assert second.event_id == first.event_id


def test_find_best_localization_returns_none_without_localization(db_conn):
    path = alert_only_fixture()
    notice = normalized_notice(path)
    insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM events WHERE external_id = %s",
            (event_external_id(path),),
        )
        event_id = cur.fetchone()[0]
        localization = find_best_localization(
            cur,
            event_id=event_id,
            cutoff_at=notice.published_at + timedelta(hours=1),
        )

    assert localization is None


def test_find_best_localization_returns_latest_usable_localization(db_conn):
    paths = localization_fixtures(2)
    for path in paths[:2]:
        insert_fixture(db_conn, path)

    latest_notice = parsed_notice(paths[1])
    earlier_notice = parsed_notice(paths[0])
    latest_normalized = normalized_notice(paths[1])

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM events WHERE external_id = %s",
            (event_external_id(paths[1]),),
        )
        event_id = cur.fetchone()[0]
        localization = find_best_localization(
            cur,
            event_id=event_id,
            cutoff_at=latest_normalized.published_at + timedelta(seconds=1),
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
    later_normalized = normalized_notice(paths[1])

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM events WHERE external_id = %s",
            (event_external_id(paths[0]),),
        )
        event_id = cur.fetchone()[0]
        localization = find_best_localization(
            cur,
            event_id=event_id,
            cutoff_at=later_normalized.published_at - timedelta(seconds=1),
        )

    assert localization == Localization(
        ra=earlier_notice.ra,
        dec=earlier_notice.dec,
        err_radius=earlier_notice.error_radius,
    )


def test_find_best_localization_ignores_conesearch_coordinates(db_conn):
    path = localization_fixtures(1)[0]
    notice = parsed_notice(path)
    normalized = normalized_notice(path)
    insert_fixture(db_conn, path)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM events WHERE external_id = %s",
            (event_external_id(path),),
        )
        event_id = cur.fetchone()[0]
        cur.execute(
            """
            SELECT id, subject_time_start, subject_time_end
            FROM jobs
            WHERE event_id = %s
            ORDER BY subject_time_start
            LIMIT 1
            """,
            (event_id,),
        )
        job_id, subject_time_start, subject_time_end = cur.fetchone()
        cur.execute(
            """
            INSERT INTO conesearches (
                event_id,
                job_id,
                broker,
                survey,
                subject_time_start,
                subject_time_end,
                queried_at,
                ra,
                dec,
                radius_arcsec,
                alert_count
            )
            VALUES (%s, %s, 'fink', 'ztf', %s, %s, %s, 1, 2, 3, 0)
            """,
            (
                event_id,
                job_id,
                subject_time_start,
                subject_time_end,
                normalized.published_at + timedelta(hours=1),
            ),
        )
        localization = find_best_localization(
            cur,
            event_id=event_id,
            cutoff_at=normalized.published_at + timedelta(hours=2),
        )

    assert localization == Localization(
        ra=notice.ra,
        dec=notice.dec,
        err_radius=notice.error_radius,
    )


def test_find_best_localization_ignores_localization_retracted_before_cutoff(db_conn):
    published_at = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    retracted_at = published_at + timedelta(minutes=10)
    cutoff_at = published_at + timedelta(minutes=20)

    with db_conn.cursor() as cur:
        event = get_or_create_event(cur, external_id="SVOM:retracted-before-cutoff")
        insert_notice(
            cur,
            event_id=event.event_id,
            ivorn="ivo://org.svom/fsc#retracted-before-cutoff_slewing",
            topic="gcn.notices.svom.voevent.eclairs",
            mission="SVOM",
            instrument="ECLAIRs",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/retracted-before-cutoff_slewing.xml",
            ra=1,
            dec=2,
            err_radius=0.1,
        )
        retraction_id = insert_notice(
            cur,
            event_id=event.event_id,
            ivorn="ivo://org.svom/fsc#retracted-before-cutoff_retraction",
            topic="gcn.notices.svom.voevent.eclairs",
            mission="SVOM",
            instrument="ECLAIRs",
            is_retraction=True,
            published_at=retracted_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/retracted-before-cutoff_retraction.xml",
        )
        mark_retracted_notices(
            cur,
            event_id=event.event_id,
            retraction_notice_id=retraction_id,
            target_ivorns=("ivo://org.svom/fsc#retracted-before-cutoff_slewing",),
        )

        localization = find_best_localization(cur, event.event_id, cutoff_at=cutoff_at)

    assert localization is None


def test_find_best_localization_keeps_localization_retracted_after_cutoff(db_conn):
    published_at = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    cutoff_at = published_at + timedelta(minutes=10)
    retracted_at = published_at + timedelta(minutes=20)

    with db_conn.cursor() as cur:
        event = get_or_create_event(cur, external_id="SVOM:retracted-after-cutoff")
        insert_notice(
            cur,
            event_id=event.event_id,
            ivorn="ivo://org.svom/fsc#retracted-after-cutoff_slewing",
            topic="gcn.notices.svom.voevent.eclairs",
            mission="SVOM",
            instrument="ECLAIRs",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/retracted-after-cutoff_slewing.xml",
            ra=1,
            dec=2,
            err_radius=0.1,
        )
        retraction_id = insert_notice(
            cur,
            event_id=event.event_id,
            ivorn="ivo://org.svom/fsc#retracted-after-cutoff_retraction",
            topic="gcn.notices.svom.voevent.eclairs",
            mission="SVOM",
            instrument="ECLAIRs",
            is_retraction=True,
            published_at=retracted_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/retracted-after-cutoff_retraction.xml",
        )
        mark_retracted_notices(
            cur,
            event_id=event.event_id,
            retraction_notice_id=retraction_id,
            target_ivorns=("ivo://org.svom/fsc#retracted-after-cutoff_slewing",),
        )

        localization = find_best_localization(cur, event.event_id, cutoff_at=cutoff_at)

    assert localization == Localization(ra=1, dec=2, err_radius=0.1)
