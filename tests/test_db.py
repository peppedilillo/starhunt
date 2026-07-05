from datetime import datetime
from datetime import timedelta
from datetime import timezone

from conftest import alert_only_fixture
from conftest import event_external_id
from conftest import insert_fixture
from conftest import localization_fixtures
from conftest import normalized_notice
from conftest import parsed_notice
import psycopg
import pytest

from starhunt.db import Event
from starhunt.db import find_best_localization
from starhunt.db import get_event
from starhunt.db import get_event_conesearches
from starhunt.db import get_event_notices
from starhunt.db import insert_conesearch
from starhunt.db import insert_event
from starhunt.db import insert_notice_json
from starhunt.db import insert_notice_voevent
from starhunt.db import list_events
from starhunt.db import Localization
from starhunt.db import mark_retracted_notices


def insert_event_at(conn, *, external_id: str, created_at: datetime) -> int:
    with conn.cursor() as cur:
        event_id = insert_event(cur, external_id=external_id)
        cur.execute(
            """
            UPDATE events
            SET created_at = %s
            WHERE id = %s
            """,
            (created_at, event_id),
        )
    return event_id


def insert_job(
    cur,
    *,
    event_id: int,
    subject_time_start: datetime,
    subject_time_end: datetime | None = None,
) -> int:
    if subject_time_end is None:
        subject_time_end = subject_time_start + timedelta(hours=1)
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
        VALUES (%s, 'ztf_fink_conesearch', %s, %s, %s, %s, 1)
        RETURNING id
        """,
        (
            event_id,
            subject_time_start,
            subject_time_end,
            subject_time_start,
            subject_time_start,
        ),
    )
    return cur.fetchone()[0]


def test_get_event_returns_none_for_missing_event(db_conn):
    with db_conn.cursor() as cur:
        event = get_event(cur, external_id="Fermi:test-missing-event")

    assert event is None


def test_insert_event_returns_event_id(db_conn):
    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:test-new-event")

    assert isinstance(event_id, int)


def test_get_event_returns_event_dataclass(db_conn):
    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:test-existing-event")
        event = get_event(cur, external_id="Fermi:test-existing-event")

    assert event is not None
    assert event == Event(
        id=event_id,
        external_id="Fermi:test-existing-event",
        created_at=event.created_at,
    )
    assert isinstance(event.created_at, datetime)


def test_list_events_returns_events_newest_first(db_conn):
    oldest = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newest = datetime(2026, 1, 3, tzinfo=timezone.utc)

    insert_event_at(db_conn, external_id="Fermi:oldest", created_at=oldest)
    insert_event_at(db_conn, external_id="Fermi:newest-a", created_at=newest)
    insert_event_at(db_conn, external_id="Fermi:newest-b", created_at=newest)

    with db_conn.cursor() as cur:
        events = list_events(cur)

    assert [event.external_id for event in events] == [
        "Fermi:newest-b",
        "Fermi:newest-a",
        "Fermi:oldest",
    ]


def test_list_events_filters_tstart_inclusively(db_conn):
    tstart = datetime(2026, 1, 2, tzinfo=timezone.utc)
    insert_event_at(db_conn, external_id="Fermi:before", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    insert_event_at(db_conn, external_id="Fermi:at-start", created_at=tstart)

    with db_conn.cursor() as cur:
        events = list_events(cur, tstart=tstart)

    assert [event.external_id for event in events] == ["Fermi:at-start"]


def test_list_events_filters_tstop_exclusively(db_conn):
    tstop = datetime(2026, 1, 2, tzinfo=timezone.utc)
    insert_event_at(db_conn, external_id="Fermi:before-stop", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    insert_event_at(db_conn, external_id="Fermi:at-stop", created_at=tstop)

    with db_conn.cursor() as cur:
        events = list_events(cur, tstop=tstop)

    assert [event.external_id for event in events] == ["Fermi:before-stop"]


def test_list_events_filters_half_open_interval(db_conn):
    tstart = datetime(2026, 1, 2, tzinfo=timezone.utc)
    tstop = datetime(2026, 1, 4, tzinfo=timezone.utc)

    insert_event_at(db_conn, external_id="Fermi:before", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    insert_event_at(db_conn, external_id="Fermi:start", created_at=tstart)
    insert_event_at(db_conn, external_id="Fermi:middle", created_at=datetime(2026, 1, 3, tzinfo=timezone.utc))
    insert_event_at(db_conn, external_id="Fermi:stop", created_at=tstop)

    with db_conn.cursor() as cur:
        events = list_events(cur, tstart=tstart, tstop=tstop)

    assert [event.external_id for event in events] == ["Fermi:middle", "Fermi:start"]


def test_get_event_notices_returns_full_rows_in_published_order(db_conn):
    earlier = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = datetime(2026, 1, 2, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:timeline-notices")
        later_id = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#timeline-notices-later",
            topic="gcn.classic.voevent.FERMI_GBM_ALERT",
            kafka_partition=1,
            kafka_offset=2,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=later,
            burst_datetime=later,
            raw_uri="file:///tmp/timeline-notices-later.xml",
        )
        earlier_id = insert_notice_json(
            cur,
            event_id=event_id,
            topic="gcn.notices.einstein_probe.wxt.alert",
            kafka_partition=1,
            kafka_offset=1,
            mission="Einstein Probe",
            instrument="WXT",
            is_retraction=False,
            published_at=earlier,
            burst_datetime=earlier,
            raw_uri="file:///tmp/timeline-notices-earlier.json",
            ra=1,
            dec=2,
            err_radius=0.1,
        )

        notices = get_event_notices(cur, event_id)

    assert [notice.id for notice in notices] == [earlier_id, later_id]
    assert notices[0].event_id == event_id
    assert notices[0].format == "json"
    assert notices[0].raw_uri == "file:///tmp/timeline-notices-earlier.json"


def test_get_event_conesearches_returns_full_rows_in_subject_order(db_conn):
    earlier = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = datetime(2026, 1, 2, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:timeline-conesearches")
        later_job_id = insert_job(cur, event_id=event_id, subject_time_start=later)
        later_id = insert_conesearch(
            cur,
            event_id=event_id,
            job_id=later_job_id,
            broker="fink",
            survey="ztf",
            subject_time_start=later,
            subject_time_end=later + timedelta(hours=1),
            queried_at=later + timedelta(minutes=5),
            ra=1,
            dec=2,
            radius_arcsec=3,
            alert_count=0,
            result_uri=None,
        )
        earlier_job_id = insert_job(cur, event_id=event_id, subject_time_start=earlier)
        earlier_id = insert_conesearch(
            cur,
            event_id=event_id,
            job_id=earlier_job_id,
            broker="fink",
            survey="ztf",
            subject_time_start=earlier,
            subject_time_end=earlier + timedelta(hours=1),
            queried_at=earlier + timedelta(minutes=5),
            ra=4,
            dec=5,
            radius_arcsec=6,
            alert_count=1,
            result_uri="file:///tmp/timeline-conesearches-earlier.json",
        )

        conesearches = get_event_conesearches(cur, event_id)

    assert [conesearch.id for conesearch in conesearches] == [earlier_id, later_id]
    assert conesearches[0].event_id == event_id
    assert conesearches[0].job_id == earlier_job_id
    assert conesearches[0].result_uri == "file:///tmp/timeline-conesearches-earlier.json"


def test_insert_notice_voevent_is_idempotent_by_kafka_coordinates(db_conn):
    published_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:kafka-idempotent")
        first = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#kafka-idempotent",
            topic="gcn.classic.voevent.FERMI_GBM_ALERT",
            kafka_partition=7,
            kafka_offset=42,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/kafka-idempotent.xml",
        )
        second = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#kafka-idempotent",
            topic="gcn.classic.voevent.FERMI_GBM_ALERT",
            kafka_partition=7,
            kafka_offset=42,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/kafka-idempotent.xml",
        )
        cur.execute("SELECT count(*) FROM notices")
        notice_count = cur.fetchone()[0]

    assert second == first
    assert notice_count == 1


def test_insert_notice_json_is_idempotent_by_kafka_coordinates(db_conn):
    published_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Einstein Probe:json-idempotent")
        first = insert_notice_json(
            cur,
            event_id=event_id,
            topic="gcn.notices.einstein_probe.wxt.alert",
            kafka_partition=7,
            kafka_offset=42,
            mission="Einstein Probe",
            instrument="WXT",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/json-idempotent.json",
            ra=1,
            dec=2,
            err_radius=0.1,
        )
        second = insert_notice_json(
            cur,
            event_id=event_id,
            topic="gcn.notices.einstein_probe.wxt.alert",
            kafka_partition=7,
            kafka_offset=42,
            mission="Einstein Probe",
            instrument="WXT",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/json-idempotent.json",
            ra=1,
            dec=2,
            err_radius=0.1,
        )
        cur.execute("SELECT count(*) FROM notices")
        notice_count = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM notice_voevents")
        voevent_count = cur.fetchone()[0]

    assert second == first
    assert notice_count == 1
    assert voevent_count == 0


def test_insert_notice_voevent_rejects_duplicate_ivorn_at_different_kafka_coordinates(db_conn):
    published_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:duplicate-ivorn")
        insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#duplicate-ivorn",
            topic="gcn.classic.voevent.FERMI_GBM_ALERT",
            kafka_partition=7,
            kafka_offset=42,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/duplicate-ivorn-first.xml",
        )

        with pytest.raises(psycopg.errors.UniqueViolation):
            insert_notice_voevent(
                cur,
                event_id=event_id,
                ivorn="ivo://nasa.gsfc.gcn/Fermi#duplicate-ivorn",
                topic="gcn.classic.voevent.FERMI_GBM_ALERT",
                kafka_partition=7,
                kafka_offset=43,
                mission="Fermi",
                instrument="GBM",
                is_retraction=False,
                published_at=published_at,
                burst_datetime=published_at,
                raw_uri="file:///tmp/duplicate-ivorn-second.xml",
            )

    db_conn.rollback()


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
        event_id = insert_event(cur, external_id="SVOM:retracted-before-cutoff")
        insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://org.svom/fsc#retracted-before-cutoff_slewing",
            topic="gcn.notices.svom.voevent.eclairs",
            kafka_partition=1,
            kafka_offset=1,
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
        retraction_id = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://org.svom/fsc#retracted-before-cutoff_retraction",
            topic="gcn.notices.svom.voevent.eclairs",
            kafka_partition=1,
            kafka_offset=2,
            mission="SVOM",
            instrument="ECLAIRs",
            is_retraction=True,
            published_at=retracted_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/retracted-before-cutoff_retraction.xml",
        )
        mark_retracted_notices(
            cur,
            event_id=event_id,
            retraction_notice_id=retraction_id,
            target_ivorns=("ivo://org.svom/fsc#retracted-before-cutoff_slewing",),
        )

        localization = find_best_localization(cur, event_id, cutoff_at=cutoff_at)

    assert localization is None


def test_find_best_localization_keeps_localization_retracted_after_cutoff(db_conn):
    published_at = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    cutoff_at = published_at + timedelta(minutes=10)
    retracted_at = published_at + timedelta(minutes=20)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="SVOM:retracted-after-cutoff")
        insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://org.svom/fsc#retracted-after-cutoff_slewing",
            topic="gcn.notices.svom.voevent.eclairs",
            kafka_partition=1,
            kafka_offset=3,
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
        retraction_id = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://org.svom/fsc#retracted-after-cutoff_retraction",
            topic="gcn.notices.svom.voevent.eclairs",
            kafka_partition=1,
            kafka_offset=4,
            mission="SVOM",
            instrument="ECLAIRs",
            is_retraction=True,
            published_at=retracted_at,
            burst_datetime=published_at,
            raw_uri="file:///tmp/retracted-after-cutoff_retraction.xml",
        )
        mark_retracted_notices(
            cur,
            event_id=event_id,
            retraction_notice_id=retraction_id,
            target_ivorns=("ivo://org.svom/fsc#retracted-after-cutoff_slewing",),
        )

        localization = find_best_localization(cur, event_id, cutoff_at=cutoff_at)

    assert localization == Localization(ra=1, dec=2, err_radius=0.1)
