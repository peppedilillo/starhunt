from datetime import datetime
from datetime import timedelta
from datetime import timezone

from fastapi.testclient import TestClient

from starhunt.api import app
from starhunt.api import get_db_conn
from starhunt.db import insert_conesearch
from starhunt.db import insert_event
from starhunt.db import insert_notice_voevent


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


def client_for(conn) -> TestClient:
    app.dependency_overrides[get_db_conn] = lambda: conn
    return TestClient(app)


def test_events_returns_empty_array(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/events")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == []


def test_events_returns_bare_array_newest_first(db_conn):
    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 2, tzinfo=timezone.utc)
    older_id = insert_event_at(db_conn, external_id="Fermi:older", created_at=older)
    newer_id = insert_event_at(db_conn, external_id="Fermi:newer", created_at=newer)

    with client_for(db_conn) as client:
        response = client.get("/events")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": newer_id,
            "external_id": "Fermi:newer",
            "created_at": "2026-01-02T00:00:00Z",
        },
        {
            "id": older_id,
            "external_id": "Fermi:older",
            "created_at": "2026-01-01T00:00:00Z",
        },
    ]


def test_events_filters_created_at_interval(db_conn):
    insert_event_at(db_conn, external_id="Fermi:before", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    start_id = insert_event_at(db_conn, external_id="Fermi:start", created_at=datetime(2026, 1, 2, tzinfo=timezone.utc))
    middle_id = insert_event_at(
        db_conn, external_id="Fermi:middle", created_at=datetime(2026, 1, 3, tzinfo=timezone.utc)
    )
    insert_event_at(db_conn, external_id="Fermi:stop", created_at=datetime(2026, 1, 4, tzinfo=timezone.utc))

    with client_for(db_conn) as client:
        response = client.get(
            "/events",
            params={
                "tstart": "2026-01-02T00:00:00Z",
                "tstop": "2026-01-04T00:00:00Z",
            },
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": middle_id,
            "external_id": "Fermi:middle",
            "created_at": "2026-01-03T00:00:00Z",
        },
        {
            "id": start_id,
            "external_id": "Fermi:start",
            "created_at": "2026-01-02T00:00:00Z",
        },
    ]


def test_events_rejects_invalid_datetime(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/events", params={"tstart": "not-a-datetime"})

    app.dependency_overrides.clear()

    assert response.status_code == 422


def test_events_rejects_naive_datetime(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/events", params={"tstart": "2026-01-01T00:00:00"})

    app.dependency_overrides.clear()

    assert response.status_code == 422


def test_events_rejects_non_utc_datetime(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/events", params={"tstart": "2026-01-01T00:00:00+01:00"})

    app.dependency_overrides.clear()

    assert response.status_code == 422


def test_events_rejects_inverted_interval(db_conn):
    with client_for(db_conn) as client:
        response = client.get(
            "/events",
            params={
                "tstart": "2026-01-02T00:00:00Z",
                "tstop": "2026-01-01T00:00:00Z",
            },
        )

    app.dependency_overrides.clear()

    assert response.status_code == 422


def test_timeline_requires_event_id(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/timeline")

    app.dependency_overrides.clear()

    assert response.status_code == 404


def test_timeline_returns_404_for_unknown_event(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/timeline/Fermi:missing")

    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Event not found"}


def test_timeline_returns_empty_array_for_event_without_milestones(db_conn):
    with db_conn.cursor() as cur:
        insert_event(cur, external_id="Fermi:empty-timeline")

    with client_for(db_conn) as client:
        response = client.get("/timeline/Fermi:empty-timeline")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == []


def test_timeline_returns_notice_and_conesearch_milestones_oldest_first(db_conn):
    notice_time = datetime(2026, 1, 2, tzinfo=timezone.utc)
    conesearch_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:timeline")
        notice_id = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#timeline",
            topic="gcn.classic.voevent.FERMI_GBM_ALERT",
            kafka_partition=1,
            kafka_offset=1,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=notice_time,
            burst_datetime=notice_time,
            raw_uri="file:///tmp/timeline-notice.xml",
        )
        job_id = insert_job(cur, event_id=event_id, subject_time_start=conesearch_time)
        conesearch_id = insert_conesearch(
            cur,
            event_id=event_id,
            job_id=job_id,
            broker="fink",
            survey="ztf",
            subject_time_start=conesearch_time,
            subject_time_end=conesearch_time + timedelta(hours=1),
            queried_at=conesearch_time + timedelta(minutes=5),
            ra=1,
            dec=2,
            radius_arcsec=3,
            alert_count=1,
            result_uri="file:///tmp/timeline-conesearch.json",
        )

    with client_for(db_conn) as client:
        response = client.get("/timeline/Fermi:timeline")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    milestones = response.json()
    assert [milestone["type"] for milestone in milestones] == ["conesearch", "notice"]
    assert [milestone["time"] for milestone in milestones] == [
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
    ]
    assert milestones[0]["content"]["id"] == conesearch_id
    assert milestones[0]["content"]["event_id"] == event_id
    assert milestones[0]["content"]["job_id"] == job_id
    assert milestones[0]["content"]["result_uri"] == "file:///tmp/timeline-conesearch.json"
    assert milestones[1]["content"]["id"] == notice_id
    assert milestones[1]["content"]["event_id"] == event_id
    assert milestones[1]["content"]["format"] == "voevent"
    assert milestones[1]["content"]["raw_uri"] == "file:///tmp/timeline-notice.xml"
    assert "ivorn" not in milestones[1]["content"]
