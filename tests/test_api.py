from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from conftest import fixture_paths
from conftest import fixture_topic
from fastapi.testclient import TestClient

from starhunt.api import app
from starhunt.api.app import get_db_conn
from starhunt.db import insert_conesearch
from starhunt.db import insert_event
from starhunt.db import insert_notice_json
from starhunt.db import insert_notice_voevent

CONESEARCH_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "conesearches" / "sample.json"


def fixture_for_topic(topic: str):
    return next(path for path in fixture_paths() if fixture_topic(path) == topic)


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


def test_events_returns_bare_array_oldest_first(db_conn):
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
            "id": older_id,
            "external_id": "Fermi:older",
            "created_at": "2026-01-01T00:00:00Z",
            "last_updated": None,
            "notice_count": 0,
            "conesearch_count": 0,
            "latest_burst_datetime": None,
            "latest_localization": None,
        },
        {
            "id": newer_id,
            "external_id": "Fermi:newer",
            "created_at": "2026-01-02T00:00:00Z",
            "last_updated": None,
            "notice_count": 0,
            "conesearch_count": 0,
            "latest_burst_datetime": None,
            "latest_localization": None,
        },
    ]


def test_events_returns_summary_fields(db_conn):
    event_created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    notice_time = datetime(2026, 1, 2, tzinfo=timezone.utc)
    conesearch_time = datetime(2026, 1, 3, tzinfo=timezone.utc)
    event_id = insert_event_at(db_conn, external_id="Fermi:summary-api", created_at=event_created_at)

    with db_conn.cursor() as cur:
        insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#summary-api",
            topic="gcn.classic.voevent.FERMI_GBM_FLT_POS",
            kafka_partition=1,
            kafka_offset=1,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=notice_time,
            burst_datetime=notice_time - timedelta(minutes=1),
            raw_uri="file:///tmp/summary-api.xml",
            ra=1,
            dec=2,
            err_radius=0.1,
        )
        job_id = insert_job(cur, event_id=event_id, subject_time_start=conesearch_time)
        insert_conesearch(
            cur,
            event_id=event_id,
            job_id=job_id,
            broker="fink",
            survey="ztf",
            subject_time_start=conesearch_time,
            subject_time_end=conesearch_time + timedelta(hours=1),
            queried_at=conesearch_time,
            ra=1,
            dec=2,
            radius=3,
            alert_count=1,
            result_uri="file:///tmp/summary-api.json",
        )

    with client_for(db_conn) as client:
        response = client.get("/events")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": event_id,
            "external_id": "Fermi:summary-api",
            "created_at": "2026-01-01T00:00:00Z",
            "last_updated": "2026-01-03T00:00:00Z",
            "notice_count": 1,
            "conesearch_count": 1,
            "latest_burst_datetime": "2026-01-01T23:59:00Z",
            "latest_localization": {
                "ra": 1.0,
                "dec": 2.0,
                "err_radius": 0.1,
                "units": "degrees",
            },
        }
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
            "id": start_id,
            "external_id": "Fermi:start",
            "created_at": "2026-01-02T00:00:00Z",
            "last_updated": None,
            "notice_count": 0,
            "conesearch_count": 0,
            "latest_burst_datetime": None,
            "latest_localization": None,
        },
        {
            "id": middle_id,
            "external_id": "Fermi:middle",
            "created_at": "2026-01-03T00:00:00Z",
            "last_updated": None,
            "notice_count": 0,
            "conesearch_count": 0,
            "latest_burst_datetime": None,
            "latest_localization": None,
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
        response = client.get("/timeline/999")

    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Event not found"}


def test_timeline_rejects_external_event_id(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/timeline/Fermi:missing")

    app.dependency_overrides.clear()

    assert response.status_code == 422


def test_timeline_returns_empty_array_for_event_without_milestones(db_conn):
    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:empty-timeline")

    with client_for(db_conn) as client:
        response = client.get(f"/timeline/{event_id}")

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
            radius=3,
            alert_count=1,
            result_uri="file:///tmp/timeline-conesearch.json",
        )

    with client_for(db_conn) as client:
        response = client.get(f"/timeline/{event_id}")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    milestones = response.json()
    assert [milestone["type"] for milestone in milestones] == ["conesearch", "notice"]
    assert [milestone["time"] for milestone in milestones] == [
        "2026-01-01T00:05:00Z",
        "2026-01-02T00:00:00Z",
    ]
    conesearch_content = milestones[0]["content"]
    conesearch_created_at = conesearch_content.pop("created_at")
    assert conesearch_created_at is not None
    assert conesearch_content == {
        "id": conesearch_id,
        "event_id": event_id,
        "broker": "fink",
        "survey": "ztf",
        "subject_time_start": "2026-01-01T00:00:00Z",
        "subject_time_end": "2026-01-01T01:00:00Z",
        "queried_at": "2026-01-01T00:05:00Z",
            "search_region": {
                "ra": 1.0,
                "dec": 2.0,
                "err_radius": 3,
                "units": "degrees",
            },
        "alert_count": 1,
    }
    assert "job_id" not in milestones[0]["content"]
    assert "result_uri" not in milestones[0]["content"]
    assert "radius" not in milestones[0]["content"]

    notice_content = milestones[1]["content"]
    notice_created_at = notice_content.pop("created_at")
    assert notice_created_at is not None
    assert notice_content == {
        "id": notice_id,
        "event_id": event_id,
        "format": "voevent",
        "topic": "gcn.classic.voevent.FERMI_GBM_ALERT",
        "mission": "Fermi",
        "instrument": "GBM",
        "published_at": "2026-01-02T00:00:00Z",
        "burst_datetime": "2026-01-02T00:00:00Z",
        "localization": None,
        "is_retraction": False,
        "retracted_by": None,
    }
    assert "kafka_partition" not in milestones[1]["content"]
    assert "kafka_offset" not in milestones[1]["content"]
    assert "raw_uri" not in milestones[1]["content"]
    assert "ivorn" not in milestones[1]["content"]


def test_openapi_documents_timeline_event_id_as_primary_key():
    app.openapi_schema = None
    parameter = app.openapi()["paths"]["/timeline/{event_id}"]["get"]["parameters"][0]

    assert parameter["name"] == "event_id"
    assert parameter["description"] == "Event primary key."
    assert parameter["schema"]["type"] == "integer"


def test_notice_returns_metadata_and_parsed_voevent_payload(db_conn, tmp_path):
    topic = "gcn.classic.voevent.FERMI_GBM_ALERT"
    fixture = fixture_for_topic(topic)
    raw_path = tmp_path / fixture.name
    raw_path.write_bytes(fixture.read_bytes())
    published_at = datetime(2026, 1, 3, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:notice-voevent")
        notice_id = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#notice-voevent",
            topic=topic,
            kafka_partition=1,
            kafka_offset=10,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri=raw_path.resolve().as_uri(),
        )

    with client_for(db_conn) as client:
        response = client.get(f"/notice/{notice_id}")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    created_at = body["metadata"].pop("created_at")
    assert created_at is not None
    assert body["metadata"] == {
        "id": notice_id,
        "event_id": event_id,
        "format": "voevent",
        "topic": topic,
        "mission": "Fermi",
        "instrument": "GBM",
        "published_at": "2026-01-03T00:00:00Z",
        "burst_datetime": "2026-01-03T00:00:00Z",
        "localization": None,
        "is_retraction": False,
        "retracted_by": None,
    }
    assert "kafka_partition" not in body["metadata"]
    assert "kafka_offset" not in body["metadata"]
    assert "raw_uri" not in body["metadata"]
    assert body["payload"]["ivorn"].startswith("ivo://nasa.gsfc.gcn/Fermi#")
    assert body["payload"]["trig_id"] is not None


def test_notice_returns_metadata_and_parsed_json_payload(db_conn, tmp_path):
    topic = "gcn.notices.einstein_probe.wxt.alert"
    fixture = fixture_for_topic(topic)
    raw_path = tmp_path / fixture.name
    raw_path.write_bytes(fixture.read_bytes())
    published_at = datetime(2026, 1, 4, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Einstein Probe:notice-json")
        notice_id = insert_notice_json(
            cur,
            event_id=event_id,
            topic=topic,
            kafka_partition=1,
            kafka_offset=11,
            mission="Einstein Probe",
            instrument="WXT",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri=raw_path.resolve().as_uri(),
        )

    with client_for(db_conn) as client:
        response = client.get(f"/notice/{notice_id}")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    created_at = body["metadata"].pop("created_at")
    assert created_at is not None
    assert body["metadata"] == {
        "id": notice_id,
        "event_id": event_id,
        "format": "json",
        "topic": topic,
        "mission": "Einstein Probe",
        "instrument": "WXT",
        "published_at": "2026-01-04T00:00:00Z",
        "burst_datetime": "2026-01-04T00:00:00Z",
        "localization": None,
        "is_retraction": False,
        "retracted_by": None,
    }
    assert "kafka_partition" not in body["metadata"]
    assert "kafka_offset" not in body["metadata"]
    assert "raw_uri" not in body["metadata"]
    assert body["payload"]["instrument"] == "WXT"
    assert len(body["payload"]["id"]) == 1


def test_notice_returns_404_for_unknown_notice(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/notice/999")

    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Notice not found"}


def test_notice_returns_500_when_payload_file_is_missing(db_conn, tmp_path):
    published_at = datetime(2026, 1, 5, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:notice-missing-file")
        notice_id = insert_notice_voevent(
            cur,
            event_id=event_id,
            ivorn="ivo://nasa.gsfc.gcn/Fermi#notice-missing-file",
            topic="gcn.classic.voevent.FERMI_GBM_ALERT",
            kafka_partition=1,
            kafka_offset=12,
            mission="Fermi",
            instrument="GBM",
            is_retraction=False,
            published_at=published_at,
            burst_datetime=published_at,
            raw_uri=(tmp_path / "missing.xml").resolve().as_uri(),
        )

    with client_for(db_conn) as client:
        response = client.get(f"/notice/{notice_id}")

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json() == {"detail": "Notice payload file not found"}


def test_conesearch_returns_metadata_and_parsed_result_payload(db_conn, tmp_path):
    result_path = tmp_path / CONESEARCH_FIXTURE.name
    result_path.write_bytes(CONESEARCH_FIXTURE.read_bytes())
    subject_time_start = datetime(2026, 1, 6, tzinfo=timezone.utc)
    queried_at = subject_time_start + timedelta(minutes=5)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:conesearch-result")
        job_id = insert_job(cur, event_id=event_id, subject_time_start=subject_time_start)
        conesearch_id = insert_conesearch(
            cur,
            event_id=event_id,
            job_id=job_id,
            broker="fink",
            survey="ztf",
            subject_time_start=subject_time_start,
            subject_time_end=subject_time_start + timedelta(hours=1),
            queried_at=queried_at,
            ra=193.821,
            dec=2.897,
            radius=0.05,
            alert_count=1,
            result_uri=result_path.resolve().as_uri(),
        )

    with client_for(db_conn) as client:
        response = client.get(f"/conesearch/{conesearch_id}")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    created_at = body["metadata"].pop("created_at")
    assert created_at is not None
    assert body["metadata"] == {
        "id": conesearch_id,
        "event_id": event_id,
        "broker": "fink",
        "survey": "ztf",
        "subject_time_start": "2026-01-06T00:00:00Z",
        "subject_time_end": "2026-01-06T01:00:00Z",
        "queried_at": "2026-01-06T00:05:00Z",
        "search_region": {
            "ra": 193.821,
            "dec": 2.897,
            "err_radius": 0.05,
            "units": "degrees",
        },
        "alert_count": 1,
    }
    assert "job_id" not in body["metadata"]
    assert "result_uri" not in body["metadata"]
    assert "radius" not in body["metadata"]
    assert body["payload"][0]["i:objectId"] == "ZTF21abfmbix"


def test_conesearch_returns_empty_payload_for_zero_alert_search(db_conn):
    subject_time_start = datetime(2026, 1, 7, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:conesearch-empty")
        job_id = insert_job(cur, event_id=event_id, subject_time_start=subject_time_start)
        conesearch_id = insert_conesearch(
            cur,
            event_id=event_id,
            job_id=job_id,
            broker="fink",
            survey="ztf",
            subject_time_start=subject_time_start,
            subject_time_end=subject_time_start + timedelta(hours=1),
            queried_at=subject_time_start + timedelta(minutes=5),
            ra=10,
            dec=20,
            radius=0.25,
            alert_count=0,
            result_uri=None,
        )

    with client_for(db_conn) as client:
        response = client.get(f"/conesearch/{conesearch_id}")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["metadata"]["search_region"] == {
        "ra": 10.0,
        "dec": 20.0,
        "err_radius": 0.25,
        "units": "degrees",
    }
    assert body["payload"] == []


def test_conesearch_returns_404_for_unknown_conesearch(db_conn):
    with client_for(db_conn) as client:
        response = client.get("/conesearch/999")

    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Conesearch not found"}


def test_conesearch_returns_500_when_result_file_is_missing(db_conn, tmp_path):
    subject_time_start = datetime(2026, 1, 8, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        event_id = insert_event(cur, external_id="Fermi:conesearch-missing-file")
        job_id = insert_job(cur, event_id=event_id, subject_time_start=subject_time_start)
        conesearch_id = insert_conesearch(
            cur,
            event_id=event_id,
            job_id=job_id,
            broker="fink",
            survey="ztf",
            subject_time_start=subject_time_start,
            subject_time_end=subject_time_start + timedelta(hours=1),
            queried_at=subject_time_start + timedelta(minutes=5),
            ra=10,
            dec=20,
            radius=0.25,
            alert_count=1,
            result_uri=(tmp_path / "missing.json").resolve().as_uri(),
        )

    with client_for(db_conn) as client:
        response = client.get(f"/conesearch/{conesearch_id}")

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json() == {"detail": "Conesearch result file not found"}
