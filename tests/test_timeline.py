from datetime import datetime
from datetime import timedelta
from datetime import timezone

from starhunt.db import RowConesearch
from starhunt.db import RowNotice
from starhunt.timeline import build_event_milestones


def make_notice(*, notice_id: int, published_at: datetime) -> RowNotice:
    return RowNotice(
        id=notice_id,
        event_id=1,
        format="voevent",
        topic="gcn.classic.voevent.FERMI_GBM_ALERT",
        kafka_partition=0,
        kafka_offset=notice_id,
        mission="Fermi",
        instrument="GBM",
        published_at=published_at,
        burst_datetime=published_at,
        ra=None,
        dec=None,
        err_radius=None,
        raw_uri=f"file:///tmp/notice-{notice_id}.xml",
        is_retraction=False,
        retracted_by=None,
        created_at=published_at,
    )


def make_conesearch(*, conesearch_id: int, subject_time_start: datetime) -> RowConesearch:
    return RowConesearch(
        id=conesearch_id,
        event_id=1,
        job_id=conesearch_id,
        broker="fink",
        survey="ztf",
        subject_time_start=subject_time_start,
        subject_time_end=subject_time_start + timedelta(hours=1),
        queried_at=subject_time_start + timedelta(minutes=5),
        ra=1,
        dec=2,
        radius_arcsec=3,
        alert_count=0,
        result_uri=None,
        created_at=subject_time_start,
    )


def test_build_event_milestones_returns_empty_list():
    assert build_event_milestones([], []) == []


def test_build_event_milestones_merges_oldest_first():
    earliest = datetime(2026, 1, 1, tzinfo=timezone.utc)
    middle = datetime(2026, 1, 2, tzinfo=timezone.utc)
    latest = datetime(2026, 1, 3, tzinfo=timezone.utc)

    milestones = build_event_milestones(
        notices=[
            make_notice(notice_id=2, published_at=latest),
            make_notice(notice_id=1, published_at=middle),
        ],
        conesearches=[
            make_conesearch(conesearch_id=1, subject_time_start=earliest),
        ],
    )

    assert [(milestone.type, milestone.time, milestone.content.id) for milestone in milestones] == [
        ("conesearch", earliest, 1),
        ("notice", middle, 1),
        ("notice", latest, 2),
    ]


def test_build_event_milestones_orders_ties_by_type_then_source_id():
    tied = datetime(2026, 1, 1, tzinfo=timezone.utc)

    milestones = build_event_milestones(
        notices=[
            make_notice(notice_id=2, published_at=tied),
            make_notice(notice_id=1, published_at=tied),
        ],
        conesearches=[
            make_conesearch(conesearch_id=2, subject_time_start=tied),
            make_conesearch(conesearch_id=1, subject_time_start=tied),
        ],
    )

    assert [(milestone.type, milestone.content.id) for milestone in milestones] == [
        ("notice", 1),
        ("notice", 2),
        ("conesearch", 1),
        ("conesearch", 2),
    ]
