from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import os
from pathlib import Path
from typing import Literal

from confluent_kafka import Message
from gcn_kafka import Consumer
from gcn_parser.fermi import parse_fermi_gbm_alert
from gcn_parser.fermi import parse_fermi_gbm_fin_pos
from gcn_parser.fermi import parse_fermi_gbm_flt_pos
from gcn_parser.fermi import parse_fermi_gbm_gnd_pos
import psycopg
from psycopg import Connection

from starhunt.utils import is_tz_aware


DEFAULT_CONESEARCH_OFFSET = timedelta(hours=12)
DEFAULT_CONESEARCH_PERIOD = timedelta(hours=6)
DEFAULT_CONESEARCH_TOTAL = 24


@dataclass
class Topic:
    topic: str
    suffix: str
    parser: callable


TOPICS = [
    Topic("gcn.classic.voevent.FERMI_GBM_ALERT", "xml", parse_fermi_gbm_alert),
    Topic("gcn.classic.voevent.FERMI_GBM_FIN_POS", "xml", parse_fermi_gbm_fin_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_FLT_POS", "xml", parse_fermi_gbm_flt_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_GND_POS", "xml", parse_fermi_gbm_gnd_pos),
]

SUFFIXES = {t.topic: t.suffix for t in TOPICS}
PARSERS = {t.topic: t.parser for t in TOPICS}
ZTF_CONESEARCH_JOB_TYPE = "ztf_conesearch"


def init_consumer(
    offset: Literal["earliest", "latest"],
    group_id: str | None,
) -> Consumer:
    """Create the GCN Kafka consumer from environment credentials."""
    return Consumer(
        client_id=os.environ["GCN_CLIENT_ID"],
        client_secret=os.environ["GCN_CLIENT_SECRET"],
        config={
            "auto.offset.reset": offset,
            "enable.auto.commit": False,
        }
        | (
            {
                "group.id": group_id,
            }
            if group_id is not None
            else {}
        ),
    )


def init_db_conn() -> Connection:
    """Create DB connection from environment."""
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def write_message(
    message: Message,
    outdir: Path,
):
    """Writes a Kafka message to disk."""
    topic = message.topic()
    filepath = outdir / f"{topic}_{message.partition()}_{message.offset()}.{SUFFIXES[topic]}"
    filepath.write_bytes(message.value())
    return filepath


@dataclass
class EventInfo:
    event_id: int
    is_new: bool


def get_or_create_event(cursor, external_id: str, mission: str, instrument: str) -> EventInfo:
    """Return an event id, inserting the event when absent."""
    cursor.execute(
        """
        INSERT INTO events (external_id, mission, instrument)
        VALUES (%s, %s, %s)
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
        """,
        (external_id, mission, instrument),
    )
    row = cursor.fetchone()
    if row is not None:
        return EventInfo(event_id=row[0], is_new=True)

    cursor.execute(
        """
        SELECT id FROM events WHERE external_id = %s
        """,
        (external_id,),
    )
    return EventInfo(event_id=cursor.fetchone()[0], is_new=False)


def get_or_create_milestone(
    cursor,
    event_id: int,
    external_id: str,
    subtype: str,
    published_at: datetime,
    subject_time_start: datetime,
    subject_time_end: datetime,
) -> int:
    """Return a milestone id, inserting the milestone when absent."""
    cursor.execute(
        """
        INSERT INTO milestones (
            event_id,
            external_id,
            milestone_type,
            milestone_subtype,
            published_at,
            subject_time_start,
            subject_time_end
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
        """,
        (event_id, external_id, "notice", subtype, published_at, subject_time_start, subject_time_end),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        SELECT id FROM milestones WHERE external_id = %s
        """,
        (external_id,),
    )
    return cursor.fetchone()[0]


def insert_artifact(cursor, milestone_id: int, uri: str):
    """Record an artifact URI for a milestone."""
    cursor.execute(
        """
        INSERT INTO artifacts (milestone_id, artifact_type, uri)
        VALUES (%s, %s, %s)
        ON CONFLICT (milestone_id, artifact_type, uri)
        DO NOTHING
        """,
        (milestone_id, "gcn.voevent", uri),
    )


def schedule_ztf_conesearch(
    cursor,
    event_id: int,
    burst_datetime: datetime,
    offset: timedelta,
    period: timedelta,
    total: int,
):
    """
    Schedules ZTF conesearch for workers to execute.

    Conesearch campaign covers the time interval [burst_datetime, burst_datetime + total * period)
    with `n=total` intervals each of duration `period`. The conesearch are scheduled with an offset
    from the conesearch stopdate equal to `offset` to give some time for alerts to be ingested and
    distributed by the broker.


    Args:
        cursor: a database cursor
        event_id: the event id, it will be used by workers to query best localization at runtime
        burst_datetime: the conesearch campaign startdate.
        offset: the delay between stopdate and scheduled execution
        period: the interval between conesearch stopdate and startdate
        total: the number of conesearch to schedule
    """
    if not is_tz_aware(burst_datetime):
        raise ValueError("burst_datetime must be timezone-aware")
    if period <= timedelta(0):
        raise ValueError("period must be positive")
    if offset < timedelta(0):
        raise ValueError("offset must be non-negative")
    if total <= 0:
        raise ValueError("total must be positive")

    for index in range(total):
        subject_time_start = burst_datetime + index * period
        subject_time_end = subject_time_start + period
        scheduled_at = subject_time_end + offset
        cursor.execute(
            """
            INSERT INTO jobs (
                event_id,
                job_type,
                subject_time_start,
                subject_time_end,
                scheduled_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id, job_type, subject_time_start, subject_time_end)
            DO NOTHING
            """,
            (
                event_id,
                ZTF_CONESEARCH_JOB_TYPE,
                subject_time_start,
                subject_time_end,
                scheduled_at,
            ),
        )


def insert_message(
    message: Message,
    filepath: Path,
    db_conn: Connection,
):
    """Atomically records message and jobs to database."""
    notice = PARSERS[message.topic()](message.value())
    # TODO: mission name is hardcoded here, remember to change this once we add support for other missions
    mission, instrument = "Fermi", "GBM"
    event_external_id = f"{mission}.{instrument}:{notice.trig_id}"
    artifact_uri = filepath.resolve().as_uri()

    try:
        with db_conn.cursor() as cursor:
            event_info = get_or_create_event(
                cursor,
                external_id=event_external_id,
                mission=mission,
                instrument=instrument,
            )
            if event_info.is_new:
                schedule_ztf_conesearch(
                    cursor,
                    event_id=event_info.event_id,
                    burst_datetime=notice.burst_datetime,
                    offset=DEFAULT_CONESEARCH_OFFSET,
                    period=DEFAULT_CONESEARCH_PERIOD,
                    total=DEFAULT_CONESEARCH_TOTAL,
                )
            milestone_id = get_or_create_milestone(
                cursor,
                event_id=event_info.event_id,
                external_id=notice.ivorn,
                subtype=message.topic(),
                published_at=notice.alert_datetime,
                subject_time_start=notice.burst_datetime,
                subject_time_end=notice.burst_datetime,
            )
            insert_artifact(
                cursor,
                milestone_id=milestone_id,
                uri=artifact_uri,
            )

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
