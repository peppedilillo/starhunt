from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import os
from pathlib import Path
from typing import Callable, Literal

from confluent_kafka import Message
from gcn_kafka import Consumer
from gcn_parser import Notice
from gcn_parser.fermi import parse_fermi_gbm_alert
from gcn_parser.fermi import parse_fermi_gbm_fin_pos
from gcn_parser.fermi import parse_fermi_gbm_flt_pos
from gcn_parser.fermi import parse_fermi_gbm_gnd_pos
from psycopg import Connection

from .db import get_or_create_event
from .db import get_or_create_milestone
from .db import insert_artifact
from .db import Localization
from .utils import is_tz_aware

DEFAULT_CONESEARCH_OFFSET = timedelta(hours=12)
DEFAULT_CONESEARCH_PERIOD = timedelta(hours=6)
DEFAULT_CONESEARCH_TOTAL = 24


@dataclass
class Topic:
    """Kafka topic configuration.

    Attributes:
        topic: Kafka topic name.
        suffix: File suffix used when persisting messages from the topic.
        parser: Callable that parses message bytes into a notice object.
    """

    topic: str
    suffix: str
    parser: Callable


TOPICS = [
    Topic("gcn.classic.voevent.FERMI_GBM_ALERT", "xml", parse_fermi_gbm_alert),
    Topic("gcn.classic.voevent.FERMI_GBM_FIN_POS", "xml", parse_fermi_gbm_fin_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_FLT_POS", "xml", parse_fermi_gbm_flt_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_GND_POS", "xml", parse_fermi_gbm_gnd_pos),
]

SUFFIXES = {t.topic: t.suffix for t in TOPICS}
PARSERS = {t.topic: t.parser for t in TOPICS}
ZTF_CONESEARCH_JOB_TYPE = "ztf_fink_conesearch"


def init_consumer(
    offset: Literal["earliest", "latest"],
    group_id: str | None,
) -> Consumer:
    """Create the GCN Kafka consumer from environment credentials.

    Args:
        offset: Initial offset policy for partitions without committed offsets.
        group_id: Optional Kafka consumer group identifier.

    Returns:
        A configured GCN Kafka consumer.
    """
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


def write_message(
    message: Message,
    outdir: Path,
):
    """Write a Kafka message to disk.

    Args:
        message: Kafka message to persist.
        outdir: Directory where the message file should be written.

    Returns:
        Path to the written message file.
    """
    topic = message.topic()
    filepath = outdir / f"{topic}_{message.partition()}_{message.offset()}.{SUFFIXES[topic]}"
    filepath.write_bytes(message.value())
    return filepath


def notice_localization(subtype: str, notice: Notice) -> Localization | None:
    """Extract localization coordinates from a parsed notice.

    Args:
        subtype: GCN topic name for the notice.
        notice: Parsed GCN notice.

    Returns:
        A Localization for position notices, else None for alert notices.

    Raises:
        ValueError: If the notice subtype is unsupported.
    """
    match subtype:
        case "gcn.classic.voevent.FERMI_GBM_ALERT":
            return None
        case (
            "gcn.classic.voevent.FERMI_GBM_FIN_POS"
            | "gcn.classic.voevent.FERMI_GBM_FLT_POS"
            | "gcn.classic.voevent.FERMI_GBM_GND_POS"
        ):
            return Localization(
                ra=notice.ra,
                dec=notice.dec,
                err_radius=notice.error_radius,
            )
        case _:
            raise ValueError(f"Unsupported milestone subtype: {subtype}")


def schedule_ztf_conesearch(
    cursor,
    event_id: int,
    burst_datetime: datetime,
    offset: timedelta,
    period: timedelta,
    total: int,
):
    """Schedule ZTF conesearch jobs for workers to execute.

    The campaign covers ``[burst_datetime, burst_datetime + total * period)``
    with ``total`` windows, each lasting ``period``. Jobs are scheduled after
    each window by ``offset`` to allow broker ingestion and distribution. The
    stable ``scheduled_at`` cutoff and initial ``run_after`` eligibility are
    identical at creation; retries only move ``run_after``.

    Args:
        cursor: Database cursor.
        event_id: Event primary key used by workers to query localization.
        burst_datetime: Campaign start time.
        offset: Delay between each window end and scheduled execution.
        period: Duration of each conesearch window.
        total: Number of conesearch jobs to schedule.

    Raises:
        ValueError: If time bounds or job counts are invalid.
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
                scheduled_at,
                run_after
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id, job_type, subject_time_start, subject_time_end)
            DO NOTHING
            """,
            (
                event_id,
                ZTF_CONESEARCH_JOB_TYPE,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                scheduled_at,
            ),
        )


def insert_message(
    message: Message,
    filepath: Path,
    db_conn: Connection,
):
    """Record a message, derived milestone, artifact, and jobs atomically.

    Args:
        message: Kafka message containing a supported GCN notice.
        filepath: Path where the raw message was persisted.
        db_conn: Database connection used for the transaction.
    """
    subtype = message.topic()
    notice = PARSERS[subtype](message.value())
    localization = notice_localization(subtype, notice)
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
                subtype=subtype,
                published_at=notice.alert_datetime,
                subject_time_start=notice.burst_datetime,
                subject_time_end=notice.burst_datetime,
                milestone_type="notice",
                ra=localization.ra if localization is not None else None,
                dec=localization.dec if localization is not None else None,
                err_radius=localization.err_radius if localization is not None else None,
            )
            insert_artifact(
                cursor,
                milestone_id=milestone_id,
                uri=artifact_uri,
                artifact_type="gcn.voevent",
            )

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
